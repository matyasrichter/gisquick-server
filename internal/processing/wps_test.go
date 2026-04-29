package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newWPSBackend constructs a WPSBackend via NewBackend, pointed at the given
// base URL, using the provided http.Client (typically fakeServer.Client()).
func newWPSBackend(baseURL string, client *http.Client) (ProcessingBackend, domain.ProcessingService) {
	log := zap.NewNop().Sugar()
	svc := domain.ProcessingService{
		URL:  baseURL,
		Type: domain.ProcessingServiceTypeWPS,
	}
	return NewBackend(svc, client, log), svc
}

// ---------------------------------------------------------------------------
// FetchProcessList
// ---------------------------------------------------------------------------

func TestWPSFetchProcessList(t *testing.T) {
	fakeWPS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("request") != "GetCapabilities" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0"?>
<wps:Capabilities xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:ows="http://www.opengis.net/ows/1.1">
  <wps:Contents>
    <wps:ProcessSummary jobControlOptions="async-execute">
      <ows:Identifier>buffer</ows:Identifier>
      <ows:Title>Buffer</ows:Title>
    </wps:ProcessSummary>
    <wps:ProcessSummary jobControlOptions="sync-execute">
      <ows:Identifier>clip</ows:Identifier>
      <ows:Title>Clip</ows:Title>
    </wps:ProcessSummary>
  </wps:Contents>
</wps:Capabilities>`)
	}))
	defer fakeWPS.Close()

	backend, svc := newWPSBackend(fakeWPS.URL, fakeWPS.Client())

	summaries, err := backend.FetchProcessList(context.Background(), svc)
	if err != nil {
		t.Fatalf("FetchProcessList returned error: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(summaries))
	}

	// Verify first process.
	if summaries[0].ID != "buffer" {
		t.Errorf("expected ID 'buffer', got %q", summaries[0].ID)
	}
	if summaries[0].Title != "Buffer" {
		t.Errorf("expected title 'Buffer', got %q", summaries[0].Title)
	}
	if len(summaries[0].JobControlOptions) != 1 || summaries[0].JobControlOptions[0] != "async-execute" {
		t.Errorf("expected jobControlOptions [async-execute], got %v", summaries[0].JobControlOptions)
	}

	// Verify second process.
	if summaries[1].ID != "clip" {
		t.Errorf("expected ID 'clip', got %q", summaries[1].ID)
	}
	if summaries[1].Title != "Clip" {
		t.Errorf("expected title 'Clip', got %q", summaries[1].Title)
	}
	if len(summaries[1].JobControlOptions) != 1 || summaries[1].JobControlOptions[0] != "sync-execute" {
		t.Errorf("expected jobControlOptions [sync-execute], got %v", summaries[1].JobControlOptions)
	}
}

// ---------------------------------------------------------------------------
// DescribeProcess — type mapping for all three input kinds
// ---------------------------------------------------------------------------

func TestWPSDescribeProcessTypeMapping(t *testing.T) {
	fakeWPS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("request") == "GetCapabilities" {
			io.WriteString(w, `<wps:Capabilities xmlns:wps="http://www.opengis.net/wps/2.0" version="2.0.0" service="WPS"><wps:Contents></wps:Contents></wps:Capabilities>`)
			return
		}
		if r.URL.Query().Get("request") != "DescribeProcess" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		io.WriteString(w, `<?xml version="1.0"?>
<wps:ProcessOfferings xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:ows="http://www.opengis.net/ows/1.1">
  <wps:ProcessOffering jobControlOptions="async-execute">
    <wps:Process>
      <ows:Identifier>buffer</ows:Identifier>
      <ows:Title>Buffer</ows:Title>
      <wps:Input>
        <ows:Identifier>distance</ows:Identifier>
        <ows:Title>Distance</ows:Title>
        <wps:LiteralData>
          <ows:DataType>double</ows:DataType>
        </wps:LiteralData>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>geometry</ows:Identifier>
        <ows:Title>Geometry</ows:Title>
        <wps:ComplexData>
          <wps:Format mimeType="application/geo+json" default="true"/>
        </wps:ComplexData>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>bbox</ows:Identifier>
        <ows:Title>Bounding Box</ows:Title>
        <wps:BoundingBoxData/>
      </wps:Input>
      <wps:Output>
        <ows:Identifier>result</ows:Identifier>
        <ows:Title>Result</ows:Title>
        <wps:ComplexData>
          <wps:Format mimeType="application/geo+json"/>
        </wps:ComplexData>
      </wps:Output>
    </wps:Process>
  </wps:ProcessOffering>
</wps:ProcessOfferings>`)
	}))
	defer fakeWPS.Close()

	backend, svc := newWPSBackend(fakeWPS.URL, fakeWPS.Client())

	desc, err := backend.DescribeProcess(context.Background(), svc, "buffer")
	if err != nil {
		t.Fatalf("DescribeProcess returned error: %v", err)
	}
	if desc == nil {
		t.Fatal("expected non-nil ProcessDescription")
	}

	// Parse the Inputs JSON map to check schema types.
	var inputs map[string]map[string]any
	if err := json.Unmarshal(desc.Inputs, &inputs); err != nil {
		t.Fatalf("parsing inputs JSON: %v", err)
	}

	// LiteralData: double → JSON type "number"
	distInput, ok := inputs["distance"]
	if !ok {
		t.Fatal("expected 'distance' input")
	}
	distSchema, _ := distInput["schema"].(map[string]any)
	if distSchema == nil {
		t.Fatal("expected schema for 'distance'")
	}
	if distSchema["type"] != "number" {
		t.Errorf("expected distance type 'number', got %v", distSchema["type"])
	}

	// ComplexData with geo+json MIME type → JSON type "object" with format "geojson"
	geomInput, ok := inputs["geometry"]
	if !ok {
		t.Fatal("expected 'geometry' input")
	}
	geomSchema, _ := geomInput["schema"].(map[string]any)
	if geomSchema == nil {
		t.Fatal("expected schema for 'geometry'")
	}
	if geomSchema["type"] != "object" {
		t.Errorf("expected geometry type 'object', got %v", geomSchema["type"])
	}
	if geomSchema["format"] != "geojson" {
		t.Errorf("expected geometry format 'geojson', got %v", geomSchema["format"])
	}

	// BoundingBoxData → array schema with format "bbox"
	bboxInput, ok := inputs["bbox"]
	if !ok {
		t.Fatal("expected 'bbox' input")
	}
	bboxSchema, _ := bboxInput["schema"].(map[string]any)
	if bboxSchema == nil {
		t.Fatal("expected schema for 'bbox'")
	}
	if bboxSchema["type"] != "array" {
		t.Errorf("expected bbox type 'array', got %v", bboxSchema["type"])
	}
	if bboxSchema["format"] != "bbox" {
		t.Errorf("expected bbox format 'bbox', got %v", bboxSchema["format"])
	}

	// Verify jobControlOptions parsed correctly.
	if len(desc.JobControlOptions) != 1 || desc.JobControlOptions[0] != "async-execute" {
		t.Errorf("expected jobControlOptions [async-execute], got %v", desc.JobControlOptions)
	}
}

// ---------------------------------------------------------------------------
// Execute — async path
// ---------------------------------------------------------------------------

func TestWPSExecuteAsync(t *testing.T) {
	// getStatusCount controls which StatusInfo to return (0 = first call).
	var getStatusCount int32

	fakeWPS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.URL.Query().Get("request") {
		case "Execute":
			io.WriteString(w, `<?xml version="1.0"?>
<wps:StatusInfo xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-123</wps:JobID>
  <wps:Status>Accepted</wps:Status>
</wps:StatusInfo>`)

		case "GetStatus":
			count := atomic.AddInt32(&getStatusCount, 1)
			if count == 1 {
				// First poll: still running.
				io.WriteString(w, `<wps:StatusInfo xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-123</wps:JobID>
  <wps:Status>Running</wps:Status>
</wps:StatusInfo>`)
			} else {
				// Second poll: succeeded.
				io.WriteString(w, `<wps:StatusInfo xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-123</wps:JobID>
  <wps:Status>Succeeded</wps:Status>
</wps:StatusInfo>`)
			}

		case "GetResult":
			io.WriteString(w, `<?xml version="1.0"?>
<wps:Result xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-123</wps:JobID>
  <wps:Output id="result">
    <wps:Data>
      <wps:LiteralData>42.0</wps:LiteralData>
    </wps:Data>
  </wps:Output>
</wps:Result>`)

		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer fakeWPS.Close()

	descJSON, _ := json.Marshal(ProcessDescription{
		JobControlOptions: []string{"async-execute"},
	})
	svc := domain.ProcessingService{
		URL:  fakeWPS.URL,
		Type: domain.ProcessingServiceTypeWPS,
		Processes: map[string]domain.ProcessConfig{
			"buffer": {Description: json.RawMessage(descJSON)},
		},
	}

	log := zap.NewNop().Sugar()
	backend := NewBackend(svc, fakeWPS.Client(), log)
	backend.(*WPSBackend).pollInterval = time.Millisecond

	job := &JobRecord{
		JobID:     "local-job-123",
		ProcessID: "buffer",
		Project:   "user/test",
	}
	inputs := json.RawMessage(`{"inputs":{"distance":100}}`)

	results, remoteJobID, err := backend.Execute(context.Background(), job, svc, inputs)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if remoteJobID != "test-job-123" {
		t.Errorf("expected remoteJobID 'test-job-123', got %q", remoteJobID)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 output result, got %d", len(results))
	}
	if results[0].OutputID != "result" {
		t.Errorf("expected outputID 'result', got %q", results[0].OutputID)
	}
	if string(results[0].Value) != "42.0" {
		t.Errorf("expected value '42.0', got %q", string(results[0].Value))
	}
	if results[0].ContentType != "text/plain" {
		t.Errorf("expected ContentType 'text/plain', got %q", results[0].ContentType)
	}
}

// ---------------------------------------------------------------------------
// Execute — sync path
// ---------------------------------------------------------------------------

func TestWPSExecuteSync(t *testing.T) {
	fakeWPS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("request") != "Execute" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0"?>
<wps:Result xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-sync</wps:JobID>
  <wps:Output id="result">
    <wps:Data>
      <wps:LiteralData>42.0</wps:LiteralData>
    </wps:Data>
  </wps:Output>
</wps:Result>`)
	}))
	defer fakeWPS.Close()

	descJSON, _ := json.Marshal(ProcessDescription{
		JobControlOptions: []string{"sync-execute"},
	})
	svc := domain.ProcessingService{
		URL:  fakeWPS.URL,
		Type: domain.ProcessingServiceTypeWPS,
		Processes: map[string]domain.ProcessConfig{
			"clip": {Description: json.RawMessage(descJSON)},
		},
	}

	log := zap.NewNop().Sugar()
	backend := NewBackend(svc, fakeWPS.Client(), log)

	job := &JobRecord{
		JobID:     "local-job-sync",
		ProcessID: "clip",
		Project:   "user/test",
	}
	inputs := json.RawMessage(`{"inputs":{"distance":100}}`)

	results, remoteJobID, err := backend.Execute(context.Background(), job, svc, inputs)
	if err != nil {
		t.Fatalf("Execute (sync) returned error: %v", err)
	}
	if remoteJobID != "" {
		t.Errorf("expected empty remoteJobID for sync, got %q", remoteJobID)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 output result, got %d", len(results))
	}
	if results[0].OutputID != "result" {
		t.Errorf("expected outputID 'result', got %q", results[0].OutputID)
	}
	if string(results[0].Value) != "42.0" {
		t.Errorf("expected value '42.0', got %q", string(results[0].Value))
	}
	if results[0].ContentType != "text/plain" {
		t.Errorf("expected ContentType 'text/plain', got %q", results[0].ContentType)
	}
}

// ---------------------------------------------------------------------------
// Execute — failure path
// ---------------------------------------------------------------------------

func TestWPSExecuteFailure(t *testing.T) {
	fakeWPS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.URL.Query().Get("request") {
		case "Execute":
			io.WriteString(w, `<?xml version="1.0"?>
<wps:StatusInfo xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-fail</wps:JobID>
  <wps:Status>Accepted</wps:Status>
</wps:StatusInfo>`)
		case "GetStatus":
			io.WriteString(w, `<wps:StatusInfo xmlns:wps="http://www.opengis.net/wps/2.0">
  <wps:JobID>test-job-fail</wps:JobID>
  <wps:Status>Failed</wps:Status>
</wps:StatusInfo>`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer fakeWPS.Close()

	descJSON, _ := json.Marshal(ProcessDescription{
		JobControlOptions: []string{"async-execute"},
	})
	svc := domain.ProcessingService{
		URL:  fakeWPS.URL,
		Type: domain.ProcessingServiceTypeWPS,
		Processes: map[string]domain.ProcessConfig{
			"buffer": {Description: json.RawMessage(descJSON)},
		},
	}

	log := zap.NewNop().Sugar()
	backend := NewBackend(svc, fakeWPS.Client(), log)
	backend.(*WPSBackend).pollInterval = time.Millisecond

	job := &JobRecord{
		JobID:     "local-job-fail",
		ProcessID: "buffer",
		Project:   "user/test",
	}
	inputs := json.RawMessage(`{"inputs":{"distance":100}}`)

	_, _, err := backend.Execute(context.Background(), job, svc, inputs)
	if err == nil {
		t.Fatal("expected an error for Failed job, got nil")
	}
}

// ---------------------------------------------------------------------------
// literalDataType — table-driven tests
// ---------------------------------------------------------------------------

func TestLiteralDataType(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		// Bare names — number variants
		{"float", "number"},
		{"double", "number"},
		{"decimal", "number"},
		// Bare names — integer variants
		{"integer", "integer"},
		{"long", "integer"},
		{"int", "integer"},
		{"short", "integer"},
		// Bare name — boolean
		{"boolean", "boolean"},
		// Bare name — fallthrough to string
		{"string", "string"},
		{"anyURI", "string"},
		{"", "string"},
		// xs:-prefixed inputs
		{"xs:double", "number"},
		{"xs:float", "number"},
		{"xs:decimal", "number"},
		{"xs:integer", "integer"},
		{"xs:long", "integer"},
		{"xs:int", "integer"},
		{"xs:short", "integer"},
		{"xs:boolean", "boolean"},
		{"xs:string", "string"},
		// Full URI form (hash separator)
		{"http://www.w3.org/2001/XMLSchema#double", "number"},
		{"http://www.w3.org/2001/XMLSchema#float", "number"},
		{"http://www.w3.org/2001/XMLSchema#integer", "integer"},
		{"http://www.w3.org/2001/XMLSchema#boolean", "boolean"},
		{"http://www.w3.org/2001/XMLSchema#string", "string"},
		// Full URI form (slash separator)
		{"http://www.w3.org/2001/XMLSchema/double", "number"},
		{"http://www.w3.org/2001/XMLSchema/integer", "integer"},
		// Case-insensitivity
		{"DOUBLE", "number"},
		{"INTEGER", "integer"},
		{"BOOLEAN", "boolean"},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := literalDataType(tc.input)
			if got != tc.expected {
				t.Errorf("literalDataType(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FetchProcessList — WPS 1.0
// ---------------------------------------------------------------------------

func TestWPS1FetchProcessList(t *testing.T) {
	capsXML := `<?xml version="1.0" encoding="UTF-8"?>
<wps:WPS_Capabilities xmlns:wps="http://www.opengis.net/wps/1.0.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    version="1.0.0" service="WPS">
  <wps:ProcessOfferings>
    <wps:Process wps:processVersion="1.0">
      <ows:Identifier>buffer</ows:Identifier>
      <ows:Title>Buffer</ows:Title>
      <ows:Abstract>Buffers features</ows:Abstract>
      <ows:Keywords><ows:Keyword>geo</ows:Keyword></ows:Keywords>
    </wps:Process>
    <wps:Process wps:processVersion="1.0">
      <ows:Identifier>clip</ows:Identifier>
      <ows:Title>Clip</ows:Title>
      <ows:Abstract></ows:Abstract>
    </wps:Process>
  </wps:ProcessOfferings>
</wps:WPS_Capabilities>`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, capsXML)
	}))
	defer srv.Close()

	backend := &WPSBackend{client: srv.Client(), log: zap.NewNop().Sugar()}
	service := domain.ProcessingService{URL: srv.URL}
	summaries, err := backend.FetchProcessList(context.Background(), service)
	require.NoError(t, err)
	require.Len(t, summaries, 2)

	assert.Equal(t, "buffer", summaries[0].ID)
	assert.Equal(t, "Buffer", summaries[0].Title)
	assert.Equal(t, "Buffers features", summaries[0].Description)
	assert.Equal(t, []string{"geo"}, summaries[0].Keywords)

	assert.Equal(t, "clip", summaries[1].ID)
}

// ---------------------------------------------------------------------------
// DescribeProcess — WPS 1.0
// ---------------------------------------------------------------------------

func TestWPS1DescribeProcess(t *testing.T) {
	capsXML := `<?xml version="1.0"?>
<wps:WPS_Capabilities xmlns:wps="http://www.opengis.net/wps/1.0.0"
    xmlns:ows="http://www.opengis.net/ows/1.1" version="1.0.0" service="WPS">
  <wps:ProcessOfferings>
    <wps:Process><ows:Identifier>buffer</ows:Identifier><ows:Title>Buffer</ows:Title></wps:Process>
  </wps:ProcessOfferings>
</wps:WPS_Capabilities>`

	describeXML := `<?xml version="1.0"?>
<wps:ProcessDescriptions xmlns:wps="http://www.opengis.net/wps/1.0.0"
    xmlns:ows="http://www.opengis.net/ows/1.1" version="1.0.0" service="WPS">
  <ProcessDescription wps:processVersion="1.0" statusSupported="true" storeSupported="true">
    <ows:Identifier>buffer</ows:Identifier>
    <ows:Title>Buffer</ows:Title>
    <ows:Abstract>Buffers features by distance</ows:Abstract>
    <DataInputs>
      <Input minOccurs="1" maxOccurs="1">
        <ows:Identifier>distance</ows:Identifier>
        <ows:Title>Distance</ows:Title>
        <LiteralData>
          <ows:DataType ows:reference="http://www.w3.org/TR/xmlschema-2/#double">xs:double</ows:DataType>
          <ows:AllowedValues><ows:AnyValue/></ows:AllowedValues>
        </LiteralData>
      </Input>
      <Input minOccurs="1" maxOccurs="1">
        <ows:Identifier>features</ows:Identifier>
        <ows:Title>Input features</ows:Title>
        <ComplexData>
          <Default><Format><MimeType>application/geo+json</MimeType></Format></Default>
          <Supported><Format><MimeType>application/geo+json</MimeType></Format></Supported>
        </ComplexData>
      </Input>
      <Input minOccurs="0" maxOccurs="1">
        <ows:Identifier>bbox</ows:Identifier>
        <ows:Title>Bounding Box</ows:Title>
        <BoundingBoxData>
          <Default><CRS>EPSG:4326</CRS></Default>
        </BoundingBoxData>
      </Input>
    </DataInputs>
    <ProcessOutputs>
      <Output>
        <ows:Identifier>result</ows:Identifier>
        <ows:Title>Buffered result</ows:Title>
        <ComplexOutput>
          <Default><Format><MimeType>application/geo+json</MimeType></Format></Default>
        </ComplexOutput>
      </Output>
    </ProcessOutputs>
  </ProcessDescription>
</wps:ProcessDescriptions>`

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		reqParam := r.URL.Query().Get("request")
		if reqParam == "GetCapabilities" {
			fmt.Fprint(w, capsXML)
		} else {
			callCount++
			fmt.Fprint(w, describeXML)
		}
	}))
	defer srv.Close()

	backend := &WPSBackend{client: srv.Client(), log: zap.NewNop().Sugar()}
	service := domain.ProcessingService{URL: srv.URL}

	desc, err := backend.DescribeProcess(context.Background(), service, "buffer")
	require.NoError(t, err)
	assert.Equal(t, "Buffer", desc.Title)
	assert.Equal(t, "Buffers features by distance", desc.Description)
	assert.Contains(t, desc.JobControlOptions, "async-execute")

	var inputs map[string]any
	require.NoError(t, json.Unmarshal(desc.Inputs, &inputs))
	assert.Contains(t, inputs, "distance")
	assert.Contains(t, inputs, "features")
	assert.Contains(t, inputs, "bbox")

	distSchema := inputs["distance"].(map[string]any)["schema"].(map[string]any)
	assert.Equal(t, "number", distSchema["type"])

	featSchema := inputs["features"].(map[string]any)["schema"].(map[string]any)
	assert.Equal(t, "geojson", featSchema["format"])

	bboxInputSchema := inputs["bbox"].(map[string]any)["schema"].(map[string]any)
	assert.Equal(t, "bbox", bboxInputSchema["format"])

	var outputs map[string]any
	require.NoError(t, json.Unmarshal(desc.Outputs, &outputs))
	assert.Contains(t, outputs, "result")
	assert.Equal(t, 1, callCount)
}

// ---------------------------------------------------------------------------
// wpsDetectMajorVersion
// ---------------------------------------------------------------------------

func TestWPSDetectVersion(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantMajor int
	}{
		{
			name:      "wps 2.0.0",
			body:      `<wps:Capabilities xmlns:wps="http://www.opengis.net/wps/2.0" version="2.0.0" service="WPS"></wps:Capabilities>`,
			wantMajor: 2,
		},
		{
			name:      "wps 2.0.2",
			body:      `<wps:Capabilities xmlns:wps="http://www.opengis.net/wps/2.0" version="2.0.2" service="WPS"></wps:Capabilities>`,
			wantMajor: 2,
		},
		{
			name:      "wps 1.0.0",
			body:      `<wps:WPS_Capabilities xmlns:wps="http://www.opengis.net/wps/1.0.0" version="1.0.0" service="WPS"></wps:WPS_Capabilities>`,
			wantMajor: 1,
		},
		{
			name:      "wps 1.3.0",
			body:      `<wps:WPS_Capabilities xmlns:wps="http://www.opengis.net/wps/1.0.0" version="1.3.0" service="WPS"></wps:WPS_Capabilities>`,
			wantMajor: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wpsDetectMajorVersion([]byte(tc.body))
			require.NoError(t, err)
			assert.Equal(t, tc.wantMajor, got)
		})
	}
}
