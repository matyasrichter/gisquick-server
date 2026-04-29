package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gisquick/gisquick-server/internal/domain"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// WPS 2.0.2 XML structs — GetCapabilities response
// ---------------------------------------------------------------------------

type wpsCapabilities struct {
	XMLName  xml.Name    `xml:"http://www.opengis.net/wps/2.0 Capabilities"`
	Contents wpsContents `xml:"http://www.opengis.net/wps/2.0 Contents"`
}

type wpsContents struct {
	Processes []wpsProcessSummary `xml:"http://www.opengis.net/wps/2.0 ProcessSummary"`
}

type wpsProcessSummary struct {
	Identifier        owsIdentifier `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title             string        `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract          string        `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	Keywords          owsKeywords   `xml:"http://www.opengis.net/ows/1.1 Keywords"`
	JobControlOptions string        `xml:"jobControlOptions,attr"`
}

type owsIdentifier struct {
	Value string `xml:",chardata"`
}

type owsKeywords struct {
	Keyword []string `xml:"http://www.opengis.net/ows/1.1 Keyword"`
}

// ---------------------------------------------------------------------------
// WPS 2.0.2 XML structs — DescribeProcess response
// ---------------------------------------------------------------------------

type wpsProcessOfferings struct {
	XMLName   xml.Name            `xml:"http://www.opengis.net/wps/2.0 ProcessOfferings"`
	Offerings []wpsProcessOffering `xml:"http://www.opengis.net/wps/2.0 ProcessOffering"`
}

type wpsProcessOffering struct {
	JobControlOptions string     `xml:"jobControlOptions,attr"`
	Process           wpsProcess `xml:"http://www.opengis.net/wps/2.0 Process"`
}

type wpsProcess struct {
	Identifier owsIdentifier `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title      string        `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract   string        `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	Inputs     []wpsInput    `xml:"http://www.opengis.net/wps/2.0 Input"`
	Outputs    []wpsOutput   `xml:"http://www.opengis.net/wps/2.0 Output"`
}

type wpsInput struct {
	Identifier      owsIdentifier    `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title           string           `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract        string           `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	LiteralData     *wpsLiteralData  `xml:"http://www.opengis.net/wps/2.0 LiteralData"`
	ComplexData     *wpsComplexData  `xml:"http://www.opengis.net/wps/2.0 ComplexData"`
	BoundingBoxData *wpsBoundingBoxData `xml:"http://www.opengis.net/wps/2.0 BoundingBoxData"`
}

type wpsOutput struct {
	Identifier      owsIdentifier    `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title           string           `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract        string           `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	LiteralData     *wpsLiteralData  `xml:"http://www.opengis.net/wps/2.0 LiteralData"`
	ComplexData     *wpsComplexData  `xml:"http://www.opengis.net/wps/2.0 ComplexData"`
	BoundingBoxData *wpsBoundingBoxData `xml:"http://www.opengis.net/wps/2.0 BoundingBoxData"`
}

type wpsLiteralData struct {
	AllowedValues owsAllowedValues `xml:"http://www.opengis.net/ows/1.1 AllowedValues"`
	DataType      struct {
		Value     string `xml:",chardata"`
		Reference string `xml:"reference,attr"`
	} `xml:"http://www.opengis.net/ows/1.1 DataType"`
	Default string `xml:"default,attr"`
}

type owsAllowedValues struct {
	Values []string `xml:"http://www.opengis.net/ows/1.1 Value"`
}

type wpsComplexData struct {
	Formats []wpsFormat `xml:"http://www.opengis.net/wps/2.0 Format"`
}

type wpsFormat struct {
	MimeType string `xml:"mimeType,attr"`
	Default  string `xml:"default,attr"`
}

// wpsBoundingBoxData is a marker struct; we only need its presence.
type wpsBoundingBoxData struct{}

// ---------------------------------------------------------------------------
// WPS 1.0 XML structs — GetCapabilities response
// ---------------------------------------------------------------------------

type wps1Capabilities struct {
	XMLName          xml.Name             `xml:"http://www.opengis.net/wps/1.0.0 WPS_Capabilities"`
	Version          string               `xml:"version,attr"`
	ProcessOfferings wps1ProcessOfferings `xml:"http://www.opengis.net/wps/1.0.0 ProcessOfferings"`
}

type wps1ProcessOfferings struct {
	Processes []wps1ProcessBrief `xml:"http://www.opengis.net/wps/1.0.0 Process"`
}

type wps1ProcessBrief struct {
	Identifier owsIdentifier `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title      string        `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract   string        `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	Keywords   owsKeywords   `xml:"http://www.opengis.net/ows/1.1 Keywords"`
}

// ---------------------------------------------------------------------------
// WPS 1.0 XML structs — DescribeProcess response
// ---------------------------------------------------------------------------

type wps1ProcessDescriptions struct {
	XMLName   xml.Name                 `xml:"http://www.opengis.net/wps/1.0.0 ProcessDescriptions"`
	Processes []wps1ProcessDescription `xml:"ProcessDescription"`
}

type wps1ProcessDescription struct {
	Identifier      owsIdentifier      `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title           string             `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract        string             `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	StatusSupported bool               `xml:"statusSupported,attr"`
	StoreSupported  bool               `xml:"storeSupported,attr"`
	DataInputs      wps1DataInputsElem `xml:"DataInputs"`
	ProcessOutputs  wps1ProcessOutputs `xml:"ProcessOutputs"`
}

type wps1DataInputsElem struct {
	Inputs []wps1Input `xml:"Input"`
}

type wps1Input struct {
	Identifier      owsIdentifier        `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title           string               `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract        string               `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	MinOccurs       int                  `xml:"minOccurs,attr"`
	MaxOccurs       int                  `xml:"maxOccurs,attr"`
	LiteralData     *wps1LiteralData     `xml:"LiteralData"`
	ComplexData     *wps1ComplexData     `xml:"ComplexData"`
	BoundingBoxData *wps1BoundingBoxData `xml:"BoundingBoxData"`
}

type wps1LiteralData struct {
	DataType      wps1DataType      `xml:"http://www.opengis.net/ows/1.1 DataType"`
	AllowedValues *owsAllowedValues `xml:"http://www.opengis.net/ows/1.1 AllowedValues"`
}

type wps1DataType struct {
	Reference string `xml:"reference,attr"`
	Value     string `xml:",chardata"`
}

type wps1ComplexData struct {
	Default   wps1ComplexDefault   `xml:"Default"`
	Supported wps1ComplexSupported `xml:"Supported"`
}

type wps1ComplexDefault struct {
	Format wps1Format `xml:"Format"`
}

type wps1ComplexSupported struct {
	Formats []wps1Format `xml:"Format"`
}

type wps1Format struct {
	MimeType string `xml:"MimeType"`
	Schema   string `xml:"Schema"`
}

type wps1BoundingBoxData struct {
	Default wps1BBoxDefault `xml:"Default"`
}

type wps1BBoxDefault struct {
	CRS string `xml:"CRS"`
}

type wps1ProcessOutputs struct {
	Outputs []wps1Output `xml:"Output"`
}

type wps1Output struct {
	Identifier    owsIdentifier        `xml:"http://www.opengis.net/ows/1.1 Identifier"`
	Title         string               `xml:"http://www.opengis.net/ows/1.1 Title"`
	Abstract      string               `xml:"http://www.opengis.net/ows/1.1 Abstract"`
	ComplexOutput *wps1ComplexOutput   `xml:"ComplexOutput"`
	LiteralOutput *wps1LiteralOutput   `xml:"LiteralOutput"`
	BBoxOutput    *wps1BoundingBoxData `xml:"BoundingBoxOutput"`
}

type wps1ComplexOutput struct {
	Default   wps1ComplexDefault   `xml:"Default"`
	Supported wps1ComplexSupported `xml:"Supported"`
}

type wps1LiteralOutput struct {
	DataType wps1DataType `xml:"http://www.opengis.net/ows/1.1 DataType"`
}

// ---------------------------------------------------------------------------
// WPSBackend
// ---------------------------------------------------------------------------

// WPSBackend implements ProcessingBackend for OGC WPS services (versions 1.x and 2.x).
type WPSBackend struct {
	client       *http.Client
	log          *zap.SugaredLogger
	pollInterval time.Duration // overridable in tests; zero → use initialPollInterval
	versionCache sync.Map      // map[serviceURL string]int (1 or 2)
}

// ---------------------------------------------------------------------------
// WPS 2.0.2 XML structs — Execute request
// ---------------------------------------------------------------------------

type wpsExecuteRequest struct {
	XMLName    xml.Name           `xml:"wps:Execute"`
	WPSNs      string             `xml:"xmlns:wps,attr"`
	OWSNs      string             `xml:"xmlns:ows,attr"`
	Service    string             `xml:"service,attr"`
	Version    string             `xml:"version,attr"`
	Mode       string             `xml:"mode,attr"`
	Response   string             `xml:"response,attr"`
	Identifier wpsExecIdentifier  `xml:"ows:Identifier"`
	Inputs     []wpsInputElement  `xml:"wps:Input"`
	Outputs    []wpsOutputRequest `xml:"wps:Output"`
}

type wpsExecIdentifier struct {
	Value string `xml:",chardata"`
}

type wpsInputElement struct {
	ID   string      `xml:"id,attr"`
	Data wpsDataElem `xml:"wps:Data"`
}

type wpsDataElem struct {
	LiteralData  *wpsExecLiteralData  `xml:"wps:LiteralData"`
	ComplexData  *wpsExecComplexData  `xml:"wps:ComplexData"`
	BoundingBox  *wpsExecBoundingBox  `xml:"wps:BoundingBoxData"`
}

type wpsExecLiteralData struct {
	Value string `xml:",chardata"`
}

type wpsExecComplexData struct {
	MimeType string `xml:"mimeType,attr"`
	Value    string `xml:",chardata"`
}

type wpsExecBoundingBox struct {
	CRS        string `xml:"crs,attr"`
	LowerCorner string `xml:"ows:LowerCorner"`
	UpperCorner string `xml:"ows:UpperCorner"`
}

type wpsOutputRequest struct {
	ID           string `xml:"id,attr"`
	Transmission string `xml:"transmission,attr"`
}

// ---------------------------------------------------------------------------
// WPS 2.0.2 XML structs — StatusInfo and Result responses
// ---------------------------------------------------------------------------

type wpsStatusInfoResponse struct {
	XMLName xml.Name `xml:"http://www.opengis.net/wps/2.0 StatusInfo"`
	JobID   string   `xml:"http://www.opengis.net/wps/2.0 JobID"`
	Status  string   `xml:"http://www.opengis.net/wps/2.0 Status"`
}

type wpsResultResponse struct {
	XMLName xml.Name           `xml:"http://www.opengis.net/wps/2.0 Result"`
	JobID   string             `xml:"http://www.opengis.net/wps/2.0 JobID"`
	Outputs []wpsOutputElement `xml:"http://www.opengis.net/wps/2.0 Output"`
}

type wpsOutputElement struct {
	ID        string       `xml:"id,attr"`
	Data      *wpsDataOut  `xml:"http://www.opengis.net/wps/2.0 Data"`
	Reference *wpsRefOut   `xml:"http://www.opengis.net/wps/2.0 Reference"`
}

type wpsDataOut struct {
	LiteralData *wpsLiteralDataOut  `xml:"http://www.opengis.net/wps/2.0 LiteralData"`
	ComplexData *wpsComplexDataOut  `xml:"http://www.opengis.net/wps/2.0 ComplexData"`
}

type wpsLiteralDataOut struct {
	Value string `xml:",chardata"`
}

type wpsComplexDataOut struct {
	MimeType string `xml:"mimeType,attr"`
	Value    string `xml:",chardata"`
}

type wpsRefOut struct {
	Href string `xml:"href,attr"`
}

// ---------------------------------------------------------------------------
// URL helpers
// ---------------------------------------------------------------------------

// wpsQueryURL constructs a WPS 2.0.0 KVP request URL.
func wpsQueryURL(base, request, identifier string) string {
	u := base + "?service=WPS&version=2.0.0&request=" + request
	if identifier != "" {
		u += "&identifier=" + url.QueryEscape(identifier)
	}
	return u
}

// wpsJobURL constructs a WPS 2.0.0 KVP URL for job-scoped operations
// (GetStatus, GetResult) that use a jobId parameter instead of identifier.
func wpsJobURL(base, request, jobID string) string {
	return base + "?service=WPS&version=2.0.0&request=" + request + "&jobId=" + url.QueryEscape(jobID)
}

// wpsDetectMajorVersion parses the version attribute from the root element of a
// WPS GetCapabilities response body and returns the major version number (1 or 2).
// Any 1.x version returns 1; 2.x (including 2.0.2) returns 2.
func wpsDetectMajorVersion(body []byte) (int, error) {
	type versionRoot struct {
		Version string `xml:"version,attr"`
	}
	var r versionRoot
	if err := xml.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("parsing WPS capabilities version: %w", err)
	}
	if strings.HasPrefix(r.Version, "1.") {
		return 1, nil
	}
	return 2, nil // treat unknown/2.x as version 2
}

// wpsMajorVersion returns the cached major version for the given service URL,
// or probes via GetCapabilities and caches the result.
func (b *WPSBackend) wpsMajorVersion(ctx context.Context, service domain.ProcessingService) (int, error) {
	if v, ok := b.versionCache.Load(service.URL); ok {
		return v.(int), nil
	}
	// Probe with no version so any WPS server responds.
	probeURL := service.URL + "?service=WPS&request=GetCapabilities"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return 0, fmt.Errorf("building WPS version probe: %w", err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("WPS version probe: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("reading WPS version probe: %w", err)
	}
	major, err := wpsDetectMajorVersion(body)
	if err != nil {
		return 0, err
	}
	b.versionCache.Store(service.URL, major)
	return major, nil
}

func parseWPS1ProcessList(body []byte) ([]ProcessSummary, error) {
	var caps wps1Capabilities
	if err := xml.Unmarshal(body, &caps); err != nil {
		return nil, fmt.Errorf("decoding WPS 1.0 Capabilities XML: %w", err)
	}
	summaries := make([]ProcessSummary, 0, len(caps.ProcessOfferings.Processes))
	for _, ps := range caps.ProcessOfferings.Processes {
		summaries = append(summaries, ProcessSummary{
			ID:                 ps.Identifier.Value,
			Title:              ps.Title,
			Description:        ps.Abstract,
			Keywords:           ps.Keywords.Keyword,
			JobControlOptions:  []string{"async-execute", "sync-execute"},
			OutputTransmission: []string{"value", "reference"},
		})
	}
	return summaries, nil
}

func parseWPS2ProcessList(body []byte) ([]ProcessSummary, error) {
	var caps wpsCapabilities
	if err := xml.Unmarshal(body, &caps); err != nil {
		return nil, fmt.Errorf("decoding WPS Capabilities XML: %w", err)
	}
	summaries := make([]ProcessSummary, 0, len(caps.Contents.Processes))
	for _, ps := range caps.Contents.Processes {
		var jco []string
		if ps.JobControlOptions != "" {
			jco = strings.Fields(ps.JobControlOptions)
		}
		summaries = append(summaries, ProcessSummary{
			ID:                 ps.Identifier.Value,
			Title:              ps.Title,
			Description:        ps.Abstract,
			Keywords:           ps.Keywords.Keyword,
			Version:            "",
			JobControlOptions:  jco,
			OutputTransmission: []string{"value", "reference"},
		})
	}
	return summaries, nil
}

// FetchProcessList calls GetCapabilities and returns a ProcessSummary slice.
func (b *WPSBackend) FetchProcessList(ctx context.Context, service domain.ProcessingService) ([]ProcessSummary, error) {
	capsURL := service.URL + "?service=WPS&request=GetCapabilities"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, capsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building WPS GetCapabilities request: %w", err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("WPS GetCapabilities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("WPS GetCapabilities returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading WPS GetCapabilities body: %w", err)
	}

	major, err := wpsDetectMajorVersion(body)
	if err != nil {
		return nil, err
	}
	b.versionCache.Store(service.URL, major)
	b.log.Debugw("WPS GetCapabilities", "service", service.URL, "majorVersion", major)

	if major == 1 {
		return parseWPS1ProcessList(body)
	}
	return parseWPS2ProcessList(body)
}

// DescribeProcess calls DescribeProcess and translates the WPS XML to a
// ProcessDescription whose Inputs/Outputs fields carry OGC API JSON schemas.
func (b *WPSBackend) DescribeProcess(ctx context.Context, service domain.ProcessingService, processID string) (*ProcessDescription, error) {
	major, err := b.wpsMajorVersion(ctx, service)
	if err != nil {
		return nil, err
	}

	var descURL string
	if major == 1 {
		descURL = service.URL + "?service=WPS&version=1.0.0&request=DescribeProcess&Identifier=" + url.QueryEscape(processID)
	} else {
		descURL = wpsQueryURL(service.URL, "DescribeProcess", processID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, descURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building WPS DescribeProcess request: %w", err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("WPS DescribeProcess: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("WPS DescribeProcess returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading WPS DescribeProcess body: %w", err)
	}

	b.log.Debugw("WPS DescribeProcess", "service", service.URL, "processID", processID, "majorVersion", major)

	if major == 1 {
		return parseWPS1ProcessDescription(body, processID)
	}

	var offerings wpsProcessOfferings
	if err := xml.Unmarshal(body, &offerings); err != nil {
		return nil, fmt.Errorf("decoding WPS ProcessOfferings XML: %w", err)
	}

	// Find the offering whose nested Process identifier matches processID.
	var offering *wpsProcessOffering
	for i := range offerings.Offerings {
		if offerings.Offerings[i].Process.Identifier.Value == processID {
			offering = &offerings.Offerings[i]
			break
		}
	}
	if offering == nil {
		if len(offerings.Offerings) > 0 {
			// Fall back to the first offering when the server returns exactly one.
			offering = &offerings.Offerings[0]
		} else {
			return nil, fmt.Errorf("process %q not found in WPS DescribeProcess response", processID)
		}
	}

	// Job control options.
	jco := []string{"async-execute"}
	if offering.JobControlOptions != "" {
		jco = strings.Fields(offering.JobControlOptions)
	}

	// Translate inputs.
	inputsMap := make(map[string]any, len(offering.Process.Inputs))
	for _, inp := range offering.Process.Inputs {
		inputsMap[inp.Identifier.Value] = wpsInputToOGCAPI(inp)
	}

	// Translate outputs.
	outputsMap := make(map[string]any, len(offering.Process.Outputs))
	for _, out := range offering.Process.Outputs {
		outputsMap[out.Identifier.Value] = wpsOutputToOGCAPI(out)
	}

	inputsJSON, err := json.Marshal(inputsMap)
	if err != nil {
		return nil, fmt.Errorf("encoding inputs schema: %w", err)
	}
	outputsJSON, err := json.Marshal(outputsMap)
	if err != nil {
		return nil, fmt.Errorf("encoding outputs schema: %w", err)
	}

	return &ProcessDescription{
		Title:             offering.Process.Title,
		Description:       offering.Process.Abstract,
		Version:           "",
		JobControlOptions: jco,
		Inputs:            json.RawMessage(inputsJSON),
		Outputs:           json.RawMessage(outputsJSON),
		Raw:               nil,
	}, nil
}

// Execute submits an execution request to a WPS 2.0.2 service, waits for
// completion (polling for async, or parsing inline for sync), and returns the
// output results together with the remote job ID.
func (b *WPSBackend) Execute(ctx context.Context, job *JobRecord, service domain.ProcessingService, inputs json.RawMessage) ([]OutputResult, string, error) {
	// ------------------------------------------------------------------
	// Step 1: determine execution mode from stored process description.
	// ------------------------------------------------------------------
	mode := "async" // default
	if pc, ok := service.Processes[job.ProcessID]; ok && len(pc.Description) > 0 {
		var desc ProcessDescription
		if err := json.Unmarshal(pc.Description, &desc); err == nil {
			hasAsync := false
			hasSync := false
			for _, opt := range desc.JobControlOptions {
				switch opt {
				case "async-execute":
					hasAsync = true
				case "sync-execute":
					hasSync = true
				}
			}
			if hasSync && !hasAsync {
				mode = "sync"
			}
		}
	}

	// ------------------------------------------------------------------
	// Step 2: translate OGC API JSON inputs → WPS Execute XML.
	// ------------------------------------------------------------------
	xmlBody, err := b.buildExecuteXML(job.ProcessID, mode, inputs)
	if err != nil {
		return nil, "", fmt.Errorf("building WPS Execute XML: %w", err)
	}

	// ------------------------------------------------------------------
	// Step 3: POST the Execute request.
	// ------------------------------------------------------------------
	executeURL := service.URL + "?service=WPS&version=2.0.0&request=Execute"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, executeURL, bytes.NewReader(xmlBody))
	if err != nil {
		return nil, "", fmt.Errorf("building WPS Execute HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/xml")
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("sending WPS Execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("WPS Execute returned status %d: %s", resp.StatusCode, string(body))
	}

	// ------------------------------------------------------------------
	// Step 4/5: parse response body — async → poll; sync → parse inline.
	// ------------------------------------------------------------------
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading WPS Execute response body: %w", err)
	}

	// Detect which root element was returned regardless of requested mode.
	rootName := wpsRootElementName(body)
	switch rootName {
	case "StatusInfo":
		// Async path: parse the StatusInfo, then poll.
		var si wpsStatusInfoResponse
		if err := xml.Unmarshal(body, &si); err != nil {
			return nil, "", fmt.Errorf("parsing WPS StatusInfo: %w", err)
		}
		results, err := b.wpsPollAndFetch(ctx, service, si.JobID)
		return results, si.JobID, err

	case "Result":
		// Sync path: parse wps:Result inline.
		results, err := b.parseWPSResult(body)
		return results, "", err

	default:
		return nil, "", fmt.Errorf("unexpected WPS Execute response root element %q", rootName)
	}
}

// buildExecuteXML marshals a WPS 2.0.2 Execute request body from the OGC API
// JSON inputs document.
func (b *WPSBackend) buildExecuteXML(processID, mode string, inputs json.RawMessage) ([]byte, error) {
	// Parse the OGC API inputs map.
	var wrapper struct {
		Inputs map[string]json.RawMessage `json:"inputs"`
	}
	if err := json.Unmarshal(inputs, &wrapper); err != nil {
		return nil, fmt.Errorf("parsing inputs JSON: %w", err)
	}

	// Translate each input.
	var inputElems []wpsInputElement
	for id, rawVal := range wrapper.Inputs {
		elem, err := buildWPSInputElement(id, rawVal)
		if err != nil {
			return nil, fmt.Errorf("translating input %q: %w", id, err)
		}
		inputElems = append(inputElems, elem)
	}

	exec := wpsExecuteRequest{
		WPSNs:   "http://www.opengis.net/wps/2.0",
		OWSNs:   "http://www.opengis.net/ows/1.1",
		Service:  "WPS",
		Version:  "2.0.0",
		Mode:     mode,
		Response: "document",
		Identifier: wpsExecIdentifier{Value: processID},
		Inputs:   inputElems,
		// Omit Outputs to request all outputs; WPS servers return everything by default.
	}

	out, err := xml.MarshalIndent(exec, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), out...), nil
}

// buildWPSInputElement converts a single OGC API JSON input value to a
// wpsInputElement for XML marshaling.
func buildWPSInputElement(id string, raw json.RawMessage) (wpsInputElement, error) {
	// Determine the JSON value kind.
	var val any
	if err := json.Unmarshal(raw, &val); err != nil {
		return wpsInputElement{}, fmt.Errorf("parsing value: %w", err)
	}

	elem := wpsInputElement{ID: id}

	switch v := val.(type) {
	case map[string]any:
		// JSON object → ComplexData (GeoJSON)
		elem.Data = wpsDataElem{
			ComplexData: &wpsExecComplexData{
				MimeType: "application/geo+json",
				Value:    string(raw),
			},
		}

	case []any:
		// JSON array: bounding box if 4–6 numbers, otherwise ComplexData.
		if isBBoxArray(v) {
			nums := make([]float64, len(v))
			for i, n := range v {
				nums[i] = n.(float64)
			}
			half := len(nums) / 2
			lowerParts := make([]string, half)
			upperParts := make([]string, half)
			for i := 0; i < half; i++ {
				lowerParts[i] = fmt.Sprintf("%g", nums[i])
				upperParts[i] = fmt.Sprintf("%g", nums[half+i])
			}
			lower := strings.Join(lowerParts, " ")
			upper := strings.Join(upperParts, " ")
			elem.Data = wpsDataElem{
				BoundingBox: &wpsExecBoundingBox{
					CRS:         "EPSG:4326",
					LowerCorner: lower,
					UpperCorner: upper,
				},
			}
		} else {
			elem.Data = wpsDataElem{
				ComplexData: &wpsExecComplexData{
					MimeType: "application/json",
					Value:    string(raw),
				},
			}
		}

	default:
		// Scalar (string, number, boolean) → LiteralData.
		var s string
		switch tv := v.(type) {
		case string:
			s = tv
		case float64:
			s = fmt.Sprintf("%g", tv)
		case bool:
			if tv {
				s = "true"
			} else {
				s = "false"
			}
		default:
			s = string(raw)
		}
		elem.Data = wpsDataElem{
			LiteralData: &wpsExecLiteralData{Value: s},
		}
	}

	return elem, nil
}

// isBBoxArray returns true when v is a JSON array of 4–6 float64 numbers.
func isBBoxArray(v []any) bool {
	if len(v) < 4 || len(v) > 6 {
		return false
	}
	for _, item := range v {
		if _, ok := item.(float64); !ok {
			return false
		}
	}
	return true
}

// wpsRootElementName returns the local name of the root XML element of buf.
func wpsRootElementName(buf []byte) string {
	dec := xml.NewDecoder(bytes.NewReader(buf))
	for {
		tok, err := dec.Token()
		if err != nil {
			return ""
		}
		if se, ok := tok.(xml.StartElement); ok {
			return se.Name.Local
		}
	}
}

// wpsPollAndFetch polls GetStatus until the job succeeds or fails, then calls
// GetResult and parses the output.
func (b *WPSBackend) wpsPollAndFetch(ctx context.Context, service domain.ProcessingService, jobID string) ([]OutputResult, error) {
	interval := b.pollInterval
	if interval == 0 {
		interval = initialPollInterval
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}

		statusURL := wpsJobURL(service.URL, "GetStatus", jobID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if err != nil {
			return nil, fmt.Errorf("building GetStatus request: %w", err)
		}
		for k, v := range service.Headers {
			req.Header.Set(k, v)
		}

		resp, err := b.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("WPS GetStatus: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("reading GetStatus body: %w", readErr)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("WPS GetStatus returned status %d: %s", resp.StatusCode, string(body))
		}

		var si wpsStatusInfoResponse
		if err := xml.Unmarshal(body, &si); err != nil {
			return nil, fmt.Errorf("parsing WPS StatusInfo: %w", err)
		}

		b.log.Debugw("WPS job poll", "jobID", jobID, "status", si.Status)

		switch si.Status {
		case "Succeeded":
			return b.wpsGetResult(ctx, service, jobID)
		case "Failed", "Dismissed":
			return nil, fmt.Errorf("WPS job %s: status=%s", jobID, si.Status)
		default:
			// Accepted / Running — backoff and retry.
			interval *= 2
			if interval > maxPollInterval {
				interval = maxPollInterval
			}
		}
	}
}

// wpsGetResult calls GetResult and parses the wps:Result body.
func (b *WPSBackend) wpsGetResult(ctx context.Context, service domain.ProcessingService, jobID string) ([]OutputResult, error) {
	resultURL := wpsJobURL(service.URL, "GetResult", jobID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resultURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building GetResult request: %w", err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("WPS GetResult: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading GetResult body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("WPS GetResult returned status %d: %s", resp.StatusCode, string(body))
	}

	return b.parseWPSResult(body)
}

// parseWPSResult parses a wps:Result document into a slice of OutputResult.
func (b *WPSBackend) parseWPSResult(body []byte) ([]OutputResult, error) {
	var result wpsResultResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parsing WPS Result XML: %w", err)
	}

	outputs := make([]OutputResult, 0, len(result.Outputs))
	for _, out := range result.Outputs {
		r, err := wpsOutputToResult(out)
		if err != nil {
			b.log.Warnw("skipping unparseable WPS output", "id", out.ID, zap.Error(err))
			continue
		}
		outputs = append(outputs, r)
	}
	return outputs, nil
}

// wpsOutputToResult converts a parsed wpsOutputElement to an OutputResult.
func wpsOutputToResult(out wpsOutputElement) (OutputResult, error) {
	if out.Reference != nil && out.Reference.Href != "" {
		return OutputResult{
			OutputID:  out.ID,
			Reference: out.Reference.Href,
		}, nil
	}

	if out.Data != nil {
		if out.Data.LiteralData != nil {
			return OutputResult{
				OutputID:    out.ID,
				Value:       []byte(out.Data.LiteralData.Value),
				ContentType: "text/plain",
			}, nil
		}
		if out.Data.ComplexData != nil {
			ct := out.Data.ComplexData.MimeType
			if ct == "" {
				ct = "application/octet-stream"
			}
			return OutputResult{
				OutputID:    out.ID,
				Value:       []byte(out.Data.ComplexData.Value),
				ContentType: ct,
			}, nil
		}
	}

	return OutputResult{}, fmt.Errorf("output %q has no data or reference", out.ID)
}

// ---------------------------------------------------------------------------
// Translation helpers: WPS XML → OGC API JSON Schema
// ---------------------------------------------------------------------------

// wpsInputToOGCAPI converts a wpsInput to the OGC API input descriptor map.
func wpsInputToOGCAPI(inp wpsInput) map[string]any {
	entry := map[string]any{
		"title":  inp.Title,
		"schema": wpsInputSchema(inp),
	}
	if inp.Abstract != "" {
		entry["description"] = inp.Abstract
	}
	return entry
}

// wpsOutputToOGCAPI converts a wpsOutput to the OGC API output descriptor map.
func wpsOutputToOGCAPI(out wpsOutput) map[string]any {
	entry := map[string]any{
		"title":  out.Title,
		"schema": wpsOutputSchema(out),
	}
	if out.Abstract != "" {
		entry["description"] = out.Abstract
	}
	return entry
}

// wpsInputSchema derives the OGC API JSON Schema object for a WPS input.
func wpsInputSchema(inp wpsInput) map[string]any {
	if inp.BoundingBoxData != nil {
		return bboxSchema()
	}
	if inp.ComplexData != nil {
		return complexDataSchema(inp.ComplexData)
	}
	if inp.LiteralData != nil {
		return literalDataSchema(inp.LiteralData)
	}
	// No type declared — default to string.
	return map[string]any{"type": "string"}
}

// wpsOutputSchema derives the OGC API JSON Schema object for a WPS output.
func wpsOutputSchema(out wpsOutput) map[string]any {
	if out.BoundingBoxData != nil {
		return bboxSchema()
	}
	if out.ComplexData != nil {
		return complexDataSchema(out.ComplexData)
	}
	if out.LiteralData != nil {
		return literalDataSchema(out.LiteralData)
	}
	return map[string]any{"type": "string"}
}

func bboxSchema() map[string]any {
	return map[string]any{
		"type":     "array",
		"format":   "bbox",
		"items":    map[string]any{"type": "number"},
		"minItems": 4,
		"maxItems": 6,
	}
}

func complexDataSchema(cd *wpsComplexData) map[string]any {
	for _, f := range cd.Formats {
		mt := strings.ToLower(f.MimeType)
		if strings.Contains(mt, "geo+json") || strings.Contains(mt, "geojson") {
			return map[string]any{"type": "object", "format": "geojson"}
		}
	}
	return map[string]any{"type": "object"}
}

func literalDataSchema(ld *wpsLiteralData) map[string]any {
	// Prefer the reference attribute (URI like xs:double) over text content.
	dt := ld.DataType.Reference
	if dt == "" {
		dt = ld.DataType.Value
	}
	schema := map[string]any{"type": literalDataType(dt)}

	if len(ld.AllowedValues.Values) > 0 {
		enum := make([]any, len(ld.AllowedValues.Values))
		for i, v := range ld.AllowedValues.Values {
			enum[i] = v
		}
		schema["enum"] = enum
	}
	return schema
}

// literalDataType maps a WPS DataType string to an OGC API JSON Schema type.
// The input may be a bare name ("double"), an xs:-prefixed name ("xs:double"),
// or a full URI ("http://www.w3.org/2001/XMLSchema#double").
func literalDataType(dt string) string {
	// Strip namespace prefix or URI path to get the local name.
	if i := strings.LastIndexAny(dt, ":#/"); i >= 0 {
		dt = dt[i+1:]
	}
	switch strings.ToLower(dt) {
	case "float", "double", "decimal":
		return "number"
	case "integer", "long", "int", "short":
		return "integer"
	case "boolean":
		return "boolean"
	default:
		return "string"
	}
}

// ---------------------------------------------------------------------------
// WPS 1.0 translation helpers: XML → OGC API JSON Schema
// ---------------------------------------------------------------------------

func wps1InputToOGCAPI(inp wps1Input) map[string]any {
	entry := map[string]any{
		"title":  inp.Title,
		"schema": wps1InputSchema(inp),
	}
	if inp.Abstract != "" {
		entry["description"] = inp.Abstract
	}
	return entry
}

func wps1OutputToOGCAPI(out wps1Output) map[string]any {
	entry := map[string]any{
		"title":  out.Title,
		"schema": wps1OutputSchema(out),
	}
	if out.Abstract != "" {
		entry["description"] = out.Abstract
	}
	return entry
}

func wps1InputSchema(inp wps1Input) map[string]any {
	if inp.BoundingBoxData != nil {
		return bboxSchema()
	}
	if inp.ComplexData != nil {
		return wps1ComplexDataSchema(&inp.ComplexData.Default.Format)
	}
	if inp.LiteralData != nil {
		return wps1LiteralDataSchema(inp.LiteralData)
	}
	return map[string]any{"type": "string"}
}

func wps1OutputSchema(out wps1Output) map[string]any {
	if out.BBoxOutput != nil {
		return bboxSchema()
	}
	if out.ComplexOutput != nil {
		return wps1ComplexDataSchema(&out.ComplexOutput.Default.Format)
	}
	if out.LiteralOutput != nil {
		return wps1LiteralOutputSchema(out.LiteralOutput)
	}
	return map[string]any{"type": "string"}
}

func wps1ComplexDataSchema(f *wps1Format) map[string]any {
	mt := strings.ToLower(f.MimeType)
	if strings.Contains(mt, "geo+json") || strings.Contains(mt, "geojson") {
		return map[string]any{"type": "object", "format": "geojson"}
	}
	return map[string]any{"type": "object"}
}

func wps1LiteralDataSchema(ld *wps1LiteralData) map[string]any {
	dt := ld.DataType.Reference
	if dt == "" {
		dt = ld.DataType.Value
	}
	schema := map[string]any{"type": literalDataType(dt)}
	if ld.AllowedValues != nil && len(ld.AllowedValues.Values) > 0 {
		enum := make([]any, len(ld.AllowedValues.Values))
		for i, v := range ld.AllowedValues.Values {
			enum[i] = v
		}
		schema["enum"] = enum
	}
	return schema
}

func wps1LiteralOutputSchema(lo *wps1LiteralOutput) map[string]any {
	dt := lo.DataType.Reference
	if dt == "" {
		dt = lo.DataType.Value
	}
	return map[string]any{"type": literalDataType(dt)}
}

func parseWPS1ProcessDescription(body []byte, processID string) (*ProcessDescription, error) {
	var descs wps1ProcessDescriptions
	if err := xml.Unmarshal(body, &descs); err != nil {
		return nil, fmt.Errorf("decoding WPS 1.0 ProcessDescriptions XML: %w", err)
	}

	var pd *wps1ProcessDescription
	for i := range descs.Processes {
		if descs.Processes[i].Identifier.Value == processID {
			pd = &descs.Processes[i]
			break
		}
	}
	if pd == nil {
		if len(descs.Processes) > 0 {
			pd = &descs.Processes[0]
		} else {
			return nil, fmt.Errorf("process %q not found in WPS 1.0 DescribeProcess response", processID)
		}
	}

	jco := []string{"sync-execute"}
	if pd.StoreSupported && pd.StatusSupported {
		jco = []string{"async-execute", "sync-execute"}
	}

	inputsMap := make(map[string]any, len(pd.DataInputs.Inputs))
	for _, inp := range pd.DataInputs.Inputs {
		inputsMap[inp.Identifier.Value] = wps1InputToOGCAPI(inp)
	}
	outputsMap := make(map[string]any, len(pd.ProcessOutputs.Outputs))
	for _, out := range pd.ProcessOutputs.Outputs {
		outputsMap[out.Identifier.Value] = wps1OutputToOGCAPI(out)
	}

	inputsJSON, err := json.Marshal(inputsMap)
	if err != nil {
		return nil, fmt.Errorf("encoding inputs schema: %w", err)
	}
	outputsJSON, err := json.Marshal(outputsMap)
	if err != nil {
		return nil, fmt.Errorf("encoding outputs schema: %w", err)
	}

	return &ProcessDescription{
		Title:             pd.Title,
		Description:       pd.Abstract,
		Version:           "",
		JobControlOptions: jco,
		Inputs:            json.RawMessage(inputsJSON),
		Outputs:           json.RawMessage(outputsJSON),
	}, nil
}
