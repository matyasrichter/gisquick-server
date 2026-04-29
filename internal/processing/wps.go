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
// WPSBackend
// ---------------------------------------------------------------------------

// WPSBackend implements ProcessingBackend for OGC WPS 2.0.2 services.
type WPSBackend struct {
	client *http.Client
	log    *zap.SugaredLogger
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

// FetchProcessList calls GetCapabilities and returns a ProcessSummary slice.
func (b *WPSBackend) FetchProcessList(ctx context.Context, service domain.ProcessingService) ([]ProcessSummary, error) {
	url := wpsQueryURL(service.URL, "GetCapabilities", "")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

	var caps wpsCapabilities
	if err := xml.NewDecoder(resp.Body).Decode(&caps); err != nil {
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
	b.log.Debugw("WPS GetCapabilities", "service", service.URL, "count", len(summaries))
	return summaries, nil
}

// DescribeProcess calls DescribeProcess and translates the WPS XML to a
// ProcessDescription whose Inputs/Outputs fields carry OGC API JSON schemas.
func (b *WPSBackend) DescribeProcess(ctx context.Context, service domain.ProcessingService, processID string) (*ProcessDescription, error) {
	url := wpsQueryURL(service.URL, "DescribeProcess", processID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

	var offerings wpsProcessOfferings
	if err := xml.NewDecoder(resp.Body).Decode(&offerings); err != nil {
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

	b.log.Debugw("WPS DescribeProcess", "service", service.URL, "processID", processID)

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
		results, err := b.wpsPollandFetch(ctx, service, si.JobID)
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
		Outputs:  []wpsOutputRequest{{ID: "*", Transmission: "value"}},
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
			lower := fmt.Sprintf("%g %g", nums[0], nums[1])
			upper := fmt.Sprintf("%g %g", nums[2], nums[3])
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

// wpsPollandFetch polls GetStatus until the job succeeds or fails, then calls
// GetResult and parses the output.
func (b *WPSBackend) wpsPollandFetch(ctx context.Context, service domain.ProcessingService, jobID string) ([]OutputResult, error) {
	interval := initialPollInterval

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
