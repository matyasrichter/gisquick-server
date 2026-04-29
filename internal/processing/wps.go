package processing

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

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
	DataType      string           `xml:"http://www.opengis.net/ows/1.1 DataType"`
	Default       string           `xml:"default,attr"`
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

// wpsQueryURL constructs a WPS 2.0.0 KVP request URL.
func wpsQueryURL(base, request, identifier string) string {
	u := base + "?service=WPS&version=2.0.0&request=" + request
	if identifier != "" {
		u += "&identifier=" + identifier
	}
	return u
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

// Execute is a stub — Task C handles WPS execution.
func (b *WPSBackend) Execute(_ context.Context, _ *JobRecord, _ domain.ProcessingService, _ json.RawMessage) ([]OutputResult, string, error) {
	return nil, "", fmt.Errorf("WPS execute not yet implemented")
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
		"maxItems": 4,
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
	schema := map[string]any{"type": literalDataType(ld.DataType)}

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
func literalDataType(dt string) string {
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
