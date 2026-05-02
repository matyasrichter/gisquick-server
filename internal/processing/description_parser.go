package processing

import (
	"encoding/json"
	"strings"

	"github.com/gisquick/gisquick-server/internal/domain"
)

// extractInputFormats parses a stored OGC API process description and returns
// the accepted media types for each input in declaration order.
// Both OGC API (contentMediaType / oneOf schemas) and WPS-translated
// (contentMediaTypes array) formats are handled.
func extractInputFormats(descriptionJSON json.RawMessage) map[string]domain.InputFormat {
	if len(descriptionJSON) == 0 {
		return nil
	}
	var desc ProcessDescription
	if err := json.Unmarshal(descriptionJSON, &desc); err != nil || len(desc.Inputs) == 0 {
		return nil
	}

	var inputsMap map[string]json.RawMessage
	if err := json.Unmarshal(desc.Inputs, &inputsMap); err != nil {
		return nil
	}

	result := make(map[string]domain.InputFormat, len(inputsMap))
	for id, inputRaw := range inputsMap {
		var inputDef struct {
			Schema json.RawMessage `json:"schema"`
		}
		if err := json.Unmarshal(inputRaw, &inputDef); err != nil || len(inputDef.Schema) == 0 {
			continue
		}
		mediaTypes := collectMediaTypes(inputDef.Schema)
		if len(mediaTypes) > 0 {
			result[id] = domain.InputFormat{AcceptedMediaTypes: mediaTypes}
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// collectMediaTypes recursively walks a JSON Schema object and collects all
// contentMediaType / contentMediaTypes / format values in depth-first document order.
func collectMediaTypes(schemaJSON json.RawMessage) []string {
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return nil
	}

	seen := make(map[string]bool)
	var result []string
	addIfNew := func(mt string) {
		if mt != "" && !seen[mt] {
			seen[mt] = true
			result = append(result, mt)
		}
	}

	// Direct contentMediaType (OGC API singular form inside oneOf branches)
	if raw, ok := schema["contentMediaType"]; ok {
		var mt string
		if json.Unmarshal(raw, &mt) == nil {
			addIfNew(mt)
		}
	}

	// contentMediaTypes (plural array — WPS-translated schema)
	if raw, ok := schema["contentMediaTypes"]; ok {
		var mts []any
		if json.Unmarshal(raw, &mts) == nil {
			for _, v := range mts {
				if s, ok := v.(string); ok {
					addIfNew(s)
				}
			}
		}
	}

	// format shorthand → MIME type (e.g. "geojson-geometry" → "application/geo+json")
	if raw, ok := schema["format"]; ok {
		var format string
		if json.Unmarshal(raw, &format) == nil {
			if mt := formatToMIME(format); mt != "" {
				addIfNew(mt)
			}
		}
	}

	// Recurse into oneOf / anyOf / allOf in document order
	for _, key := range []string{"oneOf", "anyOf", "allOf"} {
		if raw, ok := schema[key]; ok {
			var items []json.RawMessage
			if json.Unmarshal(raw, &items) == nil {
				for _, item := range items {
					for _, mt := range collectMediaTypes(item) {
						addIfNew(mt)
					}
				}
			}
		}
	}

	return result
}

// formatToMIME maps OGC API format shorthand values to their canonical MIME types.
func formatToMIME(format string) string {
	switch strings.ToLower(format) {
	case "geojson", "geojson-geometry", "geojson-feature", "geojson-featurecollection":
		return "application/geo+json"
	case "gml", "gml3", "gml32":
		return "application/gml+xml"
	case "wkt", "ewkt":
		return "text/plain"
	}
	return ""
}
