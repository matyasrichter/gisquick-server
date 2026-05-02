package processing

import (
	"encoding/json"
	"fmt"

	"github.com/gisquick/gisquick-server/internal/domain"
)

// geojsonTypes is the set of GeoJSON type values that represent geometry data.
var geojsonTypes = map[string]bool{
	"Point": true, "LineString": true, "Polygon": true,
	"MultiPoint": true, "MultiLineString": true, "MultiPolygon": true,
	"GeometryCollection": true, "Feature": true, "FeatureCollection": true,
}

// isGeoJSONGeometry returns true when raw is a JSON object whose "type" field
// names a GeoJSON geometry, Feature, or FeatureCollection.
func isGeoJSONGeometry(raw json.RawMessage) bool {
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}
	var obj struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return false
	}
	return geojsonTypes[obj.Type]
}

// transformInputs converts geometry inputs in the execute payload from GeoJSON
// to the first format in each input's AcceptedMediaTypes that the registry has
// an encoder for. Non-geometry inputs and inputs absent from formats are left
// unchanged.
func transformInputs(payload json.RawMessage, formats map[string]domain.InputFormat, registry encoderRegistry) (json.RawMessage, error) {
	if len(formats) == 0 {
		return payload, nil
	}

	var wrapper struct {
		Inputs map[string]json.RawMessage `json:"inputs"`
	}
	if err := json.Unmarshal(payload, &wrapper); err != nil || wrapper.Inputs == nil {
		return payload, nil
	}

	changed := false
	for key, inputFmt := range formats {
		raw, ok := wrapper.Inputs[key]
		if !ok || !isGeoJSONGeometry(raw) {
			continue
		}
		enc := registry.FirstMatch(inputFmt.AcceptedMediaTypes)
		if _, isPassthrough := enc.(passthroughEncoder); isPassthrough {
			continue
		}
		encoded, err := enc.Encode(raw)
		if err != nil {
			return nil, fmt.Errorf("encoding input %q: %w", key, err)
		}
		// Non-JSON results (GML/WKT strings) must be wrapped as JSON strings.
		if len(encoded) > 0 && encoded[0] != '{' && encoded[0] != '[' {
			jsonStr, err := json.Marshal(string(encoded))
			if err != nil {
				return nil, fmt.Errorf("marshaling encoded input %q as JSON string: %w", key, err)
			}
			wrapper.Inputs[key] = json.RawMessage(jsonStr)
		} else {
			wrapper.Inputs[key] = json.RawMessage(encoded)
		}
		changed = true
	}

	if !changed {
		return payload, nil
	}

	out, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fmt.Errorf("re-marshaling transformed payload: %w", err)
	}
	return out, nil
}
