package processing

import (
	"encoding/json"
	"fmt"
	"mime"
	"strings"

	"github.com/lukeroth/gdal"
)

// GeometryEncoder converts a GeoJSON geometry value to a specific encoding.
type GeometryEncoder interface {
	// Matches reports whether this encoder handles the given MIME type.
	Matches(mediaType string) bool
	// Encode converts a GeoJSON geometry to the target encoding.
	// The input must be a GeoJSON geometry, Feature, or FeatureCollection object.
	Encode(geojson json.RawMessage) ([]byte, error)
}

// encoderRegistry is an ordered list of encoders. FirstMatch selects the first
// encoder that accepts any of the given MIME types.
type encoderRegistry []GeometryEncoder

// FirstMatch returns the first encoder that matches any of the accepted media types
// in the order they are declared. Returns passthroughEncoder when no match is found.
func (r encoderRegistry) FirstMatch(accepted []string) GeometryEncoder {
	for _, mediaType := range accepted {
		for _, enc := range r {
			if enc.Matches(mediaType) {
				return enc
			}
		}
	}
	return passthroughEncoder{}
}

// defaultRegistry is the package-level encoder registry used at execute time.
var defaultRegistry = encoderRegistry{&GMLEncoder{}, &WKTEncoder{}, &GeoJSONEncoder{}}

// passthroughEncoder returns the input unchanged. Used as the fallback.
type passthroughEncoder struct{}

func (e passthroughEncoder) Matches(_ string) bool { return false }
func (e passthroughEncoder) Encode(geojson json.RawMessage) ([]byte, error) {
	return []byte(geojson), nil
}

// GeoJSONEncoder matches GeoJSON media types and returns the value unchanged.
type GeoJSONEncoder struct{}

func (e *GeoJSONEncoder) Matches(mediaType string) bool {
	base, _, _ := mime.ParseMediaType(mediaType)
	return base == "application/geo+json" || base == "application/json"
}

func (e *GeoJSONEncoder) Encode(geojson json.RawMessage) ([]byte, error) {
	return []byte(geojson), nil
}

// GMLEncoder matches GML media types and converts GeoJSON to GML3 using GDAL.
type GMLEncoder struct{}

func (e *GMLEncoder) Matches(mediaType string) bool {
	base, params, _ := mime.ParseMediaType(mediaType)
	if base == "application/gml+xml" {
		return true
	}
	if base == "text/xml" {
		return strings.HasPrefix(strings.ToLower(params["subtype"]), "gml")
	}
	return false
}

func (e *GMLEncoder) Encode(geojson json.RawMessage) ([]byte, error) {
	geomJSON := extractGeometryJSON(geojson)
	geom := gdal.CreateFromJson(string(geomJSON))
	defer geom.Destroy()
	gml := geom.ToGML_Ex([]string{"NAMESPACE_DECL=YES"})
	if gml == "" {
		return nil, fmt.Errorf("GDAL failed to convert GeoJSON to GML")
	}
	return []byte(gml), nil
}

// WKTEncoder matches WKT/text media types and converts GeoJSON to WKT using GDAL.
type WKTEncoder struct{}

func (e *WKTEncoder) Matches(mediaType string) bool {
	base, params, _ := mime.ParseMediaType(mediaType)
	if base != "text/plain" {
		return false
	}
	sub := strings.ToLower(params["subtype"])
	return sub == "" || sub == "wkt"
}

func (e *WKTEncoder) Encode(geojson json.RawMessage) ([]byte, error) {
	geomJSON := extractGeometryJSON(geojson)
	geom := gdal.CreateFromJson(string(geomJSON))
	defer geom.Destroy()
	wkt, err := geom.ToWKT()
	if err != nil {
		return nil, fmt.Errorf("GDAL failed to convert GeoJSON to WKT: %w", err)
	}
	return []byte(wkt), nil
}

// extractGeometryJSON extracts the geometry part from a GeoJSON Feature or
// FeatureCollection so it can be passed to GDAL's CreateFromJson.
// Plain geometry objects are returned unchanged.
func extractGeometryJSON(raw json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	var typeStr string
	if err := json.Unmarshal(obj["type"], &typeStr); err != nil {
		return raw
	}
	switch typeStr {
	case "Feature":
		if geomRaw, ok := obj["geometry"]; ok {
			return geomRaw
		}
	case "FeatureCollection":
		var fc struct {
			Features []struct {
				Geometry json.RawMessage `json:"geometry"`
			} `json:"features"`
		}
		if err := json.Unmarshal(raw, &fc); err != nil {
			return raw
		}
		geoms := make([]json.RawMessage, 0, len(fc.Features))
		for _, f := range fc.Features {
			if len(f.Geometry) > 0 && string(f.Geometry) != "null" {
				geoms = append(geoms, f.Geometry)
			}
		}
		if len(geoms) == 1 {
			return geoms[0]
		}
		gc, err := json.Marshal(map[string]any{
			"type":       "GeometryCollection",
			"geometries": geoms,
		})
		if err != nil {
			return raw
		}
		return json.RawMessage(gc)
	}
	return raw
}
