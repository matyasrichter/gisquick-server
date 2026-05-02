package processing

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const geoJSONPolygon = `{"type":"Polygon","coordinates":[[[9.255951,46.808252],[9.255772,46.808309],[9.255693,46.808278],[9.255449,46.80846],[9.255539,46.808617],[9.255136,46.808886],[9.255174,46.809199],[9.255951,46.808252]]]}`

func TestGMLEncoder_IncludesNamespaceDeclaration(t *testing.T) {
	enc := &GMLEncoder{}
	out, err := enc.Encode([]byte(geoJSONPolygon))
	require.NoError(t, err)

	gml := string(out)
	assert.Contains(t, gml, `xmlns:gml=`, "GML output must carry its own namespace declaration so it is valid as a standalone document")

	// The fragment must also be well-formed XML.
	var dummy struct {
		XMLName xml.Name
	}
	require.NoError(t, xml.Unmarshal(out, &dummy), "GML output must be parseable XML")
	assert.True(t, strings.Contains(gml, "Polygon"), "GML output must contain Polygon geometry")
}
