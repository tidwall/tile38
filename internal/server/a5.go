package server

import (
	"strconv"
	"strings"

	a5 "github.com/a5geo/a5-go"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

// A5 is a discrete global grid that partitions the world into equal-area
// pentagonal cells. Cell IDs are 64-bit integers and resolutions range from 0
// (whole-world faces) to 30 (sub-centimeter). These helpers bridge the a5-go
// library and tile38's geojson/geometry types, mirroring how internal/bing
// isolates the quadkey math.

// a5MaxResolution is the finest A5 resolution supported by the a5-go library.
const a5MaxResolution = 30

// a5ValidResolution reports whether res is a valid A5 resolution for encoding.
func a5ValidResolution(res int) bool {
	return res >= 0 && res <= a5MaxResolution
}

// a5EncodePoint encodes a lon/lat point to its containing A5 cell at the given
// resolution, returning the cell ID as a hexadecimal string (the A5 ecosystem
// convention).
func a5EncodePoint(lon, lat float64, res int) (string, error) {
	cellID, err := a5.LonLatToCell(a5.LonLat{lon, lat}, res)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(cellID, 16), nil
}

// a5DecodeCell parses an A5 cell ID string. Cell IDs are hexadecimal (the form
// produced by a5EncodePoint), optionally with a 0x prefix. Decoding is always
// base 16 so that all-digit IDs round-trip unambiguously with the encoder.
//
// Hex-parseable is not the same as valid, and the difference is a crash: a5-go
// panics instead of erroring on IDs whose resolution or origin bits are out of
// range. "1" carries no resolution marker, so a5.GetResolution reports -1 and
// a5.CellVertices reaches SToAnchor with a negative resolution. This is the
// trust boundary for cell IDs off the wire -- every caller of a5CellPolygon and
// a5.CellToLonLat parses through here first.
func a5DecodeCell(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	cellID, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, err
	}
	res := a5.GetResolution(cellID)
	if !a5ValidResolution(res) {
		return 0, errInvalidArgument(s)
	}
	// The top 6 bits hold the origin, or the origin and segment together once
	// there is a segment to hold (resolution 1 and finer). Only 12 of the 64
	// values they can spell are real dodecahedron faces.
	origin := int(cellID >> a5.HilbertStartBit)
	if res > 0 {
		origin /= 5
	}
	if origin >= len(a5.Origins) {
		return 0, errInvalidArgument(s)
	}
	return cellID, nil
}

// a5CellPolygon builds the pentagon boundary of an A5 cell as a geojson
// polygon, suitable for use as a query area in WITHIN/INTERSECTS.
func a5CellPolygon(cellID uint64) (geojson.Object, error) {
	verts := a5.CellVertices(cellID)
	if len(verts) < 3 {
		return nil, errInvalidArgument(strconv.FormatUint(cellID, 16))
	}
	ring := make([]geometry.Point, 0, len(verts)+1)
	for _, c := range verts {
		ll := a5.ToLonLat(a5.ToSpherical(c))
		ring = append(ring, geometry.Point{X: ll[0], Y: ll[1]})
	}
	// close the ring
	ring = append(ring, ring[0])
	return geojson.NewPolygon(geometry.NewPoly(ring, nil, nil)), nil
}
