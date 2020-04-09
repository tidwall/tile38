package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipFeature(
	feature *geojson.Feature, clipper geojson.Object,
	opts *geometry.IndexOptions,
) geojson.Object {
	newFeature := Clip(feature.Base(), clipper, opts)
	if _, ok := newFeature.(*geojson.Feature); !ok {
		newFeature = geojson.NewFeature(newFeature, feature.Members())
	}
	return newFeature
}
