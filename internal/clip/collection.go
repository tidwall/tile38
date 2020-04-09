package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipCollection(
	collection geojson.Collection, clipper geojson.Object,
	opts *geometry.IndexOptions,
) geojson.Object {
	var features []geojson.Object
	for _, feature := range collection.Children() {
		feature = Clip(feature, clipper, opts)
		if feature.Empty() {
			continue
		}
		if _, ok := feature.(*geojson.Feature); !ok {
			feature = geojson.NewFeature(feature, "")
		}
		features = append(features, feature)
	}
	return geojson.NewFeatureCollection(features)
}
