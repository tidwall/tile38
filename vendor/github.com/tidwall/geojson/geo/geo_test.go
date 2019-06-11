// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package geo

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func init() {
	seed := time.Now().UnixNano()
	//seed = 1540656736244531000
	println(seed)
	rand.Seed(seed)
}

func TestGeoCalc(t *testing.T) {
	dist := 172853.26908429610193707048892974853515625
	bearing := 320.8560640269032546711969189345836639404296875
	latA, lonA := 33.112, -112.123
	latB, lonB := 34.312, -113.311
	// DistanceTo
	value := DistanceTo(latA, lonA, latB, lonB)
	if value != dist {
		t.Fatalf("expected '%v', got '%v'", dist, value)
	}
	// BearingTo
	value = BearingTo(latA, lonA, latB, lonB)
	if value != bearing {
		t.Fatalf("expected '%v', got '%v'", bearing, value)
	}
	// DestinationPoint
	value1, value2 := DestinationPoint(latA, lonA, dist, bearing)
	if value1 != latB {
		t.Fatalf("expected '%v', got '%v'", latB, value1)
	}
	if value2 != lonB {
		t.Fatalf("expected '%v', got '%v'", lonB, value2)
	}

	// RectFromCenter
	// testing case where given search radius is larger than Earth's radius
	var (
		minLat float64 = -90
		minLon float64 = -180
		maxLat float64 = 90
		maxLon float64 = 180
	)
	value3, value4, value5, value6 := RectFromCenter(latA, lonA, earthRadius+1)
	if value3 != minLat &&
		value4 != minLon &&
		value5 != maxLat &&
		value6 != maxLon {
		t.Fatalf("expected '%v, %v, %v, %v', got '%v, %v, %v, %v'",
			minLat, minLon, maxLat, maxLon,
			value3, value4, value5, value6)
	}
}

func TestHaversine(t *testing.T) {
	latA := rand.Float64()*180 - 90
	lonA := rand.Float64()*360 - 180
	start := time.Now()
	for time.Since(start) < time.Second/4 {
		for i := 0; i < 1000; i++ {
			latB := rand.Float64()*180 - 90
			lonB := rand.Float64()*360 - 180
			latC := rand.Float64()*180 - 90
			lonC := rand.Float64()*360 - 180
			haver1 := Haversine(latA, lonA, latB, lonB)
			haver2 := Haversine(latA, lonA, latC, lonC)
			meters1 := DistanceTo(latA, lonA, latB, lonB)
			meters2 := DistanceTo(latA, lonA, latC, lonC)
			switch {
			case haver1 < haver2:
				if meters1 >= meters2 {
					t.Fatalf("failed")
				}
			case haver1 == haver2:
				if meters1 != meters2 {
					t.Fatalf("failed")
				}
			case haver1 > haver2:
				if meters1 <= meters2 {
					t.Fatalf("failed")
				}
			}
		}
	}
}

func TestNormalizeDistance(t *testing.T) {
	start := time.Now()
	for time.Since(start) < time.Second {
		for i := 0; i < 1000; i++ {
			meters1 := rand.Float64() * earthRadius * 3 // wrap three times
			meters2 := NormalizeDistance(meters1)
			dist1 := math.Floor(DistanceToHaversine(meters2) * 1e8)
			dist2 := math.Floor(DistanceToHaversine(meters1) * 1e8)
			if dist1 != dist2 {
				t.Fatalf("expected %f, got %f", dist2, dist1)
			}
		}
	}
}

type point struct {
	lat, lon float64
}

func BenchmarkHaversine(b *testing.B) {
	pointA := point{
		lat: rand.Float64()*180 - 90,
		lon: rand.Float64()*360 - 180,
	}
	points := make([]point, b.N)
	for i := 0; i < b.N; i++ {
		points[i].lat = rand.Float64()*180 - 90
		points[i].lon = rand.Float64()*360 - 180
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Haversine(pointA.lat, pointA.lon, points[i].lat, points[i].lon)
	}
}

func BenchmarkDistanceTo(b *testing.B) {
	pointA := point{
		lat: rand.Float64()*180 - 90,
		lon: rand.Float64()*360 - 180,
	}
	points := make([]point, b.N)
	for i := 0; i < b.N; i++ {
		points[i].lat = rand.Float64()*180 - 90
		points[i].lon = rand.Float64()*360 - 180
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DistanceTo(pointA.lat, pointA.lon, points[i].lat, points[i].lon)
	}
}
