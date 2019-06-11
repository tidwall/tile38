// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package geo

import (
	"math"
)

const (
	earthRadius = 6371e3
	radians     = math.Pi / 180
	degrees     = 180 / math.Pi
	piR         = math.Pi * earthRadius
	twoPiR      = 2 * piR
)

// Haversine ...
func Haversine(latA, lonA, latB, lonB float64) float64 {
	φ1 := latA * radians
	λ1 := lonA * radians
	φ2 := latB * radians
	λ2 := lonB * radians
	Δφ := φ2 - φ1
	Δλ := λ2 - λ1
	sΔφ2 := math.Sin(Δφ / 2)
	sΔλ2 := math.Sin(Δλ / 2)
	return sΔφ2*sΔφ2 + math.Cos(φ1)*math.Cos(φ2)*sΔλ2*sΔλ2
}

// NormalizeDistance ...
func NormalizeDistance(meters float64) float64 {
	m1 := math.Mod(meters, twoPiR)
	if m1 <= piR {
		return m1
	}
	return twoPiR - m1
}

// DistanceToHaversine ...
func DistanceToHaversine(meters float64) float64 {
	// convert the given distance to its haversine
	sin := math.Sin(0.5 * meters / earthRadius)
	return sin * sin
}

// DistanceFromHaversine ...
func DistanceFromHaversine(haversine float64) float64 {
	return earthRadius * 2 * math.Asin(math.Sqrt(haversine))
}

// DistanceTo return the distance in meters between two point.
func DistanceTo(latA, lonA, latB, lonB float64) (meters float64) {
	a := Haversine(latA, lonA, latB, lonB)
	return DistanceFromHaversine(a)
}

// DestinationPoint return the destination from a point based on a
// distance and bearing.
func DestinationPoint(lat, lon, meters, bearingDegrees float64) (
	destLat, destLon float64,
) {
	// see http://williams.best.vwh.net/avform.htm#LL
	δ := meters / earthRadius // angular distance in radians
	θ := bearingDegrees * radians
	φ1 := lat * radians
	λ1 := lon * radians
	φ2 := math.Asin(math.Sin(φ1)*math.Cos(δ) +
		math.Cos(φ1)*math.Sin(δ)*math.Cos(θ))
	λ2 := λ1 + math.Atan2(math.Sin(θ)*math.Sin(δ)*math.Cos(φ1),
		math.Cos(δ)-math.Sin(φ1)*math.Sin(φ2))
	λ2 = math.Mod(λ2+3*math.Pi, 2*math.Pi) - math.Pi // normalise to -180..+180°
	return φ2 * degrees, λ2 * degrees
}

// BearingTo returns the (initial) bearing from point 'A' to point 'B'.
func BearingTo(latA, lonA, latB, lonB float64) float64 {
	// tanθ = sinΔλ⋅cosφ2 / cosφ1⋅sinφ2 − sinφ1⋅cosφ2⋅cosΔλ
	// see mathforum.org/library/drmath/view/55417.html for derivation

	φ1 := latA * radians
	φ2 := latB * radians
	Δλ := (lonB - lonA) * radians
	y := math.Sin(Δλ) * math.Cos(φ2)
	x := math.Cos(φ1)*math.Sin(φ2) - math.Sin(φ1)*math.Cos(φ2)*math.Cos(Δλ)
	θ := math.Atan2(y, x)

	return math.Mod(θ*degrees+360, 360)
}

// RectFromCenter calculates the bounding box surrounding a circle.
func RectFromCenter(lat, lon, meters float64) (
	minLat, minLon, maxLat, maxLon float64,
) {

	// Skip all calculations if meters (i.e., provided radius) is larger than radius of Earth and return rectangle containing entire Earth
	if meters > earthRadius {
		return -90, -180, 90, 180
	}

	// see http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#Latitude
	lat *= radians
	lon *= radians

	r := meters / earthRadius // angular radius

	minLat = lat - r
	maxLat = lat + r

	latT := math.Asin(math.Sin(lat) / math.Cos(r))
	lonΔ := math.Acos((math.Cos(r) - math.Sin(latT)*math.Sin(lat)) / (math.Cos(latT) * math.Cos(lat)))

	minLon = lon - lonΔ
	maxLon = lon + lonΔ

	// Adjust for north poll
	if maxLat > math.Pi/2 {
		minLon = -math.Pi
		maxLat = math.Pi / 2
		maxLon = math.Pi
	}

	// Adjust for south poll
	if minLat < -math.Pi/2 {
		minLat = -math.Pi / 2
		minLon = -math.Pi
		maxLon = math.Pi
	}

	// Adjust for wraparound. Remove this if the commented-out condition below this block is added.
	if minLon < -math.Pi || maxLon > math.Pi {
		minLon = -math.Pi
		maxLon = math.Pi
	}

	/*
	   	// Consider splitting area into two bboxes, using the below checks, and erasing above block for performance. See http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#PolesAnd180thMeridian
	   	// Adjust for wraparound if minimum longitude is less than -180 degrees.
	   	if lonMin < -math.Pi {
	   // box 1:
	   		latMin = latMin
	   		latMax = latMax
	   		lonMin += 2*math.Pi
	   		lonMax = math.Pi
	   // box 2:
	   		latMin = latMin
	   		latMax = latMax
	   		lonMin = -math.Pi
	   		lonMax = lonMax
	   	}
	   	// Adjust for wraparound if maximum longitude is greater than 180 degrees.
	   	if lonMax > math.Pi {
	   // box 1:
	   		latMin = latMin
	   		latMax = latMax
	   		lonMin = lonMin
	   		lonMax = -math.Pi
	   // box 2:
	   		latMin = latMin
	   		latMax = latMax
	   		lonMin = -math.Pi
	   		lonMax -= 2*math.Pi
	   	}
	*/

	minLon = math.Mod(minLon+3*math.Pi, 2*math.Pi) - math.Pi // normalise to -180..+180°
	maxLon = math.Mod(maxLon+3*math.Pi, 2*math.Pi) - math.Pi

	minLat *= degrees
	minLon *= degrees
	maxLat *= degrees
	maxLon *= degrees
	return

}
