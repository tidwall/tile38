package collection

import "math"

func geodeticDistAlgo[T any](center [2]float64) (
	algo func(min, max [2]float64, data T, item bool) (dist float64),
) {
	const earthRadius = 6371e3
	return func(min, max [2]float64, data T, item bool) (dist float64) {
		return earthRadius * pointRectDistGeodeticDeg(
			center[1], center[0],
			min[1], min[0],
			max[1], max[0],
		)
	}
}

func pointRectDistGeodeticDeg(pLat, pLng, minLat, minLng, maxLat, maxLng float64) float64 {
	result := pointRectDistGeodeticRad(
		pLat*math.Pi/180, pLng*math.Pi/180,
		minLat*math.Pi/180, minLng*math.Pi/180,
		maxLat*math.Pi/180, maxLng*math.Pi/180,
	)
	return result
}

func pointRectDistGeodeticRad(φq, λq, φl, λl, φh, λh float64) float64 {
	// Algorithm from:
	// Schubert, E., Zimek, A., & Kriegel, H.-P. (2013).
	// Geodetic Distance Queries on R-Trees for Indexing Geographic Data.
	// Lecture Notes in Computer Science, 146–164.
	// doi:10.1007/978-3-642-40235-7_9
	const (
		twoΠ  = 2 * math.Pi
		halfΠ = math.Pi / 2
	)

	// distance on the unit sphere computed using Haversine formula
	distRad := func(φa, λa, φb, λb float64) float64 {
		if φa == φb && λa == λb {
			return 0
		}

		Δφ := φa - φb
		Δλ := λa - λb
		sinΔφ := math.Sin(Δφ / 2)
		sinΔλ := math.Sin(Δλ / 2)
		cosφa := math.Cos(φa)
		cosφb := math.Cos(φb)

		return 2 * math.Asin(math.Sqrt(sinΔφ*sinΔφ+sinΔλ*sinΔλ*cosφa*cosφb))
	}

	// Simple case, point or invalid rect
	if φl >= φh && λl >= λh {
		return distRad(φl, λl, φq, λq)
	}

	if λl <= λq && λq <= λh {
		// q is between the bounding meridians of r
		// hence, q is north, south or within r
		if φl <= φq && φq <= φh { // Inside
			return 0
		}

		if φq < φl { // South
			return φl - φq
		}

		return φq - φh // North
	}

	// determine if q is closer to the east or west edge of r to select edge for
	// tests below
	Δλe := λl - λq
	Δλw := λq - λh
	if Δλe < 0 {
		Δλe += twoΠ
	}
	if Δλw < 0 {
		Δλw += twoΠ
	}
	var Δλ float64    // distance to closest edge
	var λedge float64 // longitude of closest edge
	if Δλe <= Δλw {
		Δλ = Δλe
		λedge = λl
	} else {
		Δλ = Δλw
		λedge = λh
	}

	sinΔλ, cosΔλ := math.Sincos(Δλ)
	tanφq := math.Tan(φq)

	if Δλ >= halfΠ {
		// If Δλ > 90 degrees (1/2 pi in radians) we're in one of the corners
		// (NW/SW or NE/SE depending on the edge selected). Compare against the
		// center line to decide which case we fall into
		φmid := (φh + φl) / 2
		if tanφq >= math.Tan(φmid)*cosΔλ {
			return distRad(φq, λq, φh, λedge) // North corner
		}
		return distRad(φq, λq, φl, λedge) // South corner
	}

	if tanφq >= math.Tan(φh)*cosΔλ {
		return distRad(φq, λq, φh, λedge) // North corner
	}

	if tanφq <= math.Tan(φl)*cosΔλ {
		return distRad(φq, λq, φl, λedge) // South corner
	}

	// We're to the East or West of the rect, compute distance using cross-track
	// Note that this is a simplification of the cross track distance formula
	// valid since the track in question is a meridian.
	return math.Asin(math.Cos(φq) * sinΔλ)
}
