package server

import (
	"math/rand"
	"strconv"
	"testing"

	a5 "github.com/a5geo/a5-go"
)

func TestA5DecodeCell(t *testing.T) {
	// Cell 51575d8000000000 is the resolution 10 cell holding POINT 52 13.
	for _, tt := range []struct {
		name string
		in   string
		want uint64
		ok   bool
	}{
		{"res10", "51575d8000000000", 0x51575d8000000000, true},
		{"res0", "1200000000000000", 0x1200000000000000, true},
		{"hexprefix", "0x51575d8000000000", 0x51575d8000000000, true},
		{"uppercase", "51575D8000000000", 0x51575d8000000000, true},
		{"nothex", "nothex", 0, false},
		{"empty", "", 0, false},
		// Hex-parseable but structurally invalid: these used to reach a5-go
		// and panic the server.
		{"noresmarker", "1", 0, false},
		{"worldcell", "0", 0, false},
		{"negativeres", "4000000000000000", 0, false},
		{"originoutofrange", "fc00000000000001", 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := a5DecodeCell(tt.in)
			if tt.ok != (err == nil) {
				t.Fatalf("a5DecodeCell(%q): err = %v, want ok = %v", tt.in, err, tt.ok)
			}
			if tt.ok && got != tt.want {
				t.Errorf("a5DecodeCell(%q) = %x, want %x", tt.in, got, tt.want)
			}
		})
	}
}

// TestA5EncodeDecodeRoundTrip pins the other side of the validity check: every
// cell a5EncodePoint produces must survive a5DecodeCell, or a validator that is
// too strict silently breaks SET key id A5 <cell>.
func TestA5EncodeDecodeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for res := 0; res <= a5MaxResolution; res++ {
		for range 200 {
			lon := rng.Float64()*360 - 180
			lat := rng.Float64()*180 - 90
			cell, err := a5EncodePoint(lon, lat, res)
			if err != nil {
				t.Fatalf("a5EncodePoint(%v, %v, %d): %v", lon, lat, res, err)
			}
			if _, err := a5DecodeCell(cell); err != nil {
				t.Fatalf("a5DecodeCell(%q) from lon %v lat %v res %d: %v",
					cell, lon, lat, res, err)
			}
		}
	}
}

// TestA5DecodeCellRejectsPanics is the guard on a5DecodeCell being a trust
// boundary: a5-go panics on malformed cell IDs, so anything a5DecodeCell
// accepts must survive both of the calls that follow it in the command paths.
func TestA5DecodeCellRejectsPanics(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var accepted int
	for i := range 200000 {
		var cellID uint64
		switch i % 3 {
		case 0:
			cellID = rng.Uint64()
		case 1:
			// bias towards small values, where the resolution marker lands in
			// the low bits or is missing entirely
			cellID = uint64(rng.Intn(1 << 20))
		case 2:
			// bias towards the top 6 bits, which hold the origin and segment
			cellID = rng.Uint64() &^ (a5.RemovalMask >> 4)
		}
		s := strconv.FormatUint(cellID, 16)
		got, err := a5DecodeCell(s)
		if err != nil {
			continue
		}
		accepted++
		if got != cellID {
			t.Fatalf("a5DecodeCell(%q) = %x, want %x", s, got, cellID)
		}
		if _, err := a5CellPolygon(cellID); err != nil {
			t.Fatalf("a5CellPolygon(%x): %v", cellID, err)
		}
		a5.CellToLonLat(cellID)
	}
	// a sweep that rejects everything would pass vacuously
	if accepted < 1000 {
		t.Fatalf("only %d ids accepted, sweep proves nothing", accepted)
	}
	t.Logf("%d of 200000 ids accepted", accepted)
}
