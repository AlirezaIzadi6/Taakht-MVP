// Package geo loads the eligibility file and measures distances between neighborhoods.
package geo

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
)

type Neighborhood struct {
	ID   string  `json:"id"`
	Name string  `json:"name"`
	City string  `json:"city"`
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
}

// Map holds neighborhood coordinates by id.
type Map map[string]Neighborhood

func Load(path string) (Map, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read eligibility file: %w", err)
	}
	var cfg struct {
		Neighborhoods []Neighborhood `json:"neighborhoods"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse eligibility file: %w", err)
	}
	m := Map{}
	for _, n := range cfg.Neighborhoods {
		m[n.ID] = n
	}
	return m, nil
}

const earthRadiusKm = 6371.0

// Haversine returns the great-circle distance in kilometers.
func Haversine(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Min(1, math.Sqrt(a)))
}

// MinDistanceKm returns the distance between the closest pair of known neighborhoods
// from a and b; ok is false when no pair has known coordinates.
func (m Map) MinDistanceKm(a, b []string) (km float64, ok bool) {
	best := math.Inf(1)
	for _, x := range a {
		nx, okx := m[x]
		if !okx {
			continue
		}
		for _, y := range b {
			if x == y {
				return 0, true
			}
			ny, oky := m[y]
			if !oky {
				continue
			}
			if d := Haversine(nx.Lat, nx.Lon, ny.Lat, ny.Lon); d < best {
				best = d
			}
		}
	}
	if math.IsInf(best, 1) {
		return 0, false
	}
	return best, true
}
