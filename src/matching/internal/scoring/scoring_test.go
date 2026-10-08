package scoring

import (
	"math"
	"testing"

	"github.com/taakht/taakht/src/matching/internal/geo"
)

var g = geo.Map{
	"a": {ID: "a", Lat: 35.7219, Lon: 51.4093},
	"b": {ID: "b", Lat: 35.8044, Lon: 51.4339},
	"c": {ID: "c", Lat: 35.8204, Lon: 50.9393},
}

func TestHaversine(t *testing.T) {
	if d := geo.Haversine(0, 0, 0, 0); d != 0 {
		t.Fatalf("zero distance, got %v", d)
	}
	d := geo.Haversine(35.7219, 51.4093, 35.8044, 51.4339)
	if d < 8 || d > 10 {
		t.Fatalf("expected about 9 km, got %v", d)
	}
}

func TestProximity(t *testing.T) {
	if p := Proximity(g, []string{"a"}, []string{"a"}); p != 1 {
		t.Fatalf("same neighborhood must be 1, got %v", p)
	}
	near := Proximity(g, []string{"a"}, []string{"b"})
	far := Proximity(g, []string{"a"}, []string{"c"})
	if !(near < 1 && near > far && far > 0) {
		t.Fatalf("expected 1 > near > far > 0, got near=%v far=%v", near, far)
	}
	if p := Proximity(g, nil, []string{"a"}); p != unknownProximity {
		t.Fatalf("unknown side must be neutral, got %v", p)
	}
	if p := Proximity(g, []string{"zzz"}, []string{"a"}); p != 0 {
		t.Fatalf("unknown ids must be 0, got %v", p)
	}
	if p := Proximity(g, []string{"a", "c"}, []string{"b", "c"}); p != 1 {
		t.Fatalf("closest pair (shared c) must win, got %v", p)
	}
}

func TestTwoSided(t *testing.T) {
	me := Spec{HaveCategory: "books", WantCategories: []string{"tools"}, NeighborhoodIDs: []string{"a"}}
	cand := Spec{HaveCategory: "tools", WantCategories: []string{"books"}, NeighborhoodIDs: []string{"a"}}
	s, ok := TwoSided(g, me, cand)
	if !ok || math.Abs(s-3.0) > 1e-9 {
		t.Fatalf("exact both sides + same place = 3.0, got %v %v", s, ok)
	}

	open := Spec{HaveCategory: "tools", NeighborhoodIDs: []string{"a"}}
	s2, ok := TwoSided(g, me, open)
	if !ok || math.Abs(s2-2.5) > 1e-9 {
		t.Fatalf("one side open = 2.5, got %v %v", s2, ok)
	}
	if s2 >= s {
		t.Fatal("exact match must outscore open match")
	}

	if _, ok := TwoSided(g, me, Spec{HaveCategory: "sports"}); ok {
		t.Fatal("candidate item not wanted by me must not match")
	}
	if _, ok := TwoSided(g, me, Spec{HaveCategory: "tools", WantCategories: []string{"kitchen"}}); ok {
		t.Fatal("my item not wanted by candidate must not match")
	}
	if _, ok := TwoSided(g, Spec{HaveCategory: "books"}, Spec{HaveCategory: "x"}); !ok {
		t.Fatal("both open must match")
	}
}

func TestBrowse(t *testing.T) {
	cand := Spec{HaveCategory: "tools", NeighborhoodIDs: []string{"a", "b"}}
	if _, ok := Browse(g, Criteria{}, cand); !ok {
		t.Fatal("empty criteria matches everything")
	}
	if _, ok := Browse(g, Criteria{WantCategories: []string{"books"}}, cand); ok {
		t.Fatal("category filter must exclude")
	}
	if _, ok := Browse(g, Criteria{NeighborhoodIDs: []string{"c"}}, cand); ok {
		t.Fatal("neighborhood filter must exclude")
	}
	s1, ok := Browse(g, Criteria{WantCategories: []string{"tools"}, NeighborhoodIDs: []string{"b"}}, cand)
	s2, _ := Browse(g, Criteria{}, cand)
	if !ok || s1 <= s2 {
		t.Fatalf("filtered match should outscore unfiltered: %v vs %v", s1, s2)
	}
}
