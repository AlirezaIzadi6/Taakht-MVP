package ad

import (
	"strings"
	"testing"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/src/ad/internal/eligibility"
)

var testElig = eligibility.New([]string{"books", "tools"}, []string{"n-valiasr"})

func TestValidateSpec(t *testing.T) {
	cases := []struct {
		name    string
		spec    *adv1.AdSpec
		wantErr bool
	}{
		{"ok", &adv1.AdSpec{Title: "x", HaveCategory: "books", WantCategories: []string{"tools"}, NeighborhoodIds: []string{"n-valiasr"}}, false},
		{"partial have side", &adv1.AdSpec{Title: "x"}, false},
		{"nil", nil, true},
		{"no title", &adv1.AdSpec{HaveCategory: "books"}, true},
		{"bad have", &adv1.AdSpec{Title: "x", HaveCategory: "cars"}, true},
		{"bad want", &adv1.AdSpec{Title: "x", WantCategories: []string{"cars"}}, true},
		{"bad hood", &adv1.AdSpec{Title: "x", NeighborhoodIds: []string{"n-x"}}, true},
		{"blank title", &adv1.AdSpec{Title: "   "}, true},
		{"long title", &adv1.AdSpec{Title: strings.Repeat("x", 121)}, true},
		{"long description", &adv1.AdSpec{Title: "x", Description: strings.Repeat("d", 4001)}, true},
		{"too many wants", &adv1.AdSpec{Title: "x", WantCategories: make([]string, 21)}, true},
		{"long entry", &adv1.AdSpec{Title: "x", NeighborhoodIds: []string{strings.Repeat("n", 65)}}, true},
		{"negative value", &adv1.AdSpec{Title: "x", ValueEstimate: -1}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := ValidateSpec(c.spec, testElig); (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}

func TestHasFilter(t *testing.T) {
	if HasFilter(&adv1.AdSpec{Title: "x"}) {
		t.Fatal("no filter expected")
	}
	if !HasFilter(&adv1.AdSpec{WantCategories: []string{"books"}}) || !HasFilter(&adv1.AdSpec{NeighborhoodIds: []string{"n"}}) {
		t.Fatal("filter expected")
	}
}

func TestStateRules(t *testing.T) {
	for _, s := range []string{StatusPublished, StatusHidden, StatusLocked, StatusClosed} {
		editable := s == StatusPublished || s == StatusHidden
		if Editable(s) != editable || Lockable(s) != editable {
			t.Errorf("%s: editable/lockable mismatch", s)
		}
		if CanPublish(s) != (s == StatusHidden) {
			t.Errorf("%s: CanPublish", s)
		}
		if CanHide(s) != (s == StatusPublished) {
			t.Errorf("%s: CanHide", s)
		}
	}
}

func TestValidateSpecNormalizes(t *testing.T) {
	spec := &adv1.AdSpec{Title: "  hello  ", WantCategories: []string{"books", "tools", "books"}, NeighborhoodIds: []string{"n-valiasr", "n-valiasr"}}
	if err := ValidateSpec(spec, testElig); err != nil {
		t.Fatal(err)
	}
	if spec.Title != "hello" || len(spec.WantCategories) != 2 || len(spec.NeighborhoodIds) != 1 {
		t.Fatalf("not normalized: %v", spec)
	}
	// 20 distinct-looking duplicates still count against the cap before de-duplication.
	if err := ValidateSpec(&adv1.AdSpec{Title: "x", WantCategories: make([]string, 20)}, testElig); err == nil {
		t.Fatal("empty category names are unknown and must fail")
	}
}
