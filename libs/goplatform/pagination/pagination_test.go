package pagination

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPageSize(t *testing.T) {
	for in, want := range map[int32]int{-5: 50, 0: 50, 1: 1, 50: 50, 200: 200, 201: 200, 1 << 30: 200} {
		if got := PageSize(in); got != want {
			t.Errorf("PageSize(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 9, 12, 30, 45, 123456000, time.UTC)
	id := uuid.NewString()
	c, ok, err := Decode(Encode(ts, id))
	if err != nil || !ok || !c.CreatedAt.Equal(ts) || c.ID != id {
		t.Fatalf("round trip: %v %v %+v", ok, err, c)
	}
}

func TestEmptyTokenIsFirstPage(t *testing.T) {
	if _, ok, err := Decode(""); ok || err != nil {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
}

func TestInvalidTokens(t *testing.T) {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for _, tok := range []string{"!!!", "abc", enc("nopipe"), enc("2026-10-09|" + uuid.NewString()), enc("2026-10-09T12:30:45.123456Z|not-a-uuid"), enc("|")} {
		if _, _, err := Decode(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("Decode(%q) err = %v", tok, err)
		}
	}
}
