package geo_test

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/taakht/taakht/src/matching/internal/geo"
)

func TestReloadableKeepsOldConfigOnInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "e.json")
	t0 := time.Now().Add(-time.Hour)
	write := func(body string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"neighborhoods":[{"id":"n1","lat":35.7,"lon":51.4}]}`, t0)
	r, err := geo.LoadReloadable(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := (*r.Get())["n1"]; !ok {
		t.Fatal("n1 missing")
	}

	write(`{"neighborhoods":[{"id":"n1","lat":35.7,"lon":51.4},{"id":"n2","lat":35.8,"lon":51.5}]}`, t0.Add(time.Minute))
	if changed, err := r.Check(); !changed || err != nil {
		t.Fatalf("reload: %v %v", changed, err)
	}
	if _, ok := (*r.Get())["n2"]; !ok {
		t.Fatal("n2 not picked up")
	}

	for i, bad := range []string{`{`, `{"neighborhoods":[{"id":"","lat":1,"lon":1}]}`, `{"neighborhoods":[{"id":"x","lat":91,"lon":1}]}`} {
		write(bad, t0.Add(time.Duration(2+i)*time.Minute))
		if _, err := r.Check(); err == nil {
			t.Fatalf("%s accepted", bad)
		}
		if _, ok := (*r.Get())["n2"]; !ok {
			t.Fatalf("old config lost after %s", bad)
		}
	}
}
