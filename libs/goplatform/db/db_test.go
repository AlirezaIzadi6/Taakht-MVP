package db_test

import (
	"testing"

	"github.com/taakht/taakht/libs/goplatform/db"
)

func TestPoolSizes(t *testing.T) {
	tests := []struct {
		name, maxEnv, minEnv string
		wantMax, wantMin     int32
		wantErr              bool
	}{
		{"defaults", "", "", 20, 2, false},
		{"explicit", "5", "1", 5, 1, false},
		{"tiny max lowers the default min", "1", "", 1, 1, false},
		{"min zero allowed", "3", "0", 3, 0, false},
		{"max zero rejected", "0", "", 0, 0, true},
		{"max not a number", "many", "", 0, 0, true},
		{"negative min rejected", "", "-1", 0, 0, true},
		{"explicit min above max rejected", "2", "5", 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DB_MAX_CONNS", tc.maxEnv)
			t.Setenv("DB_MIN_CONNS", tc.minEnv)
			maxC, minC, err := db.PoolSizes()
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && (maxC != tc.wantMax || minC != tc.wantMin) {
				t.Fatalf("got %d/%d, want %d/%d", maxC, minC, tc.wantMax, tc.wantMin)
			}
		})
	}
}
