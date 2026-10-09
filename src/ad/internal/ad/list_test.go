package ad

import (
	"context"
	"encoding/base64"
	"testing"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"

	"google.golang.org/grpc/codes"
)

// seedAds creates n ads for owner and gives them only three distinct created_at values, so the id tie-breaker matters.
func seedAds(t *testing.T, s *Service, owner string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mustCreate(t, s, owner, "ad")
	}
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE ad SET created_at = timestamptz '2026-01-01 00:00:00+00' + (abs(hashtext(id::text)) % 3) * interval '1 hour' WHERE owner_id = $1`, owner); err != nil {
		t.Fatal(err)
	}
}

func walk(t *testing.T, s *Service, owner string, size int32) (ids []string, pages int) {
	t.Helper()
	token := ""
	for {
		resp, err := s.ListMyAds(as(owner), &adv1.ListMyAdsRequest{PageSize: size, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, a := range resp.Ads {
			ids = append(ids, a.Id)
		}
		if resp.NextPageToken == "" {
			return ids, pages
		}
		token = resp.NextPageToken
		if pages > 100 {
			t.Fatal("pagination does not terminate")
		}
	}
}

func TestListMyAdsPagination(t *testing.T) {
	s, _ := newTestService(t)
	seedAds(t, s, "user-1", 7)
	mustCreate(t, s, "user-2", "other")

	first, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageSize: 3})
	if err != nil || len(first.Ads) != 3 || first.NextPageToken == "" {
		t.Fatalf("first page: %v %v", first, err)
	}

	ids, pages := walk(t, s, "user-1", 3)
	if len(ids) != 7 || pages != 3 {
		t.Fatalf("walk: %d ids over %d pages", len(ids), pages)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate %s", id)
		}
		seen[id] = true
	}
	all, _ := walk(t, s, "user-1", 200)
	if len(all) != 7 || !equalStrings(all, ids) {
		t.Fatalf("order differs between page sizes:\n%v\n%v", all, ids)
	}

	// Exactly one full last page has no token.
	exact, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageSize: 7})
	if err != nil || len(exact.Ads) != 7 || exact.NextPageToken != "" {
		t.Fatalf("exact page: %d token=%q err=%v", len(exact.Ads), exact.NextPageToken, err)
	}

	// Newest first: created_at never increases along the walk.
	resp, _ := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{})
	for i := 1; i < len(resp.Ads); i++ {
		if resp.Ads[i].CreatedAt.AsTime().After(resp.Ads[i-1].CreatedAt.AsTime()) {
			t.Fatalf("not newest first at %d", i)
		}
	}
}

func TestListMyAdsEmptyAndClamp(t *testing.T) {
	s, _ := newTestService(t)
	empty, err := s.ListMyAds(as("user-9"), &adv1.ListMyAdsRequest{})
	if err != nil || len(empty.Ads) != 0 || empty.NextPageToken != "" {
		t.Fatalf("empty: %v %v", empty, err)
	}
	seedAds(t, s, "user-1", 205)
	for size, want := range map[int32]int{0: 50, -1: 50, 1000: 200} {
		resp, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageSize: size})
		if err != nil || len(resp.Ads) != want || resp.NextPageToken == "" {
			t.Fatalf("size %d: got %d token=%q err=%v, want %d", size, len(resp.Ads), resp.NextPageToken, err, want)
		}
	}
}

func TestListMyAdsInvalidToken(t *testing.T) {
	s, _ := newTestService(t)
	for _, tok := range []string{"!!!", "abc", base64.RawURLEncoding.EncodeToString([]byte("x|y"))} {
		_, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageToken: tok})
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestListMyAdsConcurrentInsertDoesNotDisturbCursor(t *testing.T) {
	s, _ := newTestService(t)
	seedAds(t, s, "user-1", 6)
	p1, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	// A new (newest) ad appears between the pages: the rest of the walk must be exactly the older items.
	mustCreate(t, s, "user-1", "late")
	p2, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{PageSize: 10, PageToken: p1.NextPageToken})
	if err != nil || len(p2.Ads) != 3 || p2.NextPageToken != "" {
		t.Fatalf("p2: %d %q %v", len(p2.Ads), p2.NextPageToken, err)
	}
	seen := map[string]bool{}
	for _, a := range append(p1.Ads, p2.Ads...) {
		if seen[a.Id] {
			t.Fatalf("duplicate %s", a.Id)
		}
		seen[a.Id] = true
	}
	if len(seen) != 6 {
		t.Fatalf("saw %d of the 6 original ads", len(seen))
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
