package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	"github.com/taakht/taakht/libs/goplatform/identity"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/service"
	"github.com/taakht/taakht/src/matching/internal/testdb"
)

func put(t *testing.T, svc *service.Service, owner, have string, nbh []string, want ...string) string {
	t.Helper()
	id := uuid.NewString()
	_, err := index.Upsert(context.Background(), svc.DB, &adv1.Ad{
		Id: id, OwnerId: owner, Version: 1, Status: adv1.AdStatus_AD_STATUS_PUBLISHED,
		Spec: &adv1.AdSpec{Title: have, HaveCategory: have, WantCategories: want, NeighborhoodIds: nbh},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSearchAndFindMatches(t *testing.T) {
	pool := testdb.New(t)
	svc := &service.Service{DB: pool, Geo: geo.Map{
		"n1": {ID: "n1", Lat: 35.72, Lon: 51.41}, "n2": {ID: "n2", Lat: 35.80, Lon: 51.43},
	}}
	mine := put(t, svc, "user-1", "books", []string{"n1"}, "tools")
	near := put(t, svc, "user-2", "tools", []string{"n1"}, "books")
	far := put(t, svc, "user-3", "tools", []string{"n2"})
	put(t, svc, "user-4", "sports", []string{"n1"}, "books")
	put(t, svc, "user-1", "tools", []string{"n1"}) // own ad: never returned

	ctx := identity.WithUserID(context.Background(), "user-1")

	got, err := svc.FindMatches(ctx, &matchingv1.FindMatchesRequest{AdId: mine})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Candidates) != 2 || got.Candidates[0].Ad.Id != near || got.Candidates[1].Ad.Id != far ||
		got.Candidates[0].Score <= got.Candidates[1].Score {
		t.Fatalf("unexpected FindMatches result: %v", got)
	}

	if _, err := svc.FindMatches(identity.WithUserID(ctx, "user-2"), &matchingv1.FindMatchesRequest{AdId: mine}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
	if _, err := svc.FindMatches(ctx, &matchingv1.FindMatchesRequest{AdId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed ad_id should be INVALID_ARGUMENT, got %v", err)
	}
	if _, err := svc.FindMatches(ctx, &matchingv1.FindMatchesRequest{AdId: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}

	res, err := svc.Search(ctx, &matchingv1.SearchRequest{
		Criteria: &matchingv1.Criteria{WantCategories: []string{"tools"}, NeighborhoodIds: []string{"n1"}},
		Limit:    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Candidates) != 1 || res.Candidates[0].Ad.Id != near {
		t.Fatalf("unexpected Search result: %v", res)
	}

	all, err := svc.Search(ctx, &matchingv1.SearchRequest{Limit: 2})
	if err != nil || len(all.Candidates) != 2 {
		t.Fatalf("limit not applied: %v %v", all, err)
	}
}

func TestCandidatesBoundedAndRecentFirst(t *testing.T) {
	pool := testdb.New(t)
	svc := &service.Service{DB: pool, Geo: geo.Map{"n1": {ID: "n1", Lat: 35.72, Lon: 51.41}}}
	total := index.MaxCandidates + 20
	var newest string
	for i := 0; i < total; i++ {
		newest = put(t, svc, "user-"+uuid.NewString(), "tools", []string{"n1"})
	}
	got, err := index.Candidates(context.Background(), svc.DB, index.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != index.MaxCandidates {
		t.Fatalf("expected %d candidates, got %d", index.MaxCandidates, len(got))
	}
	if got[0].GetId() != newest {
		t.Fatalf("most recently updated ad should come first: got %s want %s", got[0].GetId(), newest)
	}

	// Equal scores: the freshest ad wins the single slot, every time.
	ctx := identity.WithUserID(context.Background(), "user-me")
	for i := 0; i < 3; i++ {
		res, err := svc.Search(ctx, &matchingv1.SearchRequest{Limit: 1})
		if err != nil || len(res.Candidates) != 1 || res.Candidates[0].Ad.Id != newest {
			t.Fatalf("tie-break not deterministic: %v %v", res, err)
		}
	}
}
