// Package pagination holds the keyset-pagination helpers shared by the list endpoints: page-size clamping and the
// opaque page token (base64url of "created_at|id" of the last item of a page).
package pagination

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultPageSize applies when the request asks for <= 0.
	DefaultPageSize = 50
	// MaxPageSize is the largest page; bigger requests are clamped.
	MaxPageSize = 200

	// timeLayout is UTC with microseconds, the precision of a Postgres timestamptz, so a cursor compares exactly.
	timeLayout = "2006-01-02T15:04:05.000000Z"
)

// ErrInvalidToken is returned by Decode for any malformed page token.
var ErrInvalidToken = errors.New("invalid page_token")

// Cursor is the position of the last item of a page in the (created_at DESC, id DESC) order.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// PageSize applies the default and the cap to the requested page size.
func PageSize(requested int32) int {
	switch {
	case requested <= 0:
		return DefaultPageSize
	case requested > MaxPageSize:
		return MaxPageSize
	default:
		return int(requested)
	}
}

// Encode builds the opaque token for the last item of a page.
func Encode(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(timeLayout) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// Decode parses a token. An empty token is not an error: ok is false and the caller serves the first page.
func Decode(token string) (c Cursor, ok bool, err error) {
	if token == "" {
		return Cursor{}, false, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(token, "="))
	if err != nil {
		return Cursor{}, false, ErrInvalidToken
	}
	ts, id, found := strings.Cut(string(raw), "|")
	if !found {
		return Cursor{}, false, ErrInvalidToken
	}
	t, err := time.Parse(timeLayout, ts)
	if err != nil {
		return Cursor{}, false, ErrInvalidToken
	}
	u, err := uuid.Parse(id)
	if err != nil {
		return Cursor{}, false, ErrInvalidToken
	}
	return Cursor{CreatedAt: t, ID: u.String()}, true, nil
}
