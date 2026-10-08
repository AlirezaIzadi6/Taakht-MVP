// Package migrations embeds the SQL migrations of the ad service.
package migrations

import "embed"

// FS holds the *.sql migration files.
//
//go:embed *.sql
var FS embed.FS
