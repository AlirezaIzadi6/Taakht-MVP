// Package migrations embeds the SQL migrations of the matching database.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
