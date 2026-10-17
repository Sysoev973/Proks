// Package migrations exposes the idempotent schema changes that services apply
// at startup.
package migrations

import _ "embed"

//go:embed 002_book_versions_outbox.sql
var BookVersionsOutbox string
