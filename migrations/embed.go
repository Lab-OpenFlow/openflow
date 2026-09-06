package migrations

import "embed"

// FS embeds all migration SQL files into the compiled binary.
// This ensures the binary is self-contained and migrations run automatically
// on startup without needing separate SQL files on the filesystem.
//
//go:embed *.sql
var FS embed.FS
