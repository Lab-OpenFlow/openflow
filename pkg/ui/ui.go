package ui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// GetFS returns the sub-filesystem for the embedded web assets.
func GetFS() (fs.FS, error) {
	return fs.Sub(distFS, "dist")
}
