// Package skills carries the public wefty skill inside the wefty binary, so
// `wefty setup` installs the copy that matches the build it runs from and
// needs no checkout at install time.
package skills

import "embed"

// Wefty holds skills/wefty: the skill an AI agent loads to drive a cluster.
//
//go:embed wefty
var Wefty embed.FS
