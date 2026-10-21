//go:build !unix

package main

import "io/fs"

// Plain conversion is macOS-only. Fail closed on unsupported filesystem APIs.
func conversionSingleLink(fs.FileInfo) bool { return false }
func conversionDoubleLink(fs.FileInfo) bool { return false }
