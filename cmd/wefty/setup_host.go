package main

// setupCommandHost is the command's system boundary. Binary tests replace this
// file with a Go build overlay; the shipped binary has no fixture flags or
// environment switches and always uses the real host.
func setupCommandHost() (setupHost, error) { return realSetupHost() }
