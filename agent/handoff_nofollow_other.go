//go:build !unix

package agent

// noFollowOpenFlag has no portable equivalent outside unix; the regular-file
// proof around every open carries the guarantee there.
const noFollowOpenFlag = 0
