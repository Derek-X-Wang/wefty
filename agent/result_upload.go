package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// A run's result document is pushed, not fetched.
//
// The node that ran the job is the only thing that can read its handoff
// directory, and nothing in this system lets the ledger ask a node for a file.
// So at completion the node reads result.json once and uploads it, and the
// ledger serves every later reader from its own copy. The file stays on the
// node for its retention window either way: the upload is a copy, not a move,
// and a result too large or too broken to upload is still on disk where the
// operator can go and look at it.
//
// Reading is deliberately unforgiving. result.json sits in a directory the
// workload owns, so it is proved to be a regular file, read under a byte bound,
// and parsed as JSON before a byte of it is offered to the ledger. Every way it
// can fail has a name, and that name is what a reader is told -- "this run's
// result is 40 MiB, it is on node X" is an answer; a bare absence is not.

// attemptResult is one attempt's result as the node found it: the document, or
// the reason there is none to upload.
type attemptResult struct {
	document []byte
	skip     contract.ResultUploadSkipReason
}

func (result attemptResult) empty() bool {
	return len(result.document) == 0 && result.skip == ""
}

// classifyResultDocument applies the one rule every read path shares: what the
// node uploads is a bounded JSON document, or nothing.
//
// It never answers "absent". Absence is a fact about the directory, not about
// bytes, and only the caller that looked for the file knows it. A file that
// exists and is empty is a file the run wrote and did not fill, which is a
// different thing an operator would want to hear about, so it reads as
// not_json like any other content that is not a JSON document.
func classifyResultDocument(document []byte, truncated bool) attemptResult {
	if truncated || int64(len(document)) > contract.MaxUploadedResultBytes {
		return attemptResult{skip: contract.ResultUploadSkipOversize}
	}
	if !json.Valid(document) {
		return attemptResult{skip: contract.ResultUploadSkipNotJSON}
	}
	return attemptResult{document: document}
}

// readHandoffResult reads result.json through an already-verified handle on the
// run's own directory. The handle is the receipt holder's; this function never
// resolves a path of its own, never follows a link, and opens non-blocking so a
// FIFO left in place of the document cannot hold the agent.
func readHandoffResult(run *os.Root) attemptResult {
	if run == nil {
		return attemptResult{skip: contract.ResultUploadSkipAbsent}
	}
	info, err := run.Lstat(handoffResultName)
	if errors.Is(err, fs.ErrNotExist) {
		return attemptResult{skip: contract.ResultUploadSkipAbsent}
	}
	if err != nil {
		return attemptResult{skip: contract.ResultUploadSkipUnreadable}
	}
	if !info.Mode().IsRegular() {
		return attemptResult{skip: contract.ResultUploadSkipNotFile}
	}
	if info.Size() > contract.MaxUploadedResultBytes {
		return attemptResult{skip: contract.ResultUploadSkipOversize}
	}
	file, err := run.OpenFile(handoffResultName, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return attemptResult{skip: contract.ResultUploadSkipAbsent}
		}
		return attemptResult{skip: contract.ResultUploadSkipUnreadable}
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return attemptResult{skip: contract.ResultUploadSkipUnreadable}
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return attemptResult{skip: contract.ResultUploadSkipNotFile}
	}
	document, err := io.ReadAll(io.LimitReader(file, contract.MaxUploadedResultBytes+1))
	if err != nil {
		return attemptResult{skip: contract.ResultUploadSkipUnreadable}
	}
	return classifyResultDocument(document, false)
}

// readRemoteHandoffResult reads the same document through the privileged
// helper, for an OCI attempt whose handoff volume this process cannot open. It
// must run while the attempt is still live: the helper authorizes every read
// against the live attempt, and a reaped attempt has no read path at all.
func readRemoteHandoffResult(ctx context.Context, reader handoffFileReader) attemptResult {
	if reader == nil {
		return attemptResult{skip: contract.ResultUploadSkipAbsent}
	}
	document, truncated, err := reader.readHandoffFile(ctx, handoffResultName, contract.MaxUploadedResultBytes)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return attemptResult{skip: contract.ResultUploadSkipAbsent}
		}
		// The helper's own positive classification of the object is the only
		// thing that becomes "this is not a result". Anything else is a fact
		// about the transport, and the honest answer is that the node could
		// not read it.
		if errors.Is(err, errRunMailboxEntryUnusable) || errors.Is(err, workloadrunner.ErrRunMailboxEntryUnusable) {
			return attemptResult{skip: contract.ResultUploadSkipNotFile}
		}
		return attemptResult{skip: contract.ResultUploadSkipUnreadable}
	}
	return classifyResultDocument(document, truncated)
}

// uploadAttemptResult pushes what the node found -- always, including the fact
// that it found nothing.
//
// Every completion that holds the receipt writes exactly one row, because the
// row belongs to the job's latest attempt and a retry that produced no result
// must still displace its predecessor's. Uploading nothing for an absent result
// would leave the earlier attempt's document standing as the run's answer,
// which is the one thing this row exists to prevent.
func uploadAttemptResult(ctx context.Context, client *Client, jobID, attemptID, fencingToken string,
	result attemptResult) (attemptResult, error) {
	if client == nil || result.empty() {
		return result, nil
	}
	request := l1.AttemptResultRequest{FencingToken: fencingToken, SkipReason: result.skip}
	if len(result.document) > 0 {
		digest := sha256.Sum256(result.document)
		request.Document = result.document
		request.SHA256 = hex.EncodeToString(digest[:])
	}
	if _, err := client.SetAttemptResult(ctx, jobID, attemptID, request); err != nil {
		// An upload that did not land -- a refused ledger, an unreachable one,
		// an attempt a successor has already superseded -- is recorded locally
		// as a transport skip and leaves no ledger row at all. It is not
		// retried here: the run is finishing, the file is retained on the
		// node, and a retry loop at this point would hold a completion open
		// behind a ledger that is already failing.
		return attemptResult{skip: contract.ResultUploadSkipTransport}, err
	}
	return result, nil
}
