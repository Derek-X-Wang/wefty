package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

const execSnapshotDeleted = "the exec snapshot was deleted when the run ended; run `wefty exec` again"

// Identify exec from its immutable typed params, never command/log text. New
// captures carry an explicit version. Older captures have the exact original
// exec schema: independent Git identities, an absolute allocated storage path,
// a relative start folder and argv, on an inline-script run.
func execRunStorage(record contract.RunRecord) (string, bool) {
	if record.Workflow.InlineScript == nil || record.Workflow.WorkflowRef != "" || record.Workflow.Image != nil {
		return "", false
	}
	var params execParams
	decoder := json.NewDecoder(bytes.NewReader(record.Params))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&params) != nil || (params.ExecVersion != 0 && params.ExecVersion != 1) {
		return "", false
	}
	if !filepath.IsAbs(params.Storage) || !strings.HasPrefix(filepath.Base(params.Storage), "wefty-exec-") ||
		params.Folder == "" || filepath.IsAbs(params.Folder) || len(params.Argv) == 0 {
		return "", false
	}
	for _, hash := range []string{params.Base, params.Tree} {
		if decoded, err := hex.DecodeString(hash); err != nil || (len(decoded) != 20 && len(decoded) != 32) {
			return "", false
		}
	}
	return params.Storage, true
}

func checkExecRerunCapture(record contract.RunRecord) error {
	storage, isExec := execRunStorage(record)
	if !isExec {
		return nil
	}
	_, err := os.Stat(filepath.Join(storage, "snapshot.git"))
	if os.IsNotExist(err) {
		return usageError(execSnapshotDeleted)
	}
	return err
}
