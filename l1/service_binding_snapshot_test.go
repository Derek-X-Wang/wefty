package l1

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceBindingProofReadsSnapshotWhileWriterHoldsLock(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "binding.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}, NodePolicy{MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), operatorServiceSpec("binding-snapshot", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim service: claim=%+v err=%v", claim, err)
	}

	// Like the listing regressions (#695/#687), hold a real IMMEDIATE writer
	// throughout the proof. Both authority and binding reads must see committed
	// state without waiting for this writer to finish.
	writer, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(t.Context(), `UPDATE nodes SET boot_session_id='replacement' WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(t.Context(), `UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?`, job.JobID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	bound, err := store.ProveServiceBinding(ctx, "agent", job.JobID, ServiceBindingProofRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
	})
	if err != nil || ctx.Err() != nil || !bound {
		t.Fatalf("proof blocked by writer or lost committed snapshot: bound=%t err=%v context=%v", bound, err, ctx.Err())
	}
}
