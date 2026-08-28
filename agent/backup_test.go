package agent

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestBackupControllerRejectsForeignNodeAndManagedRootBeforeMutation(t *testing.T) {
	controller := &backupController{nodeID: "node-1", bootSessionID: "boot-1", rootInstanceID: "root-1"}
	create := l1.ComputerBackupDirective{BoundNodeID: "node-1", RootInstanceID: "root-1"}
	prune := l1.ComputerBackupPruneDirective{BoundNodeID: "node-1", RootInstanceID: "root-1"}
	for name, mutate := range map[string]func() error{
		"create node": func() error {
			copy := create
			copy.BoundNodeID = "node-2"
			return controller.processCreate(t.Context(), copy)
		},
		"create root": func() error {
			copy := create
			copy.RootInstanceID = "root-2"
			return controller.processCreate(t.Context(), copy)
		},
		"prune node": func() error {
			copy := prune
			copy.BoundNodeID = "node-2"
			return controller.processPrune(t.Context(), copy)
		},
		"prune root": func() error {
			copy := prune
			copy.RootInstanceID = "root-2"
			return controller.processPrune(t.Context(), copy)
		},
	} {
		t.Run(name, func(t *testing.T) {
			// The nil runtime would panic if authority validation did not return
			// before reaching mutation mechanics.
			if err := mutate(); err == nil {
				t.Fatal("foreign Backup authority reached mutation mechanics")
			}
		})
	}
}
