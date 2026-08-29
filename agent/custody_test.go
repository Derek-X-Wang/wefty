package agent

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCustodyControllerRejectsForeignNodeAndManagedRootBeforeMutation(t *testing.T) {
	controller := &custodyController{nodeID: "node-1", bootSessionID: "boot-1", rootInstanceID: "root-1"}
	directive := l1.ComputerCustodyExportDirective{BoundNodeID: "node-1", RootInstanceID: "root-1"}
	for name, mutate := range map[string]func(*l1.ComputerCustodyExportDirective){
		"node": func(value *l1.ComputerCustodyExportDirective) { value.BoundNodeID = "node-2" },
		"root": func(value *l1.ComputerCustodyExportDirective) { value.RootInstanceID = "root-2" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := directive
			mutate(&copy)
			// The nil exporter would panic if authority validation did not
			// return before reaching mutation mechanics.
			if err := controller.process(t.Context(), copy); err == nil {
				t.Fatal("foreign Custody authority reached mutation mechanics")
			}
		})
	}
}
