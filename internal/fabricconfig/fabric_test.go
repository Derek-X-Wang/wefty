package fabricconfig

import (
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestPlainFabricIDIsExplicitAndPrefixValidated(t *testing.T) {
	participant, closeFabric, err := Open(Config{
		Mode: "plain", PlainFabricID: "plain-configured-dev", Identity: fabric.Identity{NodeID: "client"},
	})
	if err != nil || participant == nil || closeFabric == nil {
		t.Fatalf("explicit plain Fabric config = participant=%v close=%v err=%v", participant, closeFabric, err)
	}
	if err := closeFabric(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"fabric-production-shaped", "plain-", " plain-dev"} {
		if _, _, err := Open(Config{Mode: "plain", PlainFabricID: invalid}); err == nil || !strings.Contains(err.Error(), "DEVELOPMENT ONLY") {
			t.Fatalf("invalid plain Fabric ID %q error = %v", invalid, err)
		}
	}
}
