package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNativeLinuxOCIReceiptDistinguishesPRDeviationFromPublishedProof(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		receipt string
		wantOK  bool
	}{
		{
			name:    "published proof",
			source:  "published-artifact",
			receipt: "pull_from_empty=true\npull_import_digest_equal=true\n",
			wantOK:  true,
		},
		{
			name:   "PR typed deviation",
			source: "pr-build",
			receipt: "pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published\n" +
				"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=pr-build: image not published\n",
			wantOK: true,
		},
		{
			name:    "PR cannot claim published proof",
			source:  "pr-build",
			receipt: "pull_from_empty=true\npull_import_digest_equal=true\n",
		},
		{
			name:   "published lane cannot inherit PR deviation",
			source: "published-artifact",
			receipt: "pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published\n" +
				"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=pr-build: image not published\n",
		},
		{
			name:   "PR reason is exact",
			source: "pr-build",
			receipt: "pull_from_empty=NOT-RUN\npull_from_empty_reason=image unavailable\n" +
				"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=image unavailable\n",
		},
		{
			name:    "unknown source fails closed",
			source:  "other",
			receipt: "pull_from_empty=true\npull_import_digest_equal=true\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt := filepath.Join(t.TempDir(), "native-linux-oci.txt")
			if err := os.WriteFile(receipt, []byte(test.receipt), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("../scripts/check-native-linux-oci-receipt.sh", receipt, test.source)
			err := command.Run()
			if (err == nil) != test.wantOK {
				t.Fatalf("receipt gate error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}
