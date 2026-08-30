package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestNativeLinuxOCIReceiptDistinguishesPRDeviationFromPublishedProof(t *testing.T) {
	tests := []struct {
		name           string
		source         string
		receipt        string
		serviceReceipt string
		wantOK         bool
	}{
		{
			name:           "published proof with complete seals",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			wantOK:         true,
		},
		{
			name:           "PR typed deviation with incomplete seals",
			source:         "pr-build",
			receipt:        prNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(true),
			wantOK:         true,
		},
		{
			name:           "PR cannot claim published proof",
			source:         "pr-build",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
		},
		{
			name:           "published lane cannot inherit PR deviation",
			source:         "published-artifact",
			receipt:        prNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
		},
		{
			name:   "PR reason is exact",
			source: "pr-build",
			receipt: "pull_from_empty=NOT-RUN\npull_from_empty_reason=image unavailable\n" +
				"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=image unavailable\n" +
				"binding_repull_reconciliation=NOT-RUN\nbinding_repull_reconciliation_reason=image unavailable\n",
			serviceReceipt: servicePublicationReceipt(false),
		},
		{
			name:           "mixed receipt cannot satisfy either lane",
			source:         "pr-build",
			receipt:        prNativeOCIReceipt() + "pull_from_empty=true\npull_import_digest_equal=true\nbinding_repull_reconciliation=true\n",
			serviceReceipt: servicePublicationReceipt(false),
		},
		{
			name:           "service escalation is gated",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: "term_kill_escalation=false\nterm_kill_log_evidence_incomplete=false\nterm_kill_log_seal_pairing=true\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n",
		},
		{
			name:           "service seal pairing is gated",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: "term_kill_escalation=true\nterm_kill_log_evidence_incomplete=false\nterm_kill_log_seal_pairing=false\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n",
		},
		{
			name:           "unknown source fails closed",
			source:         "other",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt := filepath.Join(t.TempDir(), "native-linux-oci.txt")
			if err := os.WriteFile(receipt, []byte(test.receipt), 0o600); err != nil {
				t.Fatal(err)
			}
			serviceReceipt := filepath.Join(t.TempDir(), "oci-service-publication-linux.txt")
			if err := os.WriteFile(serviceReceipt, []byte(test.serviceReceipt), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("../scripts/check-native-linux-oci-receipt.sh", receipt, test.source, serviceReceipt)
			err := command.Run()
			if (err == nil) != test.wantOK {
				t.Fatalf("receipt gate error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}

func publishedNativeOCIReceipt() string {
	return "pull_from_empty=true\npull_import_digest_equal=true\nbinding_repull_reconciliation=true\n" + serviceReadmissionReceipt()
}

func prNativeOCIReceipt() string {
	return "pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published\n" +
		"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=pr-build: image not published\n" +
		"binding_repull_reconciliation=NOT-RUN\nbinding_repull_reconciliation_reason=pr-build: image not published\n" + serviceReadmissionReceipt()
}

func serviceReadmissionReceipt() string {
	return "service_fresh_attempt_readmission=true\nservice_recovery_elapsed=127ms\n"
}

func servicePublicationReceipt(logEvidenceIncomplete bool) string {
	return "term_kill_escalation=true\nterm_kill_log_evidence_incomplete=" + strconv.FormatBool(logEvidenceIncomplete) +
		"\nterm_kill_log_seal_pairing=true\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n" +
		"withdrawal=true\nwithdrawal_elapsed=83ms\nrepublication=true\nrepublication_elapsed=1.13s\n"
}
