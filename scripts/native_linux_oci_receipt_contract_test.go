package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestNativeLinuxOCIReceiptDistinguishesPRDeviationFromPublishedProof(t *testing.T) {
	tests := []struct {
		name           string
		source         string
		receipt        string
		serviceReceipt string
		l1Receipt      string
		wantOK         bool
	}{
		{
			name:           "published proof with complete seals",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
			wantOK:         true,
		},
		{
			name:           "PR typed deviation with incomplete seals",
			source:         "pr-build",
			receipt:        prNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(true),
			l1Receipt:      serviceReadmissionReceipt(),
			wantOK:         true,
		},
		{
			name:           "PR cannot claim published proof",
			source:         "pr-build",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "published lane cannot inherit PR deviation",
			source:         "published-artifact",
			receipt:        prNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:   "PR reason is exact",
			source: "pr-build",
			receipt: "pull_from_empty=NOT-RUN\npull_from_empty_reason=image unavailable\n" +
				"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=image unavailable\n" +
				"binding_repull_reconciliation=NOT-RUN\nbinding_repull_reconciliation_reason=image unavailable\n",
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "mixed receipt cannot satisfy either lane",
			source:         "pr-build",
			receipt:        prNativeOCIReceipt() + "pull_from_empty=true\npull_import_digest_equal=true\nbinding_repull_reconciliation=true\n",
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "service escalation is gated",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: "term_kill_escalation=false\nterm_kill_log_evidence_incomplete=false\nterm_kill_log_seal_pairing=true\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n",
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "service seal pairing is gated",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: "term_kill_escalation=true\nterm_kill_log_evidence_incomplete=false\nterm_kill_log_seal_pairing=false\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n",
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "unknown source fails closed",
			source:         "other",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "malformed recovery duration fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      "service_fresh_attempt_readmission=true\nservice_recovery_elapsed=soon\n",
		},
		{
			name:           "zero recovery duration fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      "service_fresh_attempt_readmission=true\nservice_recovery_elapsed=0s\n",
		},
		{
			name:           "recovery beyond production bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      "service_fresh_attempt_readmission=true\nservice_recovery_elapsed=15.01s\n",
		},
		{
			name:           "republication beyond production bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: strings.Replace(servicePublicationReceipt(false), "republication_elapsed=1.13s", "republication_elapsed=5.01s", 1),
			l1Receipt:      serviceReadmissionReceipt(),
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
			l1Receipt := filepath.Join(t.TempDir(), "oci-service-l1-agent-linux.txt")
			if err := os.WriteFile(l1Receipt, []byte(test.l1Receipt), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("../scripts/check-native-linux-oci-receipt.sh", receipt, test.source, serviceReceipt, l1Receipt)
			err := command.Run()
			if (err == nil) != test.wantOK {
				t.Fatalf("receipt gate error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}

func publishedNativeOCIReceipt() string {
	return "pull_from_empty=true\npull_import_digest_equal=true\nbinding_repull_reconciliation=true\n"
}

func prNativeOCIReceipt() string {
	return "pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published\n" +
		"pull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=pr-build: image not published\n" +
		"binding_repull_reconciliation=NOT-RUN\nbinding_repull_reconciliation_reason=pr-build: image not published\n"
}

func serviceReadmissionReceipt() string {
	return "service_fresh_attempt_readmission=true\nservice_recovery_elapsed=127ms\n"
}

func servicePublicationReceipt(logEvidenceIncomplete bool) string {
	return "term_kill_escalation=true\nterm_kill_log_evidence_incomplete=" + strconv.FormatBool(logEvidenceIncomplete) +
		"\nterm_kill_log_seal_pairing=true\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n" +
		"withdrawal=true\nwithdrawal_elapsed=83ms\nrepublication=true\nrepublication_elapsed=1.13s\n"
}
