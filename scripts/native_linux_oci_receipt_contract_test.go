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
		traceContains  string
		wantOK         bool
	}{
		{
			name:           "published proof with complete seals",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt(),
			traceContains:  "key=service_fresh_attempt_admission_elapsed value=127ms parsed_ns=127000000 bound_ns=28000000000 result=accepted",
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
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=soon", 1),
		},
		{
			name:           "zero recovery duration fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=0s", 1),
		},
		{
			name:           "nanosecond literal is not measured evidence",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=1ns", 1),
		},
		{
			name:           "recovery beyond production bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=30.01s", 1),
		},
		{
			name:           "end-to-end recovery beyond receipt bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_recovery_elapsed=183ms", "service_recovery_elapsed=28.01s", 1),
		},
		{
			name:           "startup preface fact is gated",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_barrier_prefaced_during_startup=true", "service_barrier_prefaced_during_startup=false", 1),
		},
		{
			name:           "lost log disposition must be unique",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      serviceReadmissionReceipt() + "service_lost_log_disposition=retained:log_spool_sealing\n",
		},
		{
			name:           "composite Go duration beyond bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=1m24.78s", 1),
		},
		{
			name:           "exact lane recovery duration beyond bound fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: servicePublicationReceipt(false),
			l1Receipt:      strings.Replace(serviceReadmissionReceipt(), "service_fresh_attempt_admission_elapsed=127ms", "service_fresh_attempt_admission_elapsed=1m24.782166924s", 1),
			traceContains:  "key=service_fresh_attempt_admission_elapsed value=1m24.782166924s parsed_ns=84782166924 bound_ns=28000000000 result=rejected",
		},
		{
			name:           "republication beyond receipt observation deadline fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: strings.Replace(servicePublicationReceipt(false), "republication_elapsed=11.03s", "republication_elapsed=11.41s", 1),
			l1Receipt:      serviceReadmissionReceipt(),
		},
		{
			name:           "missing republication observation deadline fails closed",
			source:         "published-artifact",
			receipt:        publishedNativeOCIReceipt(),
			serviceReceipt: strings.Replace(servicePublicationReceipt(false), "republication_observation_deadline=11.4s\n", "", 1),
			l1Receipt:      serviceReadmissionReceipt(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, shell := range []string{"/bin/bash", "/bin/sh"} {
				t.Run(filepath.Base(shell), func(t *testing.T) {
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
					command := exec.Command(shell, "../scripts/check-native-linux-oci-receipt.sh", receipt, test.source, serviceReceipt, l1Receipt)
					command.Env = append(os.Environ(), "WEFTY_RECEIPT_GATE_TRACE=1")
					output, err := command.CombinedOutput()
					t.Logf("%s receipt trace:\n%s", shell, output)
					if (err == nil) != test.wantOK {
						t.Fatalf("%s receipt gate error = %v, want success %t\n%s", shell, err, test.wantOK, output)
					}
					if test.traceContains != "" && !strings.Contains(string(output), test.traceContains) {
						t.Fatalf("%s receipt trace omitted %q:\n%s", shell, test.traceContains, output)
					}
				})
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
	return "service_fresh_attempt_readmission=true\nservice_helper_loss_injected=true\nservice_helper_loss_observed=true\n" +
		"service_helper_loss_observed_at=2026-09-04T12:00:00Z\nservice_recovery_elapsed=183ms\nservice_recovery_bound=28s\n" +
		"service_fresh_attempt_admission_elapsed=127ms\nservice_kill_to_fresh_attempt_admission_elapsed=151ms\nservice_fresh_attempt_admission_bound=28s\n" +
		"service_fresh_attempt_admission_margin=2s\nservice_fresh_attempt_admission_margin_basis=round4_cleanup_lt_600ms_ceil_1s_plus_preface_admission_ceil_1s\n" +
		"service_fresh_attempt_admitted_at=2026-09-04T12:00:00.151Z\n" +
		"service_barrier_advertised_reap_timeout=10s\nservice_barrier_takeover_bound=20s\nservice_barrier_verified_ready_bound=30s\n" +
		"service_barrier_started_at=2026-09-04T12:00:00.024Z\nservice_barrier_preface_completed_at=2026-09-04T12:00:00.035Z\n" +
		"service_barrier_session_admitted_at=2026-09-04T12:00:00.103Z\nservice_barrier_verified_ready_at=2026-09-04T12:00:00.136Z\n" +
		"service_barrier_prefaced_during_startup=true\n" +
		"service_barrier_handshake_elapsed=11ms\nservice_barrier_session_admission_elapsed=79ms\n" +
		"service_barrier_sweep_elapsed=31ms\nservice_barrier_verify_elapsed=2ms\nservice_barrier_verified_ready_elapsed=112ms\n" +
		"service_lost_log_typed=true\nservice_lost_log_disposition=swept:removed\n"
}

func servicePublicationReceipt(logEvidenceIncomplete bool) string {
	return "term_kill_escalation=true\nterm_kill_log_evidence_incomplete=" + strconv.FormatBool(logEvidenceIncomplete) +
		"\nterm_kill_log_seal_pairing=true\nterm_kill_stdout_log=true\nterm_kill_stderr_log=true\n" +
		"withdrawal=true\nwithdrawal_elapsed=83ms\nrepublication=true\nrepublication_elapsed=11.03s\n" +
		"republication_observation_deadline=11.4s\n"
}
