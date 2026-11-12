package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// setup-oci names what differs, the step still needed, and the host VM base.
// The base line is advisory: it names the optional upgrade step, never a
// required one (#1593).
func TestSetupOCINamesDifferencesActionAndAdvisoryBase(t *testing.T) {
	for name, tc := range map[string]struct {
		response ocicontrol.SetupResponse
		want     []string
		absent   []string
	}{
		"restart pending, older base": {
			response: ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceRestartRequired, Differences: []string{"vm_memory", "vm_cpus"},
				Base: &lima.HostVMBase{Current: "ubuntu-24.04", Desired: lima.TemplateGuestOS, UpgradeAvailable: true}},
			want: []string{"differs: vm_memory, vm_cpus; to apply, rerun setup-oci with --apply-restart\n",
				"host VM base upgrade available: guest_os current ubuntu-24.04, template " + lima.TemplateGuestOS + ". Staying on the current base is supported; to upgrade, rerun setup-oci with --recreate.\n"},
		},
		"base upgrade applied": {
			response: ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceRecreateRequired, RestartApplied: true, RecreateApplied: true, Differences: []string{"guest_os"},
				Base: &lima.HostVMBase{Current: lima.TemplateGuestOS, Desired: lima.TemplateGuestOS}},
			want:   []string{"differs: guest_os\n", "host VM base: " + lima.TemplateGuestOS + " (matches the template)\n"},
			absent: []string{"to apply", "upgrade available"},
		},
		"restart applied, unobserved base": {
			response: ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceRestartRequired, RestartApplied: true, Differences: []string{"vm_memory"},
				Base: &lima.HostVMBase{Desired: lima.TemplateGuestOS}},
			want:   []string{"differs: vm_memory\n", "host VM base: current not observed yet"},
			absent: []string{"to apply", "upgrade available"},
		},
		"unchanged, current base": {
			response: ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceUnchanged, Base: &lima.HostVMBase{Current: lima.TemplateGuestOS, Desired: lima.TemplateGuestOS}},
			want:     []string{"host VM base: " + lima.TemplateGuestOS + " (matches the template)\n"},
			absent:   []string{"differs"},
		},
		"no host VM": {
			response: ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceLiveSafe, Differences: []string{"probe_digest"}},
			want:     []string{"differs: probe_digest\n"},
			absent:   []string{"host VM base", "to apply"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeSetupConvergenceDetail(&out, tc.response); err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("output %q lacks %q", out.String(), want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(out.String(), absent) {
					t.Fatalf("output %q contains %q", out.String(), absent)
				}
			}
		})
	}
}
