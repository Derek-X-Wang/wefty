package ocicontrol

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// AP-3 also covers the public doctor: an intentionally absent veth, DNS proxy
// and firewall must not turn a healthy loopback-only Computer into a fault.
func TestDoctorAcceptsProvenComputerWithoutInternet(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		proof      bool
		otherLive  bool
		hostSocket bool
		want       DiagnosticOutcome
	}{
		{"loopback-only", true, false, false, DiagnosticOK},
		{"unverified-loopback", false, false, false, DiagnosticNotRun},
		{"another-internet-Computer-needs-firewall", true, true, false, DiagnosticFailed},
		{"host-socket-visible", true, false, true, DiagnosticFailed},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			config := healthyDoctorConfig(time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC), "")
			base := config.Helper
			config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
				snapshot, err := base(ctx)
				profile := snapshot.Runtime.LastProfile
				profile.ComputerNetwork = ocihelper.ComputerNetworkNone
				profile.ComputerLoopbackOnly = scenario.proof
				profile.ComputerNetworkAddress, profile.ComputerNetworkGateway, profile.ComputerResolverAddress = "", "", ""
				profile.ComputerDNSProxyUDP, profile.ComputerDNSProxyTCP, profile.ComputerDNSUpstreamReachable = false, false, false
				profile.ComputerDNSUpstreamAddress, profile.ComputerDNSUpstreamSource = "", ""
				profile.ComputerIPv6NATState = ""
				profile.HostAbstractSocketVisible = scenario.hostSocket
				snapshot.Runtime.ComputerAttemptsLive = scenario.otherLive
				snapshot.Runtime.ComputerFirewallPresent = false
				snapshot.Runtime.ComputerFirewallRead = ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadFailed, ErrorCode: ocihelper.DiagnosticErrorComputerFirewall}
				return snapshot, err
			}
			report := BuildDoctor(t.Context(), config)
			if report.ComputerScreenIsolation.Outcome != scenario.want {
				t.Fatalf("none Computer health = %s; want %s: %+v", report.ComputerScreenIsolation.Outcome, scenario.want, report.ComputerScreenIsolation)
			}
			var human bytes.Buffer
			if err := WriteDoctorHuman(&human, report); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(human.String(), "computer_network=none loopback_only="+map[bool]string{true: "true", false: "false"}[scenario.proof]) {
				t.Fatalf("human doctor omitted the intended network: %s", human.String())
			}
		})
	}
}
