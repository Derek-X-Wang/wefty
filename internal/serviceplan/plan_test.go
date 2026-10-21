package serviceplan

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestInstallationRolesSelectExactServicePlans(t *testing.T) {
	for _, tc := range []struct {
		role                             Role
		name                             string
		services, binaries, participants []string
	}{
		{ControlPlane, "control-plane", []string{"l1", "l3", "node-agent", "client-service"}, []string{"wefty-l1", "wefty-l3", "wefty-agent", "wefty"}, []string{"l1", "l3", "node-agent", "client"}},
		{Join, "join", []string{"node-agent", "client-service"}, []string{"wefty-agent", "wefty"}, []string{"node-agent", "client"}},
		{Legacy, "legacy-plain", []string{"l1", "l3", "node-agent"}, []string{"wefty-l1", "wefty-l3", "wefty-agent"}, []string{"", "", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := Build(tc.role, Paths{Profile: "/config/profile.json", Data: "/data", Logs: "/logs", Spool: "/spool", Managed: "/managed", ClientSocket: planTestSocket(t)})
			if err != nil {
				t.Fatal(err)
			}
			var names, binaries, participants []string
			for _, service := range plan.Services {
				names = append(names, service.Name)
				binaries = append(binaries, service.Binary)
				participants = append(participants, service.Participant)
				if service.Removal.Service != service.Name || service.Removal.Binary != service.Binary || len(service.Arguments) == 0 {
					t.Fatalf("incomplete entry: %+v", service)
				}
			}
			if plan.Name != tc.name || !reflect.DeepEqual(names, tc.services) || !reflect.DeepEqual(binaries, tc.binaries) || !reflect.DeepEqual(participants, tc.participants) {
				t.Fatalf("plan=%+v", plan)
			}
		})
	}
	if _, err := Build(Role("guessed-from-records"), Paths{}); err == nil {
		t.Fatal("unknown installation role accepted")
	}
}

func TestServicePlanRejectsInvalidOrSharedPaths(t *testing.T) {
	good := Paths{Profile: "/plan/profile.json", Data: "/plan/data", Logs: "/plan/logs", Spool: "/plan/spool", Managed: "/plan/managed", ClientSocket: planTestSocket(t)}
	if _, err := Build(Join, Paths{}); err == nil {
		t.Error("empty joining plan accepted")
	}
	for _, field := range []string{"profile", "data", "logs", "spool", "managed", "socket"} {
		for _, bad := range []string{"", "relative/path", "/plan/../other", "/plan//other"} {
			paths := good
			switch field {
			case "profile":
				paths.Profile = bad
			case "data":
				paths.Data = bad
			case "logs":
				paths.Logs = bad
			case "spool":
				paths.Spool = bad
			case "managed":
				paths.Managed = bad
			case "socket":
				paths.ClientSocket = bad
			}
			if _, err := Build(Join, paths); err == nil {
				t.Errorf("accepted %s path %q", field, bad)
			}
		}
	}
	for _, socket := range []string{"/tmp/service.sock", "/private/tmp/service.sock", "/var/tmp/service.sock", "/service.sock"} {
		paths := good
		paths.ClientSocket = socket
		if _, err := Build(Join, paths); err == nil {
			t.Errorf("accepted socket in shared folder: %s", socket)
		}
	}
	paths := good
	paths.ClientSocket = "/plan/client/" + strings.Repeat("s", 101)
	if _, err := Build(Join, paths); err == nil {
		t.Error("accepted overlong socket path")
	}
	if err := os.Chmod(filepath.Dir(good.ClientSocket), 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(Join, good); err == nil {
		t.Error("accepted socket in existing shared folder")
	}
	// No socket is used by the legacy plan, so it needs no socket path.
	good.ClientSocket = ""
	if _, err := Build(Legacy, good); err != nil {
		t.Fatalf("unused legacy socket became required: %v", err)
	}
}

func planTestSocket(t *testing.T) string {
	t.Helper()
	folder, err := os.MkdirTemp("/tmp", "wsp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(folder) })
	return filepath.Join(folder, "service.sock")
}
