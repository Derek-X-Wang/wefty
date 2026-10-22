package systemduser

import (
	"strings"
	"testing"
)

func TestRenderQuotesEveryArgumentAndSeparatesEnvironmentExpansion(t *testing.T) {
	u := Unit{Name: "dev.wefty.node-agent.service", Program: `/home/owner/bin/wefty agent`, Arguments: []string{"", `space arg`, `a"b`, `%h`, `$HOME`, `a\b`}, WorkingDirectory: "/home/owner/data", LogPath: "/home/owner/log", InputDigest: "digest-one", Environment: map[string]string{"PATH": `/opt/$tools:slash\quote"%h`}, After: []string{"dev.wefty.l3.service"}}
	b, err := Render(u)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`ExecStart=:"/home/owner/bin/wefty agent" "" "space arg" "a\"b" "%%h" "$HOME" "a\\b"`,
		`Environment="PATH=/opt/$tools:slash\\quote\"%%h"`,
		"After=dev.wefty.l3.service\n", "Restart=on-failure\n", "WantedBy=default.target\n", "# wefty-input-digest: digest-one\n",
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %q in:\n%s", want, b)
		}
	}
	if strings.Contains(string(b), "User=") || strings.Contains(string(b), "Group=") {
		t.Fatal("user unit selects an account")
	}
	u.InputDigest = "digest-two"
	changed, err := Render(u)
	if err != nil || string(changed) == string(b) {
		t.Fatal("digest did not change rendered bytes")
	}
	path, err := RecordedPATH(b)
	if err != nil || path != u.Environment["PATH"] {
		t.Fatalf("recorded PATH: %q %v", path, err)
	}
}

// systemd v252 load-fragment.c treats these as scalar paths, not word lists:
// quoting the whole WorkingDirectory or append: value makes quotes literal.
func TestRenderScalarPathsAndLiteralDollarExecutable(t *testing.T) {
	u := Unit{Name: "dev.wefty.client-service.service", Program: `/home/$owner/bin/wefty`, Arguments: []string{`$HOME`, `${TOKEN}`, `;`}, WorkingDirectory: `/home/$owner/data "quoted"\dir%h`, LogPath: `/home/$owner/log "quoted"\dir%h.log`}
	b, err := Render(u)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`WorkingDirectory=/home/$owner/data "quoted"\dir%%h` + "\n",
		`StandardOutput=append:/home/$owner/log "quoted"\dir%%h.log` + "\n",
		`StandardError=append:/home/$owner/log "quoted"\dir%%h.log` + "\n",
		`ExecStart=:"/home/$owner/bin/wefty" "$HOME" "${TOKEN}" ";"` + "\n",
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing literal %q in:\n%s", want, b)
		}
	}
}

func TestRenderRejectsUnsafeExecutablePaths(t *testing.T) {
	for _, c := range []string{"'", `"`, `\`, "\t", "\x1f", "\x7f"} {
		u := Unit{Name: "dev.wefty.l1.service", Program: "/home/owner" + c + "/wefty", WorkingDirectory: "/home/owner", LogPath: "/home/owner/log"}
		if _, err := Render(u); err == nil {
			t.Errorf("unsafe program character %q accepted", c)
		}
	}
}
func TestRenderAlwaysBoundsStopTime(t *testing.T) {
	for _, seconds := range []int{0, 47} {
		u := Unit{Name: "dev.wefty.l1.service", Program: "/home/owner/wefty", WorkingDirectory: "/home/owner", LogPath: "/home/owner/log", TimeoutStopSeconds: seconds}
		b, err := Render(u)
		if err != nil {
			t.Fatal(err)
		}
		want := "TimeoutStopSec=20s\n"
		if seconds == 47 {
			want = "TimeoutStopSec=47s\n"
		}
		if !strings.Contains(string(b), want) {
			t.Fatalf("stop timeout missing: %s", b)
		}
	}
}
