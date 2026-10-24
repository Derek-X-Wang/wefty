package lima

import (
	"flag"
	"path/filepath"
	"strings"
	"testing"
)

func TestSizingFlagsExposeExactSetupContract(t *testing.T) {
	flags := flag.NewFlagSet("setup-oci", flag.ContinueOnError)
	values := BindSizingFlags(flags, Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"})
	if err := flags.Parse([]string{"--vm-memory=3GiB", "--vm-cpus=2", "--vm-disk=48GiB"}); err != nil {
		t.Fatal(err)
	}
	got, err := values.Sizing()
	want := Sizing{Memory: "3GiB", CPUs: 2, Disk: "48GiB"}
	if err != nil || got != want {
		t.Fatalf("sizing flags = %#v, %v; want %#v", got, err, want)
	}
}

func TestDefaultSizingIsResolvedOnceFromOwnerDefaults(t *testing.T) {
	tests := []struct {
		memory int64
		cores  int
		want   Sizing
	}{
		{memory: 16 << 30, cores: 12, want: Sizing{Memory: "4096MiB", CPUs: 4, Disk: "32GiB"}},
		{memory: 8 << 30, cores: 6, want: Sizing{Memory: "2048MiB", CPUs: 4, Disk: "32GiB"}},
		{memory: 4 << 30, cores: 1, want: Sizing{Memory: "1024MiB", CPUs: 4, Disk: "32GiB"}},
	}
	for _, test := range tests {
		got, err := DefaultSizing(test.memory, test.cores)
		if err != nil || got != test.want {
			t.Fatalf("DefaultSizing(%d, %d) = %#v, %v; want %#v", test.memory, test.cores, got, err, test.want)
		}
	}
}

func TestConfiguredMacMemoryLeavesOneGiBForTheHostVM(t *testing.T) {
	// The size is the persisted host VM setting, not a live memory reading.
	// A 16 GiB Mac uses 4 GiB; a 32 GiB Mac uses 8 GiB (#1132).
	for _, test := range []struct {
		memory string
		room   int64
	}{{"4GiB", 3 << 30}, {"8GiB", 7 << 30}, {"6GiB", 5 << 30}} {
		sizing := Sizing{Memory: test.memory, CPUs: 4, Disk: DefaultDisk}
		capacity, reserve, err := DefaultMacComputerCapacity(sizing)
		if err != nil || reserve != 1<<30 || capacity-reserve != test.room {
			t.Fatalf("sizing=%+v capacity=%d reserve=%d err=%v; want room=%d", sizing, capacity, reserve, err, test.room)
		}
	}
	capacity, reserve, err := DefaultMacComputerCapacity(Sizing{Memory: "1GiB", CPUs: 4, Disk: DefaultDisk})
	if err != nil || capacity != 1<<30 || reserve != 1<<30 {
		t.Fatalf("a small host VM must still support non-Computer OCI work, with zero Computer room: %d/%d, %v", capacity, reserve, err)
	}
}

func TestParseByteQuantityUsesSetupSizingGrammar(t *testing.T) {
	for value, want := range map[string]int64{
		"1024MiB": 1 << 30,
		"2GiB":    2 << 30,
		"4GB":     4_000_000_000,
		"8kB":     8_000,
	} {
		got, err := ParseByteQuantity(value)
		if err != nil || got != want {
			t.Fatalf("ParseByteQuantity(%q) = %d, %v; want %d", value, got, err, want)
		}
	}
	for _, value := range []string{"0GiB", "4", "9223372036854775807GiB"} {
		if _, err := ParseByteQuantity(value); err == nil {
			t.Fatalf("ParseByteQuantity(%q) unexpectedly succeeded", value)
		}
	}
}

func TestTemplateCarriesOnlyTheNarrowLimaTransport(t *testing.T) {
	root := filepath.Join(string(filepath.Separator), "Users", "operator", "wefty-mounts")
	payload, err := RenderTemplate(TemplateConfig{
		Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	template := string(payload)
	for _, want := range []string{
		"vmType: vz", "memory: \"4GiB\"", "cpus: 4", "disk: \"32GiB\"",
		"system: true", "user: false", "modprobe overlay", "overlayfs",
		"template:_images/ubuntu-24.04",
		"mountPoint: \"/mnt/wefty-host\"", "writable: true",
		"guestSocket: \"/run/wefty/oci-helper.sock\"",
		"hostSocket: \"{{.Dir}}/sock/wefty-oci-helper.sock\"",
		"guestIP: \"0.0.0.0\"", "guestIPMustBeZero: false", "proto: any", "ignore: true",
	} {
		if !strings.Contains(template, want) {
			t.Fatalf("template missing %q:\n%s", want, template)
		}
	}
	forbiddenGateway := strings.Join([]string{"192", "168", "5", "2"}, ".")
	for _, forbidden := range []string{"containerd.sock\"\n    hostSocket", forbiddenGateway, "hostIP: \"0.0.0.0\"", "autostart"} {
		if strings.Contains(template, forbidden) {
			t.Fatalf("template contains forbidden %q:\n%s", forbidden, template)
		}
	}
}

func TestTemplateRejectsBroadOrRelativeMountRoots(t *testing.T) {
	for _, root := range []string{".", "relative", string(filepath.Separator)} {
		_, err := RenderTemplate(TemplateConfig{Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: root})
		if err == nil {
			t.Fatalf("accepted unsafe mount root %q", root)
		}
	}
}

func TestHelperSocketPathStaysInsideInstanceSocketDirectory(t *testing.T) {
	path, err := HelperSocketPath(filepath.Join(string(filepath.Separator), "Users", "operator", ".lima"), DefaultInstanceName)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(string(filepath.Separator), "Users", "operator", ".lima", DefaultInstanceName, "sock", HostHelperSocketName)
	if path != want {
		t.Fatalf("helper socket path = %q, want %q", path, want)
	}
	for _, unsafe := range []string{"../escape", ".", ".."} {
		if _, err := HelperSocketPath(t.TempDir(), unsafe); err == nil {
			t.Fatalf("accepted unsafe Lima instance name %q", unsafe)
		}
	}
}

func TestComputerRequestSizingUsesQuarterOfLargeMacAndFourCPUs(t *testing.T) {
	sizing, err := DefaultSizing(32<<30, 4)
	if err != nil {
		t.Fatal(err)
	}
	if sizing.Memory != "8192MiB" || sizing.CPUs != 4 {
		t.Fatalf("phase 3 host VM must use quarter RAM and 4 CPUs: %+v", sizing)
	}
}
