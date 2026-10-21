package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/pathflags"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-l3: %v", err)
		os.Exit(1)
	}
}

func run() error {
	if handled, err := serviceplan.ProfilePreflight(os.Args[1:], os.Stdout); handled {
		return err
	}
	var (
		printVersion              = flag.Bool("version", false, "print the build commit and exit")
		statePreflight            = flag.Bool("state-preflight", false, "open --db for upgrade preflight and exit; use only a temporary copy")
		fabricMode                = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		plainFabricID             = flag.String("plain-fabric-id", os.Getenv("WEFTY_DEV_PLAIN_FABRIC_ID"), "DEVELOPMENT ONLY: shared plain Fabric ID (must start with plain-)")
		listenAddress             = flag.String("listen", l3.DefaultL3Address, "run-ledger Fabric listen address")
		controlPlane              = flag.String("control-plane", l3.DefaultL1Address, "L1 control-plane Fabric address")
		databasePath              = flag.String("db", "wefty-l3.sqlite", "SQLite database path")
		captureMax                = flag.Int64("exec-capture-max-bytes", l3.DefaultExecCaptureMaxBytes, "maximum bytes per cross-node exec capture")
		captureTotal              = flag.Int64("exec-capture-total-bytes", l3.DefaultExecCaptureTotalBytes, "maximum bytes held in exec captures")
		reconcileEvery            = flag.Duration("reconcile-interval", l3.DefaultReconcileInterval, "dispatch and run-state reconciliation interval")
		fabricName                = flag.String("fabric-name", l3.DefaultL3Address, "tsnet logical node name")
		stateDirectory            = flag.String("state-dir", "", "tsnet state directory")
		authKey                   = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL                = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral                 = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		printEnrollmentURL        = flag.Bool("fabric-print-enrollment-url", os.Getenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL") == "1", "print the raw tsnet enrollment URL instead of the wefty-owned notice on first login (also WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1)")
		readyFile                 = flag.String("ready-file", "", "write listener metadata after the server is ready")
		controlPlaneNodeID        = flag.String("control-plane-node-id", "control-plane", "authenticated Fabric Node ID allowed to revoke Computer grants administratively")
		computerAuthorityInstance = flag.String("computer-authority-instance", "default", "persisted L3 authority instance marker; change only for restore or promotion")
		profilePath               = flag.String("profile", os.Getenv(connprofile.EnvironmentVariable), "connection profile that supplies the Fabric, listen and control-plane addresses not set by flags (default $"+connprofile.EnvironmentVariable+")")
	)
	flag.Parse()
	if *printVersion {
		fmt.Println(version)
		return nil
	}
	if *statePreflight {
		store, err := l3.OpenStore(*databasePath, l3.StoreOptions{})
		if err != nil {
			return err
		}
		return store.Close()
	}
	profile, _, err := connprofile.Resolve(*profilePath, "")
	if err != nil {
		return err
	}
	settings := profile.L3()
	layer := connprofile.Layer{Explicit: connprofile.ExplicitFlags(flag.CommandLine)}
	layer.Fill(fabricMode, "fabric", "", settings.Fabric)
	layer.Fill(plainFabricID, "plain-fabric-id", "WEFTY_DEV_PLAIN_FABRIC_ID", settings.PlainFabricID)
	layer.Fill(listenAddress, "listen", "", settings.Listen)
	layer.Fill(controlPlane, "control-plane", "", settings.ControlPlane)
	if *captureMax <= 0 || *captureTotal <= 0 {
		return fmt.Errorf("exec capture caps must be positive")
	}
	if *reconcileEvery <= 0 {
		return fmt.Errorf("--reconcile-interval must be positive")
	}
	if err := pathflags.Absolutize(
		pathflags.Flag{Name: "db", Value: databasePath},
		pathflags.Flag{Name: "state-dir", Value: stateDirectory},
		pathflags.Flag{Name: "ready-file", Value: readyFile},
	); err != nil {
		return err
	}
	if err := requireReachableControlPlane(*fabricMode, *controlPlane); err != nil {
		return err
	}

	// Written unconditionally: an inherited WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1
	// must not survive an explicit --fabric-print-enrollment-url=false (wefty
	// #498). The resolved boolean, not just the true case, always wins.
	if *printEnrollmentURL {
		_ = os.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "1")
	} else {
		_ = os.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "0")
	}
	config, err := fabricconfig.SavedService(profile, connprofile.ParticipantL3, fabricconfig.Config{
		Mode:          *fabricMode,
		PlainFabricID: *plainFabricID,
		Identity: fabric.Identity{
			NodeID: "run-ledger",
			Tags:   []string{l1.DefaultClientPrincipalTag},
		},
		Name:           *fabricName,
		StateDirectory: *stateDirectory,
		AuthKey:        *authKey,
		ControlURL:     *controlURL,
		Ephemeral:      *ephemeral,
		Logf:           log.Printf,
	}, connprofile.ExplicitFlags(flag.CommandLine))
	if err != nil {
		return err
	}
	*controlPlaneNodeID, err = fabricconfig.TrustedServicePeer(profile, connprofile.ParticipantL1, *controlPlaneNodeID, connprofile.ExplicitFlags(flag.CommandLine)["control-plane-node-id"])
	if err != nil {
		return err
	}
	participant, closeFabric, err := fabricconfig.Open(context.Background(), config)
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return err
	}
	store, err := l3.OpenStore(*databasePath, l3.StoreOptions{ComputerAuthorityInstanceID: *computerAuthorityInstance, ExecCaptureMaxBytes: *captureMax, ExecCaptureTotalBytes: *captureTotal})
	if err != nil {
		return err
	}
	defer store.Close()
	l1Client, err := l3.NewL1Client(participant, *controlPlane)
	if err != nil {
		return err
	}
	defer l1Client.CloseIdleConnections()
	reconciler, err := l3.NewReconciler(store, l1Client, l3.ReconcilerConfig{
		Interval: *reconcileEvery,
		OnError:  func(err error) { log.Printf("reconcile: %v", err) },
	})
	if err != nil {
		return err
	}
	server, err := l3.NewServer(participant, store, l3.ServerConfig{Reconciler: reconciler, Logs: l1Client,
		ControlPlaneNodeID: *controlPlaneNodeID})
	if err != nil {
		return err
	}
	listener, err := participant.Listen("tcp", *listenAddress)
	if err != nil {
		return err
	}
	if err := publishReadyFile(*readyFile, listener.Addr().String()); err != nil {
		return err
	}
	log.Printf("listening on %s (reconcile every %s)", listener.Addr(), reconcileEvery.String())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Serve(ctx, listener)
}

// requireReachableControlPlane is L1's -run-ledger guard, mirrored (#604).
// Plain Fabric resolves a wefty:// logical name only inside the process that
// registered it, so the default control-plane name can never be dialed from a
// separate L3 process: the ledger would start, accept runs, and never dispatch
// one. The address is knowable at start, so it is refused at start.
func requireReachableControlPlane(fabricMode, controlPlane string) error {
	if !strings.EqualFold(strings.TrimSpace(fabricMode), "plain") {
		return nil
	}
	address := strings.TrimSpace(controlPlane)
	if address == "" {
		return fmt.Errorf("-control-plane is required: pass the control plane's host:port")
	}
	if !strings.HasPrefix(strings.ToLower(address), "wefty:") {
		return nil
	}
	return fmt.Errorf("-control-plane=%s is a logical Fabric address that plain Fabric resolves only within one process; "+
		"pass the control plane's host:port instead", controlPlane)
}

func publishReadyFile(path, address string) error {
	return serviceready.Publish(path, serviceready.Metadata{Address: address})
}
