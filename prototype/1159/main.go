// A disposable launchd experiment. It deliberately imports only the standard
// library so building it never needs network access or the installed wefty stack.
package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("use controller, payload, plist, or bounded"))
	}
	var err error
	switch os.Args[1] {
	case "controller":
		err = controller(os.Args[2:])
	case "payload":
		err = payload(os.Args[2:])
	case "plist":
		err = plist(os.Args[2:])
	case "bounded":
		err = bounded(os.Args[2:])
	default:
		err = errors.New("unknown mode")
	}
	if err != nil {
		fatal(err)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func event(f *os.File, kind string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"], fields["time"], fields["pid"] = kind, time.Now().UTC().Format(time.RFC3339Nano), os.Getpid()
	if err := json.NewEncoder(f).Encode(fields); err != nil {
		fatal(err)
	}
	if err := f.Sync(); err != nil {
		fatal(err)
	}
}
func openLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
}

// An io.Writer wrapper is intentional: exec.Cmd creates a pipe and a reader in
// the controller, as runner/process does. Passing *os.File would test different
// output ownership and falsely imply that logging survives controller death.
type pipeLog struct {
	mu   sync.Mutex
	file *os.File
}

func (w *pipeLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Write(p)
}

func controller(args []string) error {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	dir := fs.String("dir", "", "generation evidence directory")
	seconds := fs.Int("seconds", 24, "payload duration")
	code := fs.Int("code", 17, "payload exit code")
	term := fs.String("term", "forward", "forward or leave")
	group := fs.String("group", "separate", "separate (runner/process) or shared control")
	outputMode := fs.String("output", "pipe", "pipe (runner/process) or file diagnostic control")
	budget := fs.Duration("shutdown-budget", 15*time.Second, "first TERM drain budget; second TERM forces cancellation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(*dir) || (*term != "forward" && *term != "leave") || (*group != "separate" && *group != "shared") || (*outputMode != "pipe" && *outputMode != "file") || *budget < 0 {
		return errors.New("invalid controller arguments")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		return err
	}
	events, err := openLog(filepath.Join(*dir, fmt.Sprintf("controller-%d.jsonl", os.Getpid())))
	if err != nil {
		return err
	}
	defer events.Close()
	event(events, "controller_start", map[string]any{"pgid": syscall.Getpgrp(), "ppid": os.Getppid(), "term": *term, "group": *group, "output": *outputMode, "shutdown_budget": budget.String()})
	// KeepAlive remains exactly as in wefty's plist. A crash may relaunch this
	// controller, but this claim prevents a second payload from hiding the first
	// one's fate. The restart is evidence, not an adopted payload.
	claim, err := os.OpenFile(filepath.Join(*dir, "payload.claim"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		event(events, "restart_without_payload", nil)
		return nil
	}
	if err != nil {
		return err
	}
	claim.Close()
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt, syscall.SIGUSR1)
	defer signal.Stop(signals)
	output, err := openLog(filepath.Join(*dir, "output.log"))
	if err != nil {
		return err
	}
	defer output.Close()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	command := exec.Command(exe, "payload", "--seconds", strconv.Itoa(*seconds), "--code", strconv.Itoa(*code), "--witness", filepath.Join(*dir, "witness.jsonl"))
	if *group == "separate" {
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	writer := &pipeLog{file: output}
	command.Stdout, command.Stderr = writer, writer
	if *outputMode == "file" {
		command.Stdout, command.Stderr = output, output
	}
	if err := command.Start(); err != nil {
		return err
	}
	pid := command.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return err
	}
	event(events, "payload_start", map[string]any{"payload_pid": pid, "payload_pgid": pgid, "controller_pgid": syscall.Getpgrp()})
	ready := filepath.Join(*dir, "ready.tmp")
	if err := os.WriteFile(ready, []byte(fmt.Sprintf("%d %d %d\n", os.Getpid(), pid, pgid)), 0600); err != nil {
		return err
	}
	if err := os.Rename(ready, filepath.Join(*dir, "ready")); err != nil {
		return err
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	var budgetC, killC <-chan time.Time
	var budgetTimer, killTimer *time.Timer
	defer func() {
		if budgetTimer != nil {
			budgetTimer.Stop()
		}
		if killTimer != nil {
			killTimer.Stop()
		}
	}()
	draining, forced := false, false
	forward := func(reason string) {
		if forced {
			return
		}
		forced = true
		// The shared-group control must not signal itself recursively.
		var e error
		if *group == "separate" {
			e = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			e = command.Process.Signal(syscall.SIGTERM)
		}
		event(events, "forward_term", map[string]any{"reason": reason, "error": fmt.Sprint(e), "payload_pgid": pgid})
		killTimer = time.NewTimer(5 * time.Second)
		killC = killTimer.C
	}
	for {
		select {
		case e := <-waited:
			fields := map[string]any{"payload_pid": pid, "wait_error": fmt.Sprint(e), "exit_code": command.ProcessState.ExitCode(), "observed_by_parent": true}
			if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				fields["signal"] = status.Signal().String()
			}
			event(events, "payload_wait", fields)
			// A chosen payload failure is not a controller crash. Exit cleanly so
			// KeepAlive's SuccessfulExit=false does not repeat the experiment.
			event(events, "controller_exit", map[string]any{"exit_code": 0})
			return nil
		case sig := <-signals:
			event(events, "controller_signal", map[string]any{"signal": sig.String()})
			if sig == syscall.SIGUSR1 {
				event(events, "retiring", map[string]any{"normal_shutdown_clock_started": false})
				continue
			}
			if *term == "leave" {
				event(events, "leave_payload_running", nil)
				continue
			}
			if draining {
				forward("second_signal")
				continue
			}
			draining = true
			event(events, "draining", nil)
			if *budget > 0 {
				budgetTimer = time.NewTimer(*budget)
				budgetC = budgetTimer.C
			}
		case <-budgetC:
			budgetC = nil
			forward("shutdown_budget")
		case <-killC:
			killC = nil
			var e error
			if *group == "separate" {
				e = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				e = command.Process.Kill()
			}
			event(events, "forward_kill", map[string]any{"error": fmt.Sprint(e)})
		}
	}
}

func payload(args []string) error {
	fs := flag.NewFlagSet("payload", flag.ContinueOnError)
	seconds := fs.Int("seconds", 24, "one counter per second")
	code := fs.Int("code", 17, "chosen exit code")
	witness := fs.String("witness", "", "independent diagnostic evidence, not controller output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *seconds < 1 || *seconds > 60 || *code < 0 || *code > 255 {
		return errors.New("invalid payload duration or code")
	}
	log, err := openLog(*witness)
	if err != nil {
		return err
	}
	defer log.Close()
	event(log, "payload_identity", map[string]any{"pgid": syscall.Getpgrp(), "ppid": os.Getppid()})
	for i := 1; i <= *seconds; i++ {
		event(log, "tick_before_stdout", map[string]any{"counter": i, "ppid": os.Getppid(), "pgid": syscall.Getpgrp()})
		if _, err := fmt.Printf("counter=%d pid=%d pgid=%d ppid=%d time=%s\n", i, os.Getpid(), syscall.Getpgrp(), os.Getppid(), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	event(log, "exit_intent", map[string]any{"code": *code, "ppid": os.Getppid()})
	// This intent is NOT a wait status: only the original parent's payload_wait
	// event can prove that. SIGKILL/SIGPIPE may prevent an intent entirely.
	os.Exit(*code)
	return nil
}

func plist(args []string) error {
	fs := flag.NewFlagSet("plist", flag.ContinueOnError)
	label := fs.String("label", "", "isolated experiment label")
	dir := fs.String("dir", "", "generation evidence directory")
	abandon := fs.Bool("abandon", false, "add AbandonProcessGroup=true; otherwise omit")
	term := fs.String("term", "forward", "controller TERM policy")
	group := fs.String("group", "separate", "payload process group policy")
	outputMode := fs.String("output", "pipe", "pipe or file diagnostic control")
	seconds := fs.Int("seconds", 24, "payload duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !strings.HasPrefix(*label, "exp.wefty1159.") || strings.ContainsAny(*label, "/ \t\r\n") || !filepath.IsAbs(*dir) {
		return errors.New("invalid experiment label or directory")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	esc := func(s string) string { var b strings.Builder; _ = xml.EscapeText(&b, []byte(s)); return b.String() }
	fmt.Print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n")
	fmt.Printf("<key>Label</key><string>%s</string>\n<key>ProgramArguments</key><array>\n", esc(*label))
	for _, a := range []string{exe, "controller", "--dir", *dir, "--term", *term, "--group", *group, "--output", *outputMode, "--seconds", strconv.Itoa(*seconds), "--code", "17", "--shutdown-budget", "15s"} {
		fmt.Printf("<string>%s</string>\n", esc(a))
	}
	fmt.Print("</array>\n")
	for _, kv := range [][2]string{{"WorkingDirectory", *dir}, {"StandardOutPath", filepath.Join(*dir, "launchd.log")}, {"StandardErrorPath", filepath.Join(*dir, "launchd.log")}, {"ProcessType", "Interactive"}} {
		fmt.Printf("<key>%s</key><string>%s</string>\n", kv[0], esc(kv[1]))
	}
	// Match today's node-agent plist. ExitTimeOut=15+5+5+30+3*10+10=95.
	// No secrets or installed HOME/PATH/profile are needed by this absolute binary.
	fmt.Print("<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>\n<key>ThrottleInterval</key><integer>10</integer>\n<key>ExitTimeOut</key><integer>95</integer>\n")
	if *abandon {
		fmt.Print("<key>AbandonProcessGroup</key><true/>\n")
	}
	fmt.Println("</dict></plist>")
	return nil
}

func bounded(args []string) error {
	fs := flag.NewFlagSet("bounded", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 8*time.Second, "command deadline")
	if err := fs.Parse(args); err != nil {
		return err
	}
	argv := fs.Args()
	if len(argv) == 0 {
		return errors.New("missing command")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	command.WaitDelay = time.Second
	err := command.Run()
	if ctx.Err() != nil {
		return fmt.Errorf("command timeout: %w", ctx.Err())
	}
	return err
}

// Keep io in the interface check so changes cannot accidentally remove the
// wrapper and make exec.Cmd pass the output file directly into the payload.
var _ io.Writer = (*pipeLog)(nil)
