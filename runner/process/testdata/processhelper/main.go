package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("missing mode")
	}

	switch os.Args[1] {
	case "exit":
		exit()
	case "stdout":
		write(os.Stdout)
	case "stderr":
		write(os.Stderr)
	case "hang":
		hang()
	case "spawn-child":
		spawnChild()
	case "wait-release":
		waitRelease()
	case "sleep":
		sleep()
	case "paced-output":
		pacedOutput()
	case "raw-output":
		rawOutput()
	default:
		fatalf("unknown mode %q", os.Args[1])
	}
}

func pacedOutput() {
	if len(os.Args) != 3 {
		fatalf("paced-output mode requires milliseconds")
	}
	milliseconds, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse paced-output duration: %v", err)
	}
	_, _ = os.Stdout.Write([]byte("first\n"))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	_, _ = os.Stdout.Write([]byte("second\n"))
}

func rawOutput() {
	_, _ = os.Stdout.Write(bytes.Repeat([]byte{'x'}, 70*1024))
	_, _ = os.Stdout.Write([]byte("partial-without-newline"))
	_, _ = os.Stdout.Write([]byte{0xff, 0xfe, 0x00, '\n'})
}

func sleep() {
	if len(os.Args) != 3 {
		fatalf("sleep mode requires milliseconds")
	}
	milliseconds, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse sleep duration: %v", err)
	}
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

func exit() {
	if len(os.Args) != 3 {
		fatalf("exit mode requires one code")
	}
	code, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fatalf("parse exit code: %v", err)
	}
	os.Exit(code)
}

func write(file *os.File) {
	if len(os.Args) == 3 && os.Args[2] == "@signal" {
		signalOutput(file)
		return
	}
	for _, value := range os.Args[2:] {
		switch value {
		case "@cwd":
			workingDirectory, err := os.Getwd()
			if err != nil {
				fatalf("get working directory: %v", err)
			}
			fmt.Fprintln(file, workingDirectory)
		default:
			if len(value) > 8 && value[:8] == "@repeat:" {
				// @repeat:<unit>:<count> keeps huge payloads out of argv;
				// Linux caps a single argument string at 128 KiB.
				spec := value[8:]
				separator := bytes.LastIndexByte([]byte(spec), ':')
				if separator <= 0 {
					fatalf("malformed repeat directive %q", value)
				}
				count, err := strconv.Atoi(spec[separator+1:])
				if err != nil {
					fatalf("parse repeat count: %v", err)
				}
				_, _ = file.Write(bytes.Repeat([]byte(spec[:separator]), count))
				fmt.Fprintln(file)
				continue
			}
			if len(value) > 5 && value[:5] == "@env:" {
				fmt.Fprintln(file, os.Getenv(value[5:]))
				continue
			}
			fmt.Fprintln(file, value)
		}
	}
}

func hang() {
	ignoreTermination(nil)
	fmt.Fprintln(os.Stdout, os.Getpid())
	select {}
}

func spawnChild() {
	executable, err := os.Executable()
	if err != nil {
		fatalf("locate helper executable: %v", err)
	}
	child := exec.Command(executable, "hang")
	ready, err := child.StdoutPipe()
	if err != nil {
		fatalf("open child readiness pipe: %v", err)
	}
	defer ready.Close()
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		fatalf("start child: %v", err)
	}
	// hang publishes its PID only after installing its SIGTERM handler.
	// Parent output therefore proves both group members are signal-ready.
	if _, err := bufio.NewReader(ready).ReadString('\n'); err != nil {
		fatalf("read child signal readiness: %v", err)
	}
	ignoreTermination(func() { fmt.Fprintln(os.Stdout, "term") })
	fmt.Fprintln(os.Stdout, child.Process.Pid)
	select {}
}

func signalOutput(file *os.File) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGTERM)
	fmt.Fprintln(file, os.Getpid())
	for received := range signals {
		if received == syscall.SIGUSR1 {
			fmt.Fprintln(file, "tick")
		}
	}
}

func ignoreTermination(onTermination func()) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM)
	go func() {
		for range signals {
			if onTermination != nil {
				onTermination()
			}
		}
	}()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

// waitRelease is a test-only rendezvous: the real process remains alive until
// its parent test has observed the lease evidence it needs over HTTP.
func waitRelease() {
	if len(os.Args) != 4 {
		fatalf("wait-release requires ready and release paths")
	}
	if err := os.WriteFile(os.Args[2], []byte("ready"), 0o600); err != nil {
		fatalf("publish workload readiness: %v", err)
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for range poll.C {
		if _, err := os.Stat(os.Args[3]); err == nil {
			return
		} else if !os.IsNotExist(err) {
			fatalf("observe release: %v", err)
		}
	}
}
