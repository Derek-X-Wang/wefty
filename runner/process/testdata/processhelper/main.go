package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
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
	default:
		fatalf("unknown mode %q", os.Args[1])
	}
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
	child.Stdout = nil
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		fatalf("start child: %v", err)
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
