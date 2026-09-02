package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/computerconformance"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(arguments []string) int {
	var config computerconformance.RuntimeConfig
	flags := flag.NewFlagSet("wefty-computer-conformance", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&config.Image, "image", "", "OCI image reference or digest to check (required)")
	flags.StringVar(&config.RepairImage, "repair-image", "", "known-good immutable image used only for detached-root permission repair (required)")
	flags.StringVar(&config.Runtime, "runtime", "docker", "OCI command-line runtime: docker or nerdctl")
	flags.StringVar(&config.Platform, "platform", "", "optional Linux platform, for example linux/amd64")
	flags.StringVar(&config.InputOraclePath, "input-oracle-path", "", "absolute image path to an input receipt; omission reports input checks NOT-RUN")
	flags.StringVar(&config.DriverOraclePath, "driver-oracle-path", "", "absolute image path to observed driver state; omission reports consumer checks NOT-RUN")
	flags.StringVar(&config.EdgeProcessPattern, "edge-process-pattern", "", "image process substring used to prove edge loss and recovery; omission reports the cell NOT-RUN")
	flags.StringVar(&config.MutationProfile, "mutation-profile", "", "repository acceptance-fixture profile mutation")
	flags.StringVar(&config.ReceiptPath, "receipt", "computer-conformance.json", "machine-readable receipt path, or - for stdout")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 64
	}
	if flags.NArg() != 0 || config.Image == "" || !strings.Contains(config.RepairImage, "@sha256:") || (config.Runtime != "docker" && config.Runtime != "nerdctl") ||
		(config.InputOraclePath != "" && !filepath.IsAbs(config.InputOraclePath)) ||
		(config.DriverOraclePath != "" && !filepath.IsAbs(config.DriverOraclePath)) {
		fmt.Fprintln(os.Stderr, "usage: wefty-computer-conformance --image IMAGE --repair-image IMAGE@DIGEST [options]")
		return 64
	}

	result := computerconformance.Run(context.Background(), config)
	payload, marshalErr := computerconformance.Marshal(result.Receipt)
	if marshalErr != nil {
		fmt.Fprintf(os.Stderr, "wefty-computer-conformance: encode receipt: %v\n", marshalErr)
		return 1
	}
	if err := writeReceipt(config.ReceiptPath, payload); err != nil {
		fmt.Fprintf(os.Stderr, "wefty-computer-conformance: write receipt: %v\n", err)
		return 1
	}
	writeSummary(result.Receipt, config.ReceiptPath)
	if result.Err != nil {
		fmt.Fprintf(os.Stderr, "conformance stopped: %v\n", result.Err)
		return 1
	}
	switch result.Receipt.Status {
	case computerconformance.StatusPass:
		return 0
	case computerconformance.StatusNotRun:
		return 2
	default:
		return 1
	}
}

func writeReceipt(path string, payload []byte) error {
	if path == "-" {
		_, err := os.Stdout.Write(append(payload, '\n'))
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	temporary := path + ".new"
	if err := os.WriteFile(temporary, append(payload, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func writeSummary(receipt computerconformance.Receipt, receiptPath string) {
	writer := os.Stdout
	if receiptPath == "-" {
		writer = os.Stderr
	}
	passed, failed, notRun := 0, 0, 0
	for _, check := range receipt.Checks {
		switch check.Status {
		case computerconformance.StatusPass:
			passed++
		case computerconformance.StatusFail:
			failed++
		case computerconformance.StatusNotRun:
			notRun++
		}
		detail := ""
		if check.Status != computerconformance.StatusPass && check.Detail != "" {
			detail = " — " + check.Detail
		}
		fmt.Fprintf(writer, "%-7s %-18s %-42s %s%s\n", check.Status, check.Scope, check.ID, check.Summary, detail)
	}
	fmt.Fprintf(writer, "image=%s harness=%s containerd-profile=%s — %d PASS, %d FAIL, %d NOT-RUN; receipt=%s\n", receipt.ImageStatus, receipt.HarnessStatus, receipt.ContainerdProfileStatus, passed, failed, notRun, receiptPath)
}
