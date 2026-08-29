package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/computerconformance"
)

func main() {
	var config computerconformance.RuntimeConfig
	flag.StringVar(&config.Image, "image", "", "OCI image reference or digest to check (required)")
	flag.StringVar(&config.Runtime, "runtime", "docker", "OCI command-line runtime: docker or nerdctl")
	flag.StringVar(&config.Platform, "platform", "", "optional Linux platform, for example linux/amd64")
	flag.StringVar(&config.InputOraclePath, "input-oracle-path", "", "absolute image path to an input receipt; omission reports input checks NOT-RUN")
	flag.StringVar(&config.DriverOraclePath, "driver-oracle-path", "", "absolute image path to observed driver state; omission reports consumer checks NOT-RUN")
	flag.StringVar(&config.ReceiptPath, "receipt", "computer-conformance.json", "machine-readable receipt path, or - for stdout")
	flag.Parse()

	result := computerconformance.Run(context.Background(), config)
	payload, marshalErr := computerconformance.Marshal(result.Receipt)
	if marshalErr != nil {
		fmt.Fprintf(os.Stderr, "wefty-computer-conformance: encode receipt: %v\n", marshalErr)
		os.Exit(1)
	}
	if err := writeReceipt(config.ReceiptPath, payload); err != nil {
		fmt.Fprintf(os.Stderr, "wefty-computer-conformance: write receipt: %v\n", err)
		os.Exit(1)
	}
	writeSummary(result.Receipt, config.ReceiptPath)
	if result.Err != nil {
		fmt.Fprintf(os.Stderr, "conformance stopped: %v\n", result.Err)
	}
	switch result.Receipt.Status {
	case computerconformance.StatusPass:
		return
	case computerconformance.StatusNotRun:
		os.Exit(2)
	default:
		os.Exit(1)
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
		fmt.Fprintf(writer, "%-7s %-42s %s\n", check.Status, check.ID, check.Summary)
	}
	fmt.Fprintf(writer, "%s — %d PASS, %d FAIL, %d NOT-RUN; receipt=%s\n", receipt.Status, passed, failed, notRun, receiptPath)
}
