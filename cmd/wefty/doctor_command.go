package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// Flags are parsed before any local install or Fabric access. A policy reprint
// needs no saved profile, client service, key or administrative credential.
func executeDoctor(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, options.jsonOutput))
	printPolicy := flags.Bool("print-policy", false, "print the required private network policy without probing or changing it")
	if err := parseCommandFlags(flags, args, false); err != nil {
		return flagUsageError(err)
	}
	if flags.NArg() != 0 {
		return usageError("usage: wefty doctor [--print-policy]")
	}
	if *printPolicy {
		policy, err := fabricconfig.PrivateNetworkPolicy("tsnet")
		if err != nil {
			return err
		}
		if options.jsonOutput {
			return writeJSON(stdout, policy)
		}
		if _, err := fmt.Fprintln(stdout, "Private network policy"); err != nil {
			return err
		}
		for _, requirement := range policy.Requirements {
			if _, err := fmt.Fprintln(stdout, "-", requirement); err != nil {
				return err
			}
		}
		for _, note := range policy.Notes {
			if _, err := fmt.Fprintln(stdout, "Note:", note); err != nil {
				return err
			}
		}
		_, err = io.WriteString(stdout, policy.Snippet)
		return err
	}
	// The established node route preserves the OCI and plain command behavior.
	return executeLocalNode(ctx, options, []string{"doctor"}, stdout, stderr)
}

func maybeExecuteFleetDoctor(ctx context.Context, host setupHost, options globalOptions, stdout io.Writer) (bool, error) {
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return false, nil
	}
	paths, _ := resolveMaintenancePaths(home)
	role, err := doctorRole(paths)
	if err != nil {
		report := plainDoctorReport{Version: 1, Install: "fleet-process", Role: role, Healthy: false, Findings: []plainDoctorFinding{{Check: "role-receipt", Severity: "error", Detail: "cannot verify the recorded fleet installation role", Fix: "restore the original installation receipt or resume setup with its recorded role"}}}
		if writeErr := writeDoctorReport(stdout, report, options.jsonOutput); writeErr != nil {
			return true, writeErr
		}
		return true, &doctorResultError{cause: &notReadyError{verdict: "fleet doctor could not verify its role receipt; follow the fix hint"}}
	}
	if role == serviceplan.Legacy {
		return false, nil
	}
	report, diagnosis := runFleetDoctor(ctx, host, options, paths, role)
	if err := writeDoctorReport(stdout, report, options.jsonOutput); err != nil {
		return true, err
	}
	if diagnosis != nil {
		return true, &doctorResultError{cause: diagnosis}
	}
	return true, nil
}
