package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
)

func computerGrantObservationError(code contract.ErrorCode, message string, result l1.ComputerGrantMutationResult) error {
	details := map[string]any{
		"mutation_applied":         result.MutationApplied,
		"observation_state":        result.ObservationState,
		"last_observed_revocation": result.LastObservedRevocation,
	}
	if result.ObservationFailure != "" {
		details["observation_failure"] = result.ObservationFailure
	}
	return &apiResponseError{Service: "CLI", StatusCode: 0, APIError: contract.APIError{
		Code: code, Message: message, Retryable: true, Details: details,
	}}
}

func computerGrantObservationMessage(result l1.ComputerGrantMutationResult, suffix string) string {
	if result.MutationApplied {
		return "revocation was applied but " + suffix
	}
	return "replayed revocation mutation was not reapplied and " + suffix
}

func executeAdminPolicy(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "get" {
		policy, err := clients.getAdminPolicy(ctx)
		if err != nil {
			return err
		}
		return writeAdminPolicy(stdout, policy, jsonOutput)
	}
	if len(args) == 0 || (args[0] != "add" && args[0] != "remove") {
		return usageError("usage: wefty admin policy get | wefty admin policy add|remove USER_ID --policy-revision REVISION")
	}
	mutationArgs := moveFirstPositionalToEnd(args[1:])
	flags := flag.NewFlagSet("admin policy "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var revision int64
	flags.Int64Var(&revision, "policy-revision", 0, "policy revision observed by admin policy get")
	if err := flags.Parse(mutationArgs); err != nil {
		return usageError(err.Error())
	}
	seenRevision := false
	flags.Visit(func(visited *flag.Flag) { seenRevision = seenRevision || visited.Name == "policy-revision" })
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" || !seenRevision || revision < 1 {
		return usageError("usage: wefty admin policy add|remove USER_ID --policy-revision REVISION")
	}
	policy, err := clients.mutateAdmin(ctx, flags.Arg(0), revision, args[0] == "remove")
	if err != nil {
		return err
	}
	return writeAdminPolicy(stdout, policy, jsonOutput)
}

func executeAdmins(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "list" {
		args = []string{"get"}
	}
	return executeAdminPolicy(ctx, clients, jsonOutput, args, stdout)
}

func executeComputerGrants(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return usageError("usage: wefty services grants COMPUTER")
	}
	computerID, err := resolveAdminComputerID(ctx, clients, args[0])
	if err != nil {
		return err
	}
	grants, err := clients.listComputerGrants(ctx, computerID)
	if err != nil {
		return err
	}
	return writeComputerGrantList(stdout, grants, jsonOutput)
}

func executeComputerGrant(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
	revoke bool,
) error {
	args = moveFirstPositionalsToEnd(args, 2)
	flags := flag.NewFlagSet("services grant", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var revision int64
	var permission, fabricID, idempotencyKey string
	var waitForCompletion bool
	var pollInterval, waitTimeout time.Duration
	flags.Int64Var(&revision, "policy-revision", 0, "policy revision observed by services grants")
	flags.StringVar(&permission, "permission", "", "grant permission: view or control")
	flags.StringVar(&fabricID, "fabric-id", "", "opaque issuing Fabric ID (defaults to the current Fabric)")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable Computer grant mutation key")
	flags.BoolVar(&waitForCompletion, "wait", false, "wait until the hosting node closes affected sessions and installs the revocation")
	flags.DurationVar(&pollInterval, "poll-interval", time.Second, "revocation status polling interval")
	flags.DurationVar(&waitTimeout, "wait-timeout", 2*time.Minute, "maximum time to observe revocation installation")
	if err := flags.Parse(args); err != nil {
		return usageError(err.Error())
	}
	seenRevision := false
	flags.Visit(func(visited *flag.Flag) { seenRevision = seenRevision || visited.Name == "policy-revision" })
	if flags.NArg() != 2 || strings.TrimSpace(flags.Arg(0)) == "" || strings.TrimSpace(flags.Arg(1)) == "" ||
		!seenRevision || revision < 1 {
		return usageError("usage: wefty services grant COMPUTER USER_ID --permission view|control --policy-revision REVISION [--idempotency-key KEY]")
	}
	if revoke {
		if permission != "" {
			return usageError("services revoke does not accept --permission; revocation records durable none")
		}
		permission = string(l1.ComputerGrantNone)
	} else if permission != string(l1.ComputerGrantView) && permission != string(l1.ComputerGrantControl) {
		return usageError("services grant requires --permission view or --permission control")
	}
	if !revoke && waitForCompletion {
		return usageError("services grant does not accept --wait")
	}
	if pollInterval <= 0 || waitTimeout <= 0 {
		return usageError("--poll-interval and --wait-timeout must be positive")
	}
	var err error
	idempotencyKey, err = ensureIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	computerID, err := resolveAdminComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	result, err := clients.mutateComputerGrant(ctx, computerID, flags.Arg(1), l1.ComputerGrantMutationRequest{
		PolicyRevision: revision,
		FabricID:       fabricID,
		Permission:     l1.ComputerGrantPermission(permission),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if waitForCompletion && result.Revocation != nil {
		result.ObservationState = "pending"
		result.LastObservedRevocation = result.Revocation
		waited := time.Duration(0)
		for result.Revocation.State != l1.ComputerPolicyRevocationCompleted {
			if waited >= waitTimeout {
				return computerGrantObservationError(contract.ErrorRevocationWaitTimeout,
					computerGrantObservationMessage(result, "observation timed out"), result)
			}
			delay := min(pollInterval, waitTimeout-waited)
			if err := clients.wait(ctx, delay); err != nil {
				result.ObservationState = "failed"
				result.ObservationFailure = err.Error()
				return computerGrantObservationError(contract.ErrorRevocationObservationFailed,
					computerGrantObservationMessage(result, "observation failed"), result)
			}
			waited += delay
			revocation, err := clients.getComputerPolicyRevocation(ctx, result.Revocation.ComputerID,
				result.Revocation.SubjectFabricID, result.Revocation.SubjectUserID, result.Revocation.PolicyRevision)
			if err != nil {
				result.ObservationState = "failed"
				result.ObservationFailure = err.Error()
				return computerGrantObservationError(contract.ErrorRevocationObservationFailed,
					computerGrantObservationMessage(result, "observation failed"), result)
			}
			result.Revocation = &revocation
			result.LastObservedRevocation = &revocation
		}
		result.ObservationState = "completed"
	}
	return writeComputerGrantMutation(stdout, result, jsonOutput)
}

func executeComputerTakeover(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	tailAudit := len(args) > 1 && args[0] == "audit" && args[1] == "tail"
	if len(args) > 0 && (args[0] == "view" || args[0] == "take" || args[0] == "release") {
		return executeComputerTakeoverAction(ctx, clients, jsonOutput, args[0], args[1:], stdout, stderr)
	}
	if len(args) > 1 && args[0] == "sessions" && args[1] == "list" {
		args = append([]string{"sessions"}, args[2:]...)
	}
	if len(args) > 1 && args[0] == "audit" && args[1] == "tail" {
		args = append([]string{"audit"}, args[2:]...)
	}
	if len(args) > 0 && args[0] == "sessions" {
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			return usageError("usage: wefty services takeover sessions COMPUTER")
		}
		computerID, err := resolveAdminComputerID(ctx, clients, args[1])
		if err != nil {
			return err
		}
		sessions, err := clients.listComputerTakeoverSessions(ctx, computerID)
		if err != nil {
			return err
		}
		return writeComputerTakeoverSessions(stdout, sessions, jsonOutput)
	}
	if len(args) > 0 && args[0] == "audit" {
		flags := flag.NewFlagSet("services takeover audit", flag.ContinueOnError)
		flags.SetOutput(stderr)
		var cursor string
		var limit int
		flags.StringVar(&cursor, "cursor", "", "opaque cursor from the previous page")
		flags.IntVar(&limit, "limit", l1.DefaultJobPageLimit, "take-over events per page")
		if err := flags.Parse(moveFirstPositionalToEnd(args[1:])); err != nil {
			return usageError(err.Error())
		}
		if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" || limit < 1 || limit > l1.MaxJobPageLimit {
			return usageError(fmt.Sprintf("usage: wefty services takeover audit COMPUTER [--limit 1..%d] [--cursor CURSOR]", l1.MaxJobPageLimit))
		}
		if tailAudit && cursor != "" {
			return usageError("takeover audit tail does not accept --cursor")
		}
		computerID, err := resolveAdminComputerID(ctx, clients, flags.Arg(0))
		if err != nil {
			return err
		}
		page, err := clients.listComputerTakeoverAudit(ctx, computerID, cursor, limit, tailAudit)
		if err != nil {
			return err
		}
		return writeComputerTakeoverAudit(stdout, page, jsonOutput)
	}
	return usageError("usage: wefty services takeover view|take|release|sessions|audit COMPUTER")
}

type computerTakeoverViewResult struct {
	FriendlyName string `json:"friendly_name"`
	ConnectHost  string `json:"connect_host"`
	// DisplayEndpoint is a deprecated JSON and table alias for ConnectHost.
	// Compatibility ends on 2026-10-04; it must never contain the full endpoint.
	DisplayEndpoint  string `json:"display_endpoint"`
	ComputerID       string `json:"computer_id"`
	Action           string `json:"action"`
	SessionTokenFile string `json:"session_token_file"`
}

type takeoverSessionCapability struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

func executeComputerTakeoverAction(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	action string,
	args []string,
	stdout, stderr io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services takeover "+action, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var tokenFile string
	flags.StringVar(&tokenFile, "session-token-file", "", "owner-readable file containing the capability issued by this live view session")
	if err := flags.Parse(args); err != nil {
		return usageError(err.Error())
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" {
		return usageError("usage: wefty services takeover " + action + " COMPUTER" + takeoverTokenFileUsage(action))
	}
	if strings.TrimSpace(tokenFile) == "" {
		return usageError("takeover " + action + " requires --session-token-file from the live view session")
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	if action == "view" {
		availability, err := clients.getComputerTakeoverAvailability(ctx, computerID)
		if err != nil {
			return err
		}
		if availability.DisplayEndpoint == nil {
			return &takeoverActionError{APIError: contract.APIError{Code: contract.ErrorPassUnavailable,
				Message: "Computer display is not ready; retry after services status reports a display endpoint", Retryable: true}}
		}
		session, err := openTakeoverViewWithPolicyRetry(ctx, clients.fabric, *availability.DisplayEndpoint, availability.PolicyRevision)
		if err != nil {
			return err
		}
		defer session.Close()
		if err := writeTakeoverSessionCapability(tokenFile, takeoverSessionCapability{Endpoint: session.Endpoint, Token: session.Token}); err != nil {
			return err
		}
		result := computerTakeoverViewResult{FriendlyName: availability.FriendlyName, ConnectHost: session.ConnectHost,
			DisplayEndpoint: session.ConnectHost, ComputerID: availability.ComputerID, Action: action, SessionTokenFile: tokenFile}
		if err := writeComputerTakeoverView(stdout, result, jsonOutput); err != nil {
			return err
		}
		if err := session.Wait(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}
	capability, err := readTakeoverSessionCapability(tokenFile)
	if err != nil {
		return err
	}
	receipt, err := clients.performComputerTakeoverAction(ctx, capability.Endpoint, capability.Token, action)
	if err != nil {
		return err
	}
	return writeComputerControlReceipt(stdout, receipt, jsonOutput)
}

const (
	// The durable L1 mutation can briefly lead the hosting agent's policy
	// installation. Admission retries only the typed stale-policy refusal and
	// never extends the caller's larger command deadline.
	takeoverPolicyInstallRetryWindow   = 2 * time.Second
	takeoverPolicyInstallRetryInterval = 100 * time.Millisecond
)

type takeoverViewOpen func(context.Context) (*takeover.Session, error)

func openTakeoverViewWithPolicyRetry(ctx context.Context, participant fabric.Fabric, endpoint string, policyRevision int64) (*takeover.Session, error) {
	return retryTakeoverViewPolicyInstallation(ctx, takeoverPolicyInstallRetryWindow, takeoverPolicyInstallRetryInterval,
		func(openContext context.Context) (*takeover.Session, error) {
			return takeover.OpenAtPolicyRevision(openContext, participant, endpoint, policyRevision)
		})
}

func retryTakeoverViewPolicyInstallation(ctx context.Context, window, interval time.Duration, open takeoverViewOpen) (*takeover.Session, error) {
	retryContext, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	for {
		session, err := open(retryContext)
		if err == nil {
			return session, nil
		}
		var actionErr *takeover.ActionError
		if !errors.As(err, &actionErr) || actionErr.APIError.Code != contract.ErrorStalePolicyRevision || !actionErr.APIError.Retryable {
			return nil, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-retryContext.Done():
			timer.Stop()
			return nil, err
		case <-timer.C:
		}
	}
}

func readTakeoverSessionCapability(path string) (takeoverSessionCapability, error) {
	file, err := os.Open(path)
	if err != nil {
		return takeoverSessionCapability{}, fmt.Errorf("open live take-over session token file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return takeoverSessionCapability{}, fmt.Errorf("inspect live take-over session token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return takeoverSessionCapability{}, usageError("session token file must be a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return takeoverSessionCapability{}, usageError("session token file must not be readable or writable by group or others")
	}
	tokenBytes, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return takeoverSessionCapability{}, fmt.Errorf("read live take-over session token file: %w", err)
	}
	var capability takeoverSessionCapability
	if len(tokenBytes) > 4096 || json.Unmarshal(tokenBytes, &capability) != nil ||
		strings.TrimSpace(capability.Endpoint) == "" || strings.TrimSpace(capability.Token) == "" {
		return takeoverSessionCapability{}, usageError("session token file must contain one valid live-session capability")
	}
	return capability, nil
}

func writeTakeoverSessionCapability(path string, capability takeoverSessionCapability) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".wefty-takeover-*")
	if err != nil {
		return fmt.Errorf("create live take-over session token file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if err := json.NewEncoder(temporary).Encode(capability); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("install live take-over session token file: %w", err)
	}
	return nil
}

func takeoverTokenFileUsage(action string) string {
	return " --session-token-file FILE"
}

func writeComputerTakeoverView(writer io.Writer, result computerTakeoverViewResult, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, result)
	}
	_, err := fmt.Fprintf(writer, "FRIENDLY NAME\t%s\nCONNECT HOST\t%s\nDISPLAY ENDPOINT\t%s\nCOMPUTER ID\t%s\nACTION\t%s\nSESSION TOKEN FILE\t%s\n",
		result.FriendlyName, result.ConnectHost, result.DisplayEndpoint, result.ComputerID, result.Action, result.SessionTokenFile)
	return err
}

func writeComputerControlReceipt(writer io.Writer, receipt contract.ComputerControlReceipt, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, receipt)
	}
	_, err := fmt.Fprintf(writer, "COMPUTER ID\t%s\nACTION\t%s\nHOLDER SESSION ID\t%s\nADMITTED MODE\t%s\nTENURE STATE\t%s\nPOLICY REVISION\t%d\nOVERRIDE DISPLACED SESSION ID\t%s\nHUMAN DRIVING\t%t\nSIGNAL STAYED TRUE\t%t\n",
		receipt.ComputerID, receipt.Action, valueOrNA(receipt.HolderSessionID), valueOrNA(receipt.AdmittedMode), receipt.TenureState,
		receipt.PolicyRevision, valueOrNA(receipt.OverrideDisplacedSessionID), receipt.HumanDriving, receipt.SignalStayedTrue)
	if err != nil || receipt.SessionEndReason == "" {
		return err
	}
	_, err = fmt.Fprintf(writer, "SESSION END REASON\t%s\n", receipt.SessionEndReason)
	return err
}

func moveFirstPositionalsToEnd(args []string, count int) []string {
	if count <= 0 || len(args) == 0 {
		return args
	}
	positionals := make([]string, 0, count)
	rest := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		if len(positionals) < count && !strings.HasPrefix(args[index], "-") {
			positionals = append(positionals, args[index])
			continue
		}
		rest = append(rest, args[index])
		boolFlag := args[index] == "--wait" || args[index] == "-wait"
		if strings.HasPrefix(args[index], "-") && !strings.Contains(args[index], "=") && !boolFlag && index+1 < len(args) {
			rest = append(rest, args[index+1])
			index++
		}
	}
	return append(rest, positionals...)
}

func writeAdminPolicy(writer io.Writer, policy l1.AdminPolicy, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, policy)
	}
	if _, err := fmt.Fprintf(writer, "POLICY REVISION\t%d\n", policy.Revision); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "FABRIC ID\tUSER ID\tADDED REVISION\tADDED"); err != nil {
		return err
	}
	for _, admin := range policy.Admins {
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%d\t%s\n", admin.FabricID, admin.UserID,
			admin.AddedRevision, admin.AddedAt.Format("2006-01-02T15:04:05Z07:00")); err != nil {
			return err
		}
	}
	if len(policy.Revocations) > 0 {
		if _, err := fmt.Fprintln(writer, "REVOCATION REVISION\tCOMPUTER ID\tFABRIC ID\tUSER ID\tPERMISSION\tSTATE\tCREATED"); err != nil {
			return err
		}
		for _, revocation := range policy.Revocations {
			if _, err := fmt.Fprintf(writer, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", revocation.PolicyRevision,
				revocation.ComputerID, revocation.SubjectFabricID, revocation.SubjectUserID,
				revocation.TargetPermission, revocation.State, revocation.CreatedAt.Format(time.RFC3339)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeComputerGrantList(writer io.Writer, grants l1.ComputerGrantList, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, grants)
	}
	if _, err := fmt.Fprintf(writer, "POLICY REVISION\t%d\n", grants.PolicyRevision); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "FABRIC ID\tUSER ID\tPERMISSION\tUPDATED REVISION\tUPDATED"); err != nil {
		return err
	}
	for _, grant := range grants.Grants {
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%d\t%s\n", grant.FabricID, grant.UserID,
			grant.Permission, grant.PolicyRevision, grant.UpdatedAt.Format("2006-01-02T15:04:05Z07:00")); err != nil {
			return err
		}
	}
	return nil
}

func writeComputerGrantMutation(writer io.Writer, result l1.ComputerGrantMutationResult, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, result)
	}
	if _, err := fmt.Fprintf(writer, "POLICY REVISION\t%d\n", result.Grant.PolicyRevision); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "FABRIC ID\tUSER ID\tPERMISSION\tUPDATED REVISION\tUPDATED\tREPLAYED\tMUTATION APPLIED\tREVOCATION\n%s\t%s\t%s\t%d\t%s\t%t\t%t\t",
		result.Grant.FabricID, result.Grant.UserID, result.Grant.Permission, result.Grant.PolicyRevision,
		result.Grant.UpdatedAt.Format(time.RFC3339), result.Replayed, result.MutationApplied); err != nil {
		return err
	}
	if result.Revocation == nil {
		_, err := fmt.Fprintln(writer, "N/A")
		return err
	}
	_, err := fmt.Fprintf(writer, "%s:%s:%s:%s:%d\n", result.Revocation.ComputerID,
		result.Revocation.SubjectFabricID, result.Revocation.SubjectUserID, result.Revocation.State,
		result.Revocation.PolicyRevision)
	return err
}

func writeComputerTakeoverSessions(writer io.Writer, sessions l1.ComputerTakeoverSessionList, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, sessions)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(table, "CONTROLLER SESSION ID\t%s\n", valueOrNA(sessions.ControllerSessionID)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tJOB ID\tATTEMPT ID\tSESSION ID\tFABRIC ID\tUSER ID\tDEVICE ID\tAUTHORIZED ROLE\tOBSERVED MODE\tPOLICY REVISION\tOPENED\tEVIDENCE STATE"); err != nil {
		return err
	}
	for _, session := range sessions.Sessions {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			session.ComputerID, session.JobID, session.AttemptID, session.SessionID, session.FabricID,
			session.UserID, session.DeviceID, session.AuthorizedRole, session.AdmittedMode,
			session.PolicyRevision, session.OpenedAt.Format(time.RFC3339), session.EvidenceState); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeComputerTakeoverAudit(writer io.Writer, page l1.ComputerTakeoverAuditList, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, page)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "OCCURRED\tEVENT ID\tKIND\tCOMPUTER ID\tJOB ID\tATTEMPT ID\tSESSION ID\tFABRIC ID\tUSER ID\tDEVICE ID\tAUTHORIZED ROLE\tMODE\tPOLICY REVISION\tAUTHORITY GENERATION\tREASON\tCOUNT"); err != nil {
		return err
	}
	for _, event := range page.Events {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%d\n",
			event.OccurredAt.Format(time.RFC3339), event.EventID, event.Kind, event.ComputerID, event.JobID,
			event.AttemptID, valueOrNA(event.SessionID), valueOrNA(event.FabricID), valueOrNA(event.UserID),
			valueOrNA(event.DeviceID), valueOrNA(string(event.AuthorizedRole)), valueOrNA(string(event.AdmittedMode)),
			event.PolicyRevision, event.AuthorityGeneration, valueOrNA(string(event.Reason)), event.EventCount); err != nil {
			return err
		}
	}
	if page.NextCursor != "" {
		if _, err := fmt.Fprintf(table, "NEXT CURSOR\t%s\n", page.NextCursor); err != nil {
			return err
		}
	}
	return table.Flush()
}
