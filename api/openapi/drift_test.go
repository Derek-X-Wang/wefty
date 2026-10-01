package openapi_test

import (
	"encoding"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The drift test pins the Go wire types to the published schemas (#601).
// Hand-written checks elsewhere in this package pin particular promises; this
// one is mechanical. Every row below names a schema location and the Go type
// the server writes for it (a response) or decodes it into (a request). For
// each pair, recursively through nested objects, arrays and maps:
//
//   - a closed schema (additionalProperties:false) declares every field the Go
//     type carries, so nothing the server emits or accepts is forbidden;
//   - every field the schema requires is a Go field that encoding/json always
//     writes (no omitempty or omitzero that can drop it);
//   - every field the schema declares is a Go field, so the schema promises
//     nothing the server never sends and accepts nothing it would refuse;
//   - where the Go field has a closed vocabulary (a named string type with
//     constants), the schema publishes an enum and it is exactly that
//     vocabulary.
//
// References are followed the way a validator follows them, $dynamicRef
// included; a reference the walk cannot follow fails the test instead of
// ending the comparison quietly. Repeated property, item and map-value
// declarations retain the composition semantics that made each one apply.
//
// Every object schema a protocol file publishes must be reached by a row, by
// recursion from a row, or be listed in driftUnmapped with the reason it is
// not pinned. A new schema therefore arrives with a Go type or an explanation.
// Deliberate differences are listed in the allow tables with their reasons.

type driftRow struct {
	schema string
	goType reflect.Type
}

func typeOf[T any]() reflect.Type { return reflect.TypeFor[T]() }

func component(document, name string) string {
	return document + "#/components/schemas/" + name
}

func requestBody(document, method, route string) string {
	return document + "#/paths/" + escapePointer(route) + "/" + method + "/requestBody/content/application~1json/schema"
}

func responseBody(document, method, route, status string) string {
	return document + "#/paths/" + escapePointer(route) + "/" + method + "/responses/" + status + "/content/application~1json/schema"
}

func escapePointer(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

const (
	commonDoc = "common.v1.json"
	agentDoc  = "l1-agent.v1.json"
	clientDoc = "l1-client.v1.json"
	l3Doc     = "l3.v1.json"
)

var driftRows = []driftRow{
	// Shared components.
	{component(commonDoc, "ComputerTokenRevocationReceipt"), typeOf[contract.ComputerTokenRevocationReceipt]()},
	{component(commonDoc, "ErrorResponse"), typeOf[contract.ErrorResponse]()},
	{component(commonDoc, "Error"), typeOf[contract.APIError]()},
	{component(commonDoc, "Job"), typeOf[l1.Job]()},
	{component(commonDoc, "AdminPolicy"), typeOf[l1.AdminPolicy]()},
	{component(commonDoc, "AdminPolicyAudit"), typeOf[l1.AdminPolicyAudit]()},
	{component(commonDoc, "AdminPolicyAuditList"), typeOf[l1.AdminPolicyAuditList]()},
	{component(commonDoc, "AuthenticatedPerson"), typeOf[l1.AuthenticatedPerson]()},
	{component(commonDoc, "ComputerGrant"), typeOf[l1.ComputerGrant]()},
	{component(commonDoc, "ComputerGrantList"), typeOf[l1.ComputerGrantList]()},
	{component(commonDoc, "ComputerPolicyRevocation"), typeOf[l1.ComputerPolicyRevocation]()},
	{component(commonDoc, "ComputerGrantMutationResult"), typeOf[l1.ComputerGrantMutationResult]()},
	{component(commonDoc, "ComputerPolicyAudit"), typeOf[l1.ComputerPolicyAudit]()},
	{component(commonDoc, "ComputerPolicyAuditList"), typeOf[l1.ComputerPolicyAuditList]()},
	{component(commonDoc, "ComputerPolicySnapshot"), typeOf[l1.ComputerPolicySnapshot]()},
	{component(commonDoc, "ComputerPolicyInstallAcknowledgement"), typeOf[l1.ComputerPolicyInstallAcknowledgement]()},
	{component(commonDoc, "ComputerTakeoverAuditEvent"), typeOf[l1.ComputerTakeoverAuditEvent]()},
	{component(commonDoc, "ComputerTakeoverAuditReceipt"), typeOf[l1.ComputerTakeoverAuditReceipt]()},
	{component(commonDoc, "ComputerTakeoverAuditList"), typeOf[l1.ComputerTakeoverAuditList]()},
	{component(commonDoc, "ComputerTakeoverSession"), typeOf[l1.ComputerTakeoverSession]()},
	{component(commonDoc, "ComputerTakeoverSessionList"), typeOf[l1.ComputerTakeoverSessionList]()},
	{component(commonDoc, "ComputerTakeoverAccess"), typeOf[l1.ComputerTakeoverAvailability]()},
	{component(commonDoc, "ComputerHandleResolution"), typeOf[l1.ComputerHandleResolution]()},
	{component(commonDoc, "ComputerIntent"), typeOf[l1.ComputerIntent]()},
	{component(commonDoc, "ComputerIntentList"), typeOf[l1.ComputerIntentList]()},
	{component(commonDoc, "Computer"), typeOf[l1.Computer]()},
	{component(commonDoc, "ComputerList"), typeOf[l1.ComputerList]()},
	{component(commonDoc, "ComputerStorageGeneration"), typeOf[l1.ComputerStorageGeneration]()},
	{component(commonDoc, "ComputerStorageGenerationList"), typeOf[l1.ComputerStorageGenerationList]()},
	{component(commonDoc, "StorageProvenance"), typeOf[l1.StorageProvenance]()},
	{component(commonDoc, "ComputerCustodyBranch"), typeOf[l1.ComputerCustodyBranch]()},
	{component(commonDoc, "ComputerStorageProvenance"), typeOf[l1.ComputerStorageProvenance]()},
	{component(commonDoc, "ComputerStoragePreparationOutcome"), typeOf[l1.ComputerStoragePreparationOutcome]()},
	{component(commonDoc, "ComputerCustodyImportObservation"), typeOf[l1.ComputerCustodyImportObservation]()},
	{component(commonDoc, "ComputerCustodyImport"), typeOf[l1.ComputerCustodyImport]()},
	{component(commonDoc, "BackupCopy"), typeOf[l1.BackupCopy]()},
	{component(commonDoc, "Backup"), typeOf[l1.Backup]()},
	{component(commonDoc, "BackupList"), typeOf[l1.BackupList]()},
	{component(commonDoc, "ComputerBackupOperationOutcome"), typeOf[l1.ComputerBackupOperationOutcome]()},
	{component(commonDoc, "ComputerStorageGrowOutcome"), typeOf[l1.ComputerStorageGrowOutcome]()},
	{component(commonDoc, "ComputerRestoreOperation"), typeOf[l1.ComputerRestoreOperation]()},
	{component(commonDoc, "ComputerCloneOperation"), typeOf[l1.ComputerCloneOperation]()},
	{component(commonDoc, "ServiceRemovalStall"), typeOf[l1.ServiceRemovalStall]()},
	{component(commonDoc, "ServiceRemovalStallEvidence"), typeOf[l1.ServiceRemovalStallEvidence]()},
	{component(commonDoc, "ComputerStorageCleanupQuarantine"), typeOf[l1.ComputerStorageCleanupQuarantine]()},
	{component(commonDoc, "OwedComputerRevocation"), typeOf[l1.OwedComputerRevocation]()},
	{component(commonDoc, "ComputerRestoreRevocationReceipt"), typeOf[l1.ComputerRestoreRevocationReceipt]()},
	{component(commonDoc, "ComputerBackupDirective"), typeOf[l1.ComputerBackupDirective]()},
	{component(commonDoc, "ComputerBackupPruneDirective"), typeOf[l1.ComputerBackupPruneDirective]()},
	{component(commonDoc, "ComputerStorageCopyDirective"), typeOf[l1.ComputerStorageCopyDirective]()},
	{component(commonDoc, "ComputerCustodyExportDirective"), typeOf[l1.ComputerCustodyExportDirective]()},
	{component(commonDoc, "Attempt"), typeOf[l1.Attempt]()},
	{component(commonDoc, "LateResultEvidence"), typeOf[l1.LateResultEvidence]()},
	{component(commonDoc, "AttemptLease"), typeOf[l1.AttemptLease]()},
	{component(commonDoc, "LogGap"), typeOf[contract.LogGap]()},
	{component(commonDoc, "LogEvent"), typeOf[contract.LogEvent]()},
	{component(commonDoc, "LogTruncation"), typeOf[l1.LogTruncation]()},
	{component(commonDoc, "ServiceLogTruncation"), typeOf[l1.ServiceLogTruncation]()},
	{component(commonDoc, "ProcessResult"), typeOf[l1.ProcessResult]()},
	{component(commonDoc, "SpawnFailure"), typeOf[contract.SpawnFailure]()},
	{component(commonDoc, "RuntimeFailure"), typeOf[contract.RuntimeFailure]()},
	{component(commonDoc, "OCIImageEvidence"), typeOf[l1.OCIImageEvidence]()},
	{component(commonDoc, "LogPage"), typeOf[l1.LogPage]()},
	{component(commonDoc, "AttemptResultRequest"), typeOf[l1.AttemptResultRequest]()},
	{component(commonDoc, "AttemptResultResponse"), typeOf[l1.AttemptResultResponse]()},
	{component(commonDoc, "JobResult"), typeOf[l1.JobResult]()},
	{component(commonDoc, "RunSummary"), typeOf[l3.RunSummary]()},
	{component(commonDoc, "RunListPage"), typeOf[l3.RunListPage]()},
	{component(commonDoc, "RunResult"), typeOf[l3.RunResult]()},
	{component(commonDoc, "NodeRegistration"), typeOf[contract.NodeRegistration]()},
	{component(commonDoc, "ComputerStorageClaim"), typeOf[l1.ComputerStorageClaim]()},
	{component(commonDoc, "RemovalDirective"), typeOf[l1.RemovalDirective]()},
	{component(commonDoc, "ComputerStorageResetDirective"), typeOf[l1.ComputerStorageResetDirective]()},
	{component(commonDoc, "ComputerStorageGrowDirective"), typeOf[l1.ComputerStorageGrowDirective]()},
	{component(commonDoc, "ComputerReimagePreflightDirective"), typeOf[l1.ComputerReimagePreflightDirective]()},
	{component(commonDoc, "Node"), typeOf[l1.Node]()},
	{component(commonDoc, "HeartbeatResponse"), typeOf[l1.HeartbeatResponse]()},
	{component(commonDoc, "JobSpec"), typeOf[contract.JobSpec]()},
	{component(commonDoc, "JobRecordSpec"), typeOf[contract.JobSpec]()},
	{component(commonDoc, "RunRecord"), typeOf[contract.RunRecord]()},
	{component(commonDoc, "Envelope"), typeOf[contract.Envelope]()},
	{component(commonDoc, "GateResult"), typeOf[contract.GateResult]()},

	// L3 components.
	{component(l3Doc, "WorkflowVersionInput"), typeOf[l3.WorkflowVersionInput]()},
	{component(l3Doc, "ImageProgram"), typeOf[contract.ImageProgram]()},
	{component(l3Doc, "ComputerRunPage"), typeOf[l3.ComputerRunPage]()},
	{component(l3Doc, "RunAccepted"), typeOf[l3.RunAccepted]()},
	{component(l3Doc, "ComputerTokenMintRequest"), typeOf[l3.ComputerTokenMintRequest]()},
	{component(l3Doc, "ComputerTokenGrant"), typeOf[l3.ComputerTokenGrant]()},
	{component(l3Doc, "ComputerTokenRevocationRequest"), typeOf[l3.ComputerTokenRevocationRequest]()},
	{component(l3Doc, "ComputerInflightState"), typeOf[l3.ComputerInflightState]()},
	{component(l3Doc, "ComputerAttemptTokenRevocationRequest"), typeOf[l3.ComputerAttemptTokenRevocationRequest]()},
	{component(l3Doc, "HostComputerTokenRevocationRequest"), typeOf[l3.HostComputerTokenRevocationRequest]()},
	{component(l3Doc, "ComputerSelf"), typeOf[l3.ComputerSelf]()},
	{component(l3Doc, "RunExecution"), typeOf[l3.RunExecution]()},
	{component(l3Doc, "LineageEntry"), typeOf[l3.LineageEntry]()},
	{component(l3Doc, "RunLineage"), typeOf[l3.RunLineage]()},
	{requestBody(l3Doc, "post", "/v1/runs"), typeOf[l3.CreateRunRequest]()},

	// L1 client inline bodies.
	{responseBody(clientDoc, "get", "/v1/computers/{computer_id}/submission", "200"), typeOf[l1.ComputerSubmissionState]()},
	{requestBody(clientDoc, "put", "/v1/computers/{computer_id}/submission"), typeOf[l1.ComputerSubmissionRequest]()},
	{responseBody(clientDoc, "put", "/v1/computers/{computer_id}/submission", "200"), typeOf[l1.ComputerSubmissionMutationResult]()},
	{responseBody(clientDoc, "post", "/v1/computers/{computer_id}/token-scope-proof", "200"), typeOf[l1.ComputerTokenScopeProof]()},
	{responseBody(clientDoc, "get", "/v1/jobs", "200"), typeOf[l1.JobList]()},
	{responseBody(clientDoc, "get", "/v1/jobs/{job_id}/children", "200"), typeOf[l1.JobList]()},
	{requestBody(clientDoc, "put", "/v1/jobs/{job_id}/desired-state"), typeOf[l1.ServiceDesiredStateRequest]()},
	{requestBody(clientDoc, "post", "/v1/jobs/{job_id}/restart"), typeOf[l1.ServiceRestartRequest]()},
	{requestBody(clientDoc, "post", "/v1/jobs/{job_id}/forget"), typeOf[l1.ForceForgetRequest]()},
	{requestBody(clientDoc, "post", "/v1/admin-bootstrap"), typeOf[l1.BootstrapAdminRequest]()},
	{requestBody(clientDoc, "put", "/v1/admin-policy/admins/{user_id}"), typeOf[l1.AdminPolicyMutationRequest]()},
	{requestBody(clientDoc, "delete", "/v1/admin-policy/admins/{user_id}"), typeOf[l1.AdminPolicyMutationRequest]()},
	{requestBody(clientDoc, "put", "/v1/computers/{computer_id}/grants/{user_id}"), typeOf[l1.ComputerGrantMutationRequest]()},
	{requestBody(clientDoc, "delete", "/v1/computers/{computer_id}/grants/{user_id}"), typeOf[l1.ComputerGrantDeleteRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers"), typeOf[l1.CreateComputerRequest]()},
	{requestBody(clientDoc, "put", "/v1/computers/{computer_id}/desired-state"), typeOf[l1.ComputerDesiredStateRequest]()},
	{requestBody(clientDoc, "put", "/v1/computers/{computer_id}/backup-cap"), typeOf[l1.ComputerBackupCapRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/restart"), typeOf[l1.ComputerRestartRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/reimage"), typeOf[l1.ComputerReimageRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/grow"), typeOf[l1.ComputerGrowRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/reconfiguration-abort"), typeOf[l1.ComputerReconfigurationAbortRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/storage-reset"), typeOf[l1.ComputerStorageResetRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/backups"), typeOf[l1.ComputerBackupCreateRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/backups/{backup_id}/prune"), typeOf[l1.ComputerBackupPruneRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/backups/{backup_id}/restore"), typeOf[l1.ComputerRestoreRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/backups/{backup_id}/clone"), typeOf[l1.ComputerCloneRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/backups/{backup_id}/export"), typeOf[l1.ComputerCustodyExportRequest]()},
	{requestBody(clientDoc, "post", "/v1/custody-exports/{export_id}/attest-deleted"), typeOf[l1.ComputerCustodyAttestationRequest]()},
	{requestBody(clientDoc, "post", "/v1/custody-exports/{export_id}/import"), typeOf[l1.ComputerCustodyImportRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/projections"), typeOf[l1.ComputerProjectionRequest]()},
	{requestBody(clientDoc, "post", "/v1/computers/{computer_id}/remove"), typeOf[l1.ComputerRemoveRequest]()},
	{responseBody(clientDoc, "get", "/v1/nodes", "200"), typeOf[l1.NodeList]()},
	{requestBody(clientDoc, "post", "/v1/nodes/{node_id}/drain"), typeOf[l1.NodeIntentRequest]()},
	{requestBody(clientDoc, "post", "/v1/nodes/{node_id}/claims"), typeOf[l1.NodeIntentRequest]()},

	// L1 agent inline bodies.
	{requestBody(agentDoc, "post", "/v1/agent/nodes/{node_id}/heartbeat"), typeOf[l1.HeartbeatRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/nodes/{node_id}/drain"), typeOf[l1.DrainRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/claim"), typeOf[l1.ClaimRequest]()},
	{responseBody(agentDoc, "post", "/v1/agent/jobs/claim", "200"), typeOf[l1.Claim]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/service-binding-proof"), typeOf[l1.ServiceBindingProofRequest]()},
	{responseBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/service-binding-proof", "200"), typeOf[l1.ServiceBindingProofResponse]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/image-reconciliation-failure"), typeOf[l1.ServiceImageReconciliationFailureRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/lease"), typeOf[l1.RenewalRequest]()},
	{requestBody(agentDoc, "put", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/publication"), typeOf[l1.PublicationRequest]()},
	{requestBody(agentDoc, "put", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/image"), typeOf[l1.ImageObservationRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/started"), typeOf[l1.StartedRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs"), typeOf[l1.AppendLogsRequest]()},
	{responseBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs", "200"), typeOf[l1.AppendLogsResponse]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete"), typeOf[l1.CompletionRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/jobs/{job_id}/removal-acknowledgement"), typeOf[l1.RemovalAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/jobs/{job_id}/attempts/{attempt_id}/takeover-audit"), typeOf[l1.ComputerTakeoverAuditRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/backup-acknowledgement"), typeOf[l1.ComputerBackupAcknowledgementRequest]()},
	{responseBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/backup-acknowledgement", "200"), typeOf[l1.ComputerBackupAcknowledgementResponse]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/backup-prune-acknowledgement"), typeOf[l1.ComputerBackupPruneAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/storage-copy-acknowledgement"), typeOf[l1.ComputerStorageCopyAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/custody-export-acknowledgement"), typeOf[l1.ComputerCustodyExportAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/restore-retirement-acknowledgement"), typeOf[l1.RemovalAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/storage-grow-acknowledgement"), typeOf[l1.ComputerStorageGrowAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/reimage-preflight-acknowledgement"), typeOf[l1.ComputerReimagePreflightAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/storage-reset-acknowledgement"), typeOf[l1.ComputerStorageResetAcknowledgementRequest]()},
	{requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/storage-retirement-acknowledgement"), typeOf[l1.RemovalAcknowledgementRequest]()},
}

// driftUnmapped lists the object schemas no row pins, each with the reason.
// Keys are schema locations as the rows spell them.
var driftUnmapped = map[string]string{
	component(l3Doc, "WorkflowVersion"): "l3.WorkflowVersion has a custom MarshalJSON that writes one of two " +
		"discriminated shapes; reflection over its struct tags does not describe the wire",
	component(l3Doc, "EnvelopeWrite"): "L3 decodes envelope writes as raw JSON and validates them against the " +
		"contract envelope schema; there is no Go request type",
	component(l3Doc, "GateResultWrite"): "L3 decodes gate writes as raw JSON and validates them against the " +
		"contract gate-result schema; there is no Go request type",
	responseBody(l3Doc, "get", "/v1/runs", "200"): "a union of RunListPage and ComputerRunPage, each pinned as its own component",
	requestBody(clientDoc, "post", "/v1/computers/{computer_id}/token-scope-proof"): "the handler decodes an " +
		"anonymous struct, which reflection cannot name from outside l1",
	requestBody(clientDoc, "post", "/v1/jobs/{job_id}/prompt"): "reserved route; the server answers 501 without decoding",
}

// driftRequiredOmittable lists schema-required fields whose Go field may be
// dropped by omitempty, with the reason the field is present wherever the
// schema requires it. Keys are "<go type>.<json name>".
var driftRequiredOmittable = map[string]string{
	"l1.ComputerStoragePreparationOutcome.recorded_at": "L1 refuses a preparation outcome without recorded_at " +
		"(validateStorageCopyPreparationOutcome) and projects only outcomes it accepted",
	"l1.ComputerCustodyImportRequest.node_id": "L1 refuses a Custody import without node_id, so the required " +
		"schema is exactly the accepted request",
	"contract.OCIImageSpec.digest": "the one schema that requires it, ComputerReimagePreflightDirective.target_image, " +
		"is always built with the resolved digest (reimage_preflight.go)",
	"contract.Envelope.attempt_id": "omitempty serves client writes that leave the binding to L3; every stored " +
		"envelope L3 returns carries the run token's attempt",
	"contract.GateResult.attempt_id": "omitempty serves client writes that leave the binding to L3; every stored " +
		"gate result L3 returns carries the run token's attempt",
	"l1.LogPage.next_cursor": "L1 always encodes a cursor. KNOWN GAP (#601 follow-up): L3's page for a run with no " +
		"dispatched job echoes the caller's cursor, so a first read omits it",
}

// driftSchemaOnly lists schema properties the Go type does not carry, with the
// reason the schema still declares them. Keys are "<go type>.<json name>".
var driftSchemaOnly = map[string]string{}

// driftGoOnly lists Go fields a closed schema deliberately leaves out, with the
// reason the server never writes (or never honours) them at that location.
// Keys are "<go type>.<json name>@<schema location>".
var driftGoOnly = map[string]string{
	"l1.ComputerGrantMutationResult.observation_state@" + component(commonDoc, "ComputerGrantMutationResult"):        cliOnlyObservation,
	"l1.ComputerGrantMutationResult.observation_failure@" + component(commonDoc, "ComputerGrantMutationResult"):      cliOnlyObservation,
	"l1.ComputerGrantMutationResult.last_observed_revocation@" + component(commonDoc, "ComputerGrantMutationResult"): cliOnlyObservation,
	"l1.RemovalAcknowledgementRequest.cleanup_stall@" +
		requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/storage-retirement-acknowledgement"): retirementHasNoStall,
	"l1.RemovalAcknowledgementRequest.cleanup_stall@" +
		requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/restore-retirement-acknowledgement"): retirementHasNoStall,
}

const (
	cliOnlyObservation = "the CLI annotates its copy of the result with its own revocation wait; L1 never sets " +
		"the field, and omitempty keeps it off the wire"
	retirementHasNoStall = "the Go request type is shared with service removal, where a stall declaration is " +
		"published; the agent never sends one on a Computer Storage retirement, which has no stall outcome"
)

// driftEnumException is a Go closed vocabulary that legitimately differs from
// one schema enum. A closed-subset exception pins the exact subset the route or
// projection admits, and the schema enum must equal it. An open exception
// (subset nil) is a location that deliberately publishes no enum, and it must
// stay that way.
type driftEnumException struct {
	subset []string
	reason string
}

// driftEnumExceptions is keyed by "<go type>@<schema location>".
var driftEnumExceptions = map[string]driftEnumException{
	"contract.ServiceDesiredState@" + component(commonDoc, "Job") + "/properties/removal/properties/desired_state": {
		subset: []string{"removed"}, reason: "a removal projection is always desired_state removed",
	},
	"contract.ServiceDesiredState@" + requestBody(clientDoc, "put", "/v1/jobs/{job_id}/desired-state") + "/properties/desired_state": {
		subset: []string{"running", "stopped"}, reason: "the route refuses removed; removal has its own route",
	},
	"contract.ServiceDesiredState@" + requestBody(clientDoc, "put", "/v1/computers/{computer_id}/desired-state") + "/properties/desired_state": {
		subset: []string{"running", "stopped"}, reason: "the route refuses removed; removal has its own route",
	},
	"l1.ComputerGrantPermission@" + component(commonDoc, "ComputerPolicyRevocation") + "/properties/target_permission": {
		subset: []string{"none", "view"}, reason: "a revocation is recorded only for a downgrade, so its target is never control",
	},
	"l1.ComputerGrantPermission@" + component(commonDoc, "ComputerTakeoverAuditEvent") + "/properties/authorized_role": {
		subset: []string{"view", "control"}, reason: "L1 refuses a takeover audit event whose role is not view or control",
	},
	"l1.ComputerGrantPermission@" + component(commonDoc, "ComputerTakeoverSession") + "/properties/authorized_role": {
		subset: []string{"view", "control"}, reason: "sessions are projected from audit events, whose role is view or control",
	},
	"l1.ComputerStorageGenerationPhase@" + component(commonDoc, "ComputerStorageGeneration") + "/properties/phase": {
		subset: []string{"current", "staging", "retired"},
		reason: "absent names a missing generation row in a refusal's error details; it is never a stored phase",
	},
	"contract.ErrorCode@" + component(commonDoc, "Error") + "/properties/code": {
		reason: "deliberately open: the one error envelope serves L1 and L3 and gains codes with new refusals, " +
			"so a client acts on the codes it knows and on retryable for the rest",
	},
	"contract.SpawnFailureCode@" + component(commonDoc, "SpawnFailure") + "/properties/code": {
		reason: "deliberately open: the schema tells readers that unknown codes default to terminal service policy",
	},
	"contract.RuntimeFailureCode@" + component(commonDoc, "RuntimeFailure") + "/properties/code": {
		reason: "deliberately open: the schema tells readers that unknown codes default to terminal",
	},
}

// driftMustCompare names Go type and schema pairs the walk has to reach. They
// sit behind references a weaker resolver would silently stop at, such as the
// $dynamicRef that lets JobRecordSpec widen JobSpec's executable.
var driftMustCompare = []string{
	"contract.ExecutableSpec@../../contract/schemas/v1/job-spec.schema.json#/$defs/executable",
	"contract.ExecutableSpec@" + component(commonDoc, "JobRecordSpec") + "/$defs/executable",
}

// driftValidatorEnums pins schema enums to validator functions whose single
// switch statement is the accepted set, for wire fields Go keeps as plain strings.
var driftValidatorEnums = []struct {
	source   string
	function string
	schema   string
}{
	{
		source:   "l1",
		function: "validCustodyExportFailureCode",
		schema: requestBody(agentDoc, "post", "/v1/agent/computers/{computer_id}/custody-export-acknowledgement") +
			"/properties/receipt/properties/failure_code",
	},
}

func TestGoWireTypesMatchPublishedSchemas(t *testing.T) {
	t.Parallel()

	set := newSchemaSet(t)
	checker := &driftChecker{
		set:      set,
		consts:   loadGoConstants(t),
		done:     map[string]bool{},
		compared: map[string]bool{},
		seen:     map[string]bool{},
	}
	for _, row := range driftRows {
		if set.location(row.schema).node == nil {
			t.Errorf("drift row %s names no schema", row.schema)
			continue
		}
		checker.compare(row.schema, row.goType, set.root(row.schema))
	}
	for _, problem := range checker.problems {
		t.Error(problem)
	}
	for _, pair := range driftMustCompare {
		if !checker.compared[pair] {
			t.Errorf("the drift walk never compared %s", pair)
		}
	}

	// Every published object schema is pinned by a row, reached from one, or
	// listed with a reason.
	for _, location := range set.publishedObjectSchemas() {
		key := location.key()
		_, unmapped := driftUnmapped[key]
		reached := checker.seen[key]
		switch {
		case reached && unmapped:
			t.Errorf("%s is listed as unmapped but a drift row reaches it; remove it from driftUnmapped", key)
		case !reached && !unmapped:
			t.Errorf("%s is an object schema no drift row pins; map its Go type or list it in driftUnmapped with a reason", key)
		}
	}
	for key := range driftUnmapped {
		if set.location(key).node == nil {
			t.Errorf("driftUnmapped names %s, which no longer exists", key)
		}
	}
	for key := range driftRequiredOmittable {
		if !checker.usedRequiredOmittable[key] {
			t.Errorf("driftRequiredOmittable entry %s is not needed any more", key)
		}
	}
	for key := range driftSchemaOnly {
		if !checker.usedSchemaOnly[key] {
			t.Errorf("driftSchemaOnly entry %s is not needed any more", key)
		}
	}
	for key := range driftGoOnly {
		if !checker.usedGoOnly[key] {
			t.Errorf("driftGoOnly entry %s is not needed any more", key)
		}
	}
	for key := range driftEnumExceptions {
		if !checker.usedEnumExceptions[key] {
			t.Errorf("driftEnumExceptions entry %s is not needed any more", key)
		}
	}
}

func TestValidatorVocabulariesMatchPublishedEnums(t *testing.T) {
	t.Parallel()

	set := newSchemaSet(t)
	constants := loadGoConstants(t)
	for _, entry := range driftValidatorEnums {
		accepted := validatorCases(t, entry.source, entry.function, constants)
		if set.location(entry.schema).node == nil {
			t.Errorf("%s names no schema", entry.schema)
			continue
		}
		published, closed := set.vocabulary(set.root(entry.schema))
		if !closed {
			t.Errorf("%s publishes no enum for %s.%s", entry.schema, entry.source, entry.function)
			continue
		}
		if missing, extra := setDifference(accepted, published); len(missing)+len(extra) > 0 {
			t.Errorf("%s enum differs from %s.%s: accepted but unpublished %v, published but refused %v",
				entry.schema, entry.source, entry.function, missing, extra)
		}
	}
}

// TestComposedSchemasCloseAtTheComposingLevel forbids closure keywords that
// refuse fields a sibling schema adds. additionalProperties (and a member's
// unevaluatedProperties) see only their own schema's properties, so a closed
// allOf member refuses every field its siblings add: Node was
// allOf[NodeRegistration (closed), ...] and no real Node validated (#601). A
// $ref with applicator siblings is allOf[target, siblings] and is held to the
// same rule. A composite closes itself with unevaluatedProperties over open
// arms; additionalProperties:false beside arms is allowed only when the node's
// own properties already declare everything the arms add.
func TestComposedSchemasCloseAtTheComposingLevel(t *testing.T) {
	t.Parallel()

	set := newSchemaSet(t)
	documents := []string{commonDoc, agentDoc, clientDoc, l3Doc}
	for _, name := range []string{"job-spec", "envelope", "gate-result", "run-record"} {
		documents = append(documents, "../../contract/schemas/v1/"+name+".schema.json")
	}
	composites := 0
	for _, document := range documents {
		var walk func(value any, pointer string)
		walk = func(value any, pointer string) {
			switch node := value.(type) {
			case map[string]any:
				checkComposedClosure(t, set, set.root(document+"#"+pointer), &composites)
				for key, child := range node {
					walk(child, pointer+"/"+escapePointer(key))
				}
			case []any:
				for index, child := range node {
					walk(child, pointer+"/"+strconv.Itoa(index))
				}
			}
		}
		walk(set.document(document), "")
	}
	if composites == 0 {
		t.Fatal("found no composition to check")
	}
}

func checkComposedClosure(t *testing.T, set *schemaSet, r schemaRef, composites *int) {
	t.Helper()
	node := r.loc.node
	if pureReference(node) {
		return // a plain alias adds nothing beside its target
	}
	all, alternatives, conditional := set.arms(r)
	arms := len(all)
	if _, hasRef := set.reference(r); hasRef {
		arms++ // the siblings of the $ref are an arm of their own
	}
	if arms > 1 {
		*composites++
		for _, arm := range all {
			if set.shape(arm, nil).closed {
				t.Errorf("%s: arm %s is closed and refuses its siblings' fields; leave arms open and close "+
					"the composite with unevaluatedProperties", r.loc.key(), arm.loc.key())
			}
		}
	}
	if node["additionalProperties"] != false {
		return
	}
	own, _ := node["properties"].(map[string]any)
	for _, group := range alternatives {
		all = append(all, group...)
	}
	all = append(all, conditional...)
	for _, arm := range all {
		for name := range set.shape(arm, nil).properties {
			if _, declared := own[name]; !declared {
				t.Errorf("%s: additionalProperties:false refuses %q, which arm %s declares; close with "+
					"unevaluatedProperties or declare it here", r.loc.key(), name, arm.loc.key())
			}
		}
	}
}

// TestVocabularyFollowsCompositionSemantics pins how the enum reader combines
// arms: allOf narrows, an alternative group widens and is open when any arm is,
// $ref siblings still apply, and a keyword it does not model fails loudly.
func TestVocabularyFollowsCompositionSemantics(t *testing.T) {
	t.Parallel()

	const document = `{"$defs": {
		"ab": {"enum": ["a", "b"]},
		"narrowed": {"allOf": [{"enum": ["a", "b"]}, {"enum": ["b", "c"]}]},
		"openAlternative": {"anyOf": [{"enum": ["a"]}, {"type": "string"}]},
		"closedAlternative": {"oneOf": [{"enum": ["a"]}, {"enum": ["b"]}]},
		"nullable": {"oneOf": [{"type": "null"}, {"enum": ["a"]}]},
		"refWithSibling": {"$ref": "#/$defs/ab", "enum": ["b", "z"]},
		"aliasOnly": {"$ref": "#/$defs/ab", "description": "an annotation does not narrow"},
		"plain": {"type": "string"},
		"nullableOpen": {"oneOf": [{"type": "null"}, {"type": "string"}]},
		"negated": {"type": "string", "not": {"enum": ["a"]}},
		"oneOfShared": {"oneOf": [{"enum": ["a", "b"]}, {"enum": ["b", "c"]}]},
		"oneOfOpen": {"oneOf": [{"enum": ["a"]}, {"type": "string"}]}
	}}`
	var parsed any
	if err := json.Unmarshal([]byte(document), &parsed); err != nil {
		t.Fatal(err)
	}
	type failure struct{ message string }
	set := &schemaSet{docs: map[string]any{"inline.json": parsed}}
	set.fatalf = func(format string, args ...any) { panic(failure{fmt.Sprintf(format, args...)}) }
	read := func(name string) (values []string, closed bool, refused string) {
		defer func() {
			if recovered := recover(); recovered != nil {
				caught, ok := recovered.(failure)
				if !ok {
					panic(recovered)
				}
				refused = caught.message
			}
		}()
		admitted, closed := set.vocabulary(set.root("inline.json#/$defs/" + name))
		return sortedKeys(admitted), closed, ""
	}
	for _, check := range []struct {
		name   string
		values []string
		closed bool
	}{
		{"narrowed", []string{"b"}, true},
		{"openAlternative", []string{}, false},
		{"closedAlternative", []string{"a", "b"}, true},
		{"nullable", []string{"a"}, true},
		{"refWithSibling", []string{"b"}, true},
		{"aliasOnly", []string{"a", "b"}, true},
		{"plain", []string{}, false},
		{"nullableOpen", []string{}, false},
	} {
		values, closed, refused := read(check.name)
		if refused != "" {
			t.Errorf("%s: refused: %s", check.name, refused)
			continue
		}
		if closed != check.closed || (closed && !reflect.DeepEqual(values, check.values)) {
			t.Errorf("%s: vocabulary = %v closed=%v, want %v closed=%v", check.name, values, closed, check.values, check.closed)
		}
	}
	if _, _, refused := read("negated"); !strings.Contains(refused, `"not"`) {
		t.Errorf("a schema using not was read instead of refused (%q)", refused)
	}
	// A string two oneOf arms admit is one oneOf rejects, so overlapping arms
	// are refused instead of read as their union (#620).
	for _, name := range []string{"oneOfShared", "oneOfOpen"} {
		if _, _, refused := read(name); !strings.Contains(refused, "oneOf arms") {
			t.Errorf("%s: overlapping oneOf arms were read instead of refused (%q)", name, refused)
		}
	}
}

// TestMembersComposeAcrossArms pins how property, item, and map-value schemas
// retain every declaration made through composition, and that compositions the
// reader cannot read exactly are refused rather than read leniently (#620).
func TestMembersComposeAcrossArms(t *testing.T) {
	t.Parallel()

	const document = `{"$defs": {
		"ownAndArms": {
			"type": "object",
			"properties": {"value": {"enum": ["a", "b"]}},
			"oneOf": [
				{"type": "object", "properties": {"value": {"const": "a"}}},
				{"type": "object", "properties": {"value": {"const": "b"}}}
			]
		},
		"sameArms": {"oneOf": [
			{"type": "object", "properties": {"value": {"const": "a"}}},
			{"type": "object", "properties": {"value": {"const": "a"}}}
		]},
		"overlappingArms": {"oneOf": [
			{"type": "object", "properties": {"value": {"enum": ["a", "b"]}}},
			{"type": "object", "properties": {"value": {"enum": ["b", "c"]}}}
		]},
		"oneOfMissingArm": {"oneOf": [
			{"type": "object", "properties": {"value": {"const": "a"}}},
			{"type": "object"}
		]},
		"oneOfObjects": {"oneOf": [
			{"type": "object", "properties": {"value": {"type": "object", "properties": {"kind": {"const": "a"}}}}},
			{"type": "object", "properties": {"value": {"type": "object", "properties": {"kind": {"const": "b"}}}}}
		]},
		"oneOfForbiddingArm": {
			"type": "object",
			"properties": {"value": {"enum": ["a", "b"]}},
			"oneOf": [
				{"not": {"required": ["value"]}},
				{"properties": {"value": {"const": "a"}}, "not": {"anyOf": [{"required": ["other"]}]}}
			]
		},
		"lateResultEvidence": {
			"type": "object", "additionalProperties": false,
			"required": ["kind", "late", "observed_at", "authority_lost_at"],
			"properties": {
				"kind": {"enum": ["observation", "gap"]},
				"result": {"type": "object"},
				"gap": {"type": "object", "additionalProperties": false, "required": ["reason"],
					"properties": {"reason": {"const": "observation_window_expired"}}},
				"late": {"const": true},
				"observed_at": {"type": "string", "format": "date-time"},
				"authority_lost_at": {"type": "string", "format": "date-time"}
			},
			"oneOf": [
				{"properties": {"kind": {"const": "observation"}}, "required": ["result"], "not": {"required": ["gap"]}},
				{"properties": {"kind": {"const": "gap"}}, "required": ["gap"], "not": {"required": ["result"]}}
			]
		},
		"lateResultEvidenceKindForbidden": {
			"type": "object", "additionalProperties": false,
			"required": ["kind", "late", "observed_at", "authority_lost_at"],
			"properties": {
				"kind": {"enum": ["observation", "gap"]},
				"result": {"type": "object"},
				"gap": {"type": "object", "additionalProperties": false, "required": ["reason"],
					"properties": {"reason": {"const": "observation_window_expired"}}},
				"late": {"const": true},
				"observed_at": {"type": "string", "format": "date-time"},
				"authority_lost_at": {"type": "string", "format": "date-time"}
			},
			"oneOf": [
				{"properties": {"kind": {"const": "observation"}}, "required": ["result"], "not": {"required": ["kind"]}},
				{"properties": {"kind": {"const": "gap"}}, "required": ["gap"], "not": {"required": ["result"]}}
			]
		},
		"declaringArmForbids": {
			"type": "object",
			"properties": {"value": {"enum": ["a", "b"]}},
			"anyOf": [
				{"properties": {"value": {"const": "a"}}, "not": {"anyOf": [{"required": ["value"]}]}},
				{"properties": {"value": {"const": "b"}}}
			]
		},
		"alwaysDeclaringArmForbids": {
			"type": "object",
			"allOf": [{"properties": {"value": {"const": "a"}}, "not": {"required": ["value"]}}],
			"properties": {"value": {"enum": ["a"]}}
		},
		"ownNotForbids": {
			"type": "object",
			"properties": {"value": {"enum": ["a"]}},
			"not": {"required": ["value"]}
		},
		"closedArmForbids": {"anyOf": [
			{"type": "object", "properties": {"value": {"enum": ["a"]}}},
			{"type": "object", "additionalProperties": false}
		]},
		"unevaluatedArmForbids": {"anyOf": [
			{"type": "object", "properties": {"value": {"enum": ["a"]}}},
			{"type": "object", "unevaluatedProperties": false}
		]},
		"notRequiredArmForbids": {"anyOf": [
			{"type": "object", "properties": {"value": {"enum": ["a"]}}},
			{"type": "object", "not": {"anyOf": [{"required": ["value"]}, {"required": ["other"]}]}}
		]},
		"pairRequiredDoesNotForbid": {"anyOf": [
			{"type": "object", "properties": {"value": {"enum": ["a"]}}},
			{"type": "object", "not": {"required": ["value", "other"]}}
		]},
		"everyArmForbids": {
			"type": "object",
			"properties": {"value": {"enum": ["a"]}},
			"anyOf": [
				{"additionalProperties": false},
				{"not": {"required": ["value"]}}
			]
		},
		"alwaysArmForbids": {
			"type": "object",
			"properties": {"value": {"enum": ["a"]}},
			"allOf": [{"type": "object", "additionalProperties": false}]
		},
		"bothBranchesForbid": {
			"type": "object",
			"properties": {"value": {"enum": ["a"]}},
			"if": {"properties": {"mode": {"const": "a"}}},
			"then": {"not": {"required": ["value"]}},
			"else": {"additionalProperties": false}
		},
		"closedTwice": {"allOf": [
			{"type": "object", "properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"a": {"type": "string"}}
			}}},
			{"type": "object", "properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"b": {"type": "string"}}
			}}}
		]},
		"closedBesideWider": {
			"type": "object",
			"properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"a": {"type": "string"}}
			}},
			"then": {"properties": {"value": {"properties": {"b": {"type": "string"}}}}}
		},
		"closedAlternatives": {"anyOf": [
			{"type": "object", "properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"a": {"type": "string"}}
			}}},
			{"type": "object", "properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"b": {"type": "string"}}
			}}}
		]},
		"closedAgreeing": {
			"type": "object",
			"properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"a": {"type": "string"}, "b": {"type": "string"}}
			}},
			"if": {"properties": {"mode": {"const": "a"}}},
			"then": {"properties": {"value": {
				"type": "object", "additionalProperties": false, "properties": {"a": {"const": "x"}, "b": {"type": "string"}}
			}}}
		},
		"allArms": {"allOf": [
			{"type": "object", "properties": {"value": {"enum": ["a", "b"]}}},
			{"type": "object", "properties": {"value": {"enum": ["b", "c"]}}}
		]},
		"missingArm": {"anyOf": [
			{"type": "object", "properties": {"value": {"const": "a"}}},
			{"type": "object"}
		]},
		"nullable": {"oneOf": [
			{"type": "null"},
			{"type": "object", "properties": {"value": {"const": "a"}}}
		]},
		"conditional": {
			"type": "object",
			"properties": {"value": {"enum": ["a", "b"]}},
			"if": {"properties": {"mode": {"const": "a"}}},
			"then": {"properties": {"value": {"const": "a"}}},
			"else": {"properties": {"value": {"const": "b"}}}
		},
		"thenOnly": {
			"if": {"properties": {"mode": {"const": "a"}}},
			"then": {"properties": {"value": {"const": "a"}}}
		},
		"items": {"oneOf": [
			{"type": "array", "items": {"const": "a"}},
			{"type": "array", "items": {"const": "b"}}
		]},
		"values": {"oneOf": [
			{"type": "object", "additionalProperties": {"const": "a"}},
			{"type": "object", "additionalProperties": {"const": "b"}}
		]},
		"nested": {"anyOf": [
			{"type": "object", "properties": {"value": {
				"type": "object", "properties": {"kind": {"const": "a"}}
			}}},
			{"type": "object", "properties": {"value": {
				"type": "object", "properties": {"kind": {"const": "b"}}
			}}}
		]},
		"allowedNot": {
			"type": "object",
			"properties": {"value": {"const": "a"}},
			"not": {"properties": {"value": true}}
		},
		"negated": {
			"type": "object",
			"not": {"properties": {"value": {"const": "a"}}}
		}
	}}`
	var parsed any
	if err := json.Unmarshal([]byte(document), &parsed); err != nil {
		t.Fatal(err)
	}
	type failure struct{ message string }
	set := &schemaSet{docs: map[string]any{"inline.json": parsed}}
	set.fatalf = func(format string, args ...any) { panic(failure{fmt.Sprintf(format, args...)}) }
	read := func(name string, tokens ...string) (values []string, closed bool, refused string) {
		defer func() {
			if recovered := recover(); recovered != nil {
				caught, ok := recovered.(failure)
				if !ok {
					panic(recovered)
				}
				refused = caught.message
			}
		}()
		member, ok := set.member(set.root("inline.json#/$defs/"+name), tokens...)
		if !ok {
			return nil, false, ""
		}
		admitted, closed := set.vocabulary(member)
		return sortedKeys(admitted), closed, ""
	}
	for _, check := range []struct {
		name   string
		tokens []string
		values []string
		closed bool
	}{
		{"ownAndArms", []string{"properties", "value"}, []string{"a", "b"}, true},
		{"oneOfForbiddingArm", []string{"properties", "value"}, []string{"a"}, true},
		{"closedArmForbids", []string{"properties", "value"}, []string{"a"}, true},
		{"unevaluatedArmForbids", []string{"properties", "value"}, []string{"a"}, true},
		{"notRequiredArmForbids", []string{"properties", "value"}, []string{"a"}, true},
		{"pairRequiredDoesNotForbid", []string{"properties", "value"}, nil, false},
		// An arm that declares a property but requires it to be absent admits
		// no value for it. lateResultEvidence copies common.v1.json
		// LateResultEvidence (result's ProcessResult $ref reduced to an
		// object); its KindForbidden twin names kind instead of gap in the
		// observation arm's not, which forbids every observation.
		{"lateResultEvidence", []string{"properties", "kind"}, []string{"gap", "observation"}, true},
		{"lateResultEvidenceKindForbidden", []string{"properties", "kind"}, []string{"gap"}, true},
		{"declaringArmForbids", []string{"properties", "value"}, []string{"b"}, true},
		{"allArms", []string{"properties", "value"}, []string{"b"}, true},
		{"missingArm", []string{"properties", "value"}, nil, false},
		{"nullable", []string{"properties", "value"}, []string{"a"}, true},
		{"conditional", []string{"properties", "value"}, []string{"a", "b"}, true},
		{"thenOnly", []string{"properties", "value"}, nil, false},
		{"items", []string{"items"}, []string{"a", "b"}, true},
		{"values", []string{"additionalProperties"}, []string{"a", "b"}, true},
		{"allowedNot", []string{"properties", "value"}, []string{"a"}, true},
	} {
		values, closed, refused := read(check.name, check.tokens...)
		if refused != "" {
			t.Errorf("%s: refused: %s", check.name, refused)
			continue
		}
		if closed != check.closed || (closed && !reflect.DeepEqual(values, check.values)) {
			t.Errorf("%s: member vocabulary = %v closed=%v, want %v closed=%v", check.name, values, closed, check.values, check.closed)
		}
	}
	nested, ok := set.member(set.root("inline.json#/$defs/nested"), "properties", "value")
	if !ok {
		t.Fatal("nested: value member was not found")
	}
	kind, ok := set.member(nested, "properties", "kind")
	if !ok {
		t.Fatal("nested: kind member was not found")
	}
	values, closed := set.vocabulary(kind)
	if got := sortedKeys(values); !closed || !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("nested: member vocabulary = %v closed=%v, want [a b] closed=true", got, closed)
	}
	if _, _, refused := read("negated", "properties", "value"); !strings.Contains(refused, `"not"`) {
		t.Errorf("a property narrowed by not was read instead of refused (%q)", refused)
	}
	// Shapes this reader does not model are refused, never read leniently.
	for _, check := range []struct{ name, refusal string }{
		// A value two oneOf arms admit is one oneOf rejects: identical arms
		// admit nothing, so they are not read as their union.
		{"sameArms", "oneOf arms"},
		{"overlappingArms", "oneOf arms"},
		{"oneOfMissingArm", "oneOf arms"},
		{"oneOfObjects", "oneOf arms"},
		// A property that some declaration admits and an arm that always
		// applies, or every arm of a group, forbids.
		{"everyArmForbids", "being forbidden by"},
		{"alwaysArmForbids", "being forbidden by"},
		{"bothBranchesForbid", "being forbidden by"},
		{"alwaysDeclaringArmForbids", "being forbidden by"},
		{"ownNotForbids", "being forbidden by its own not"},
		// A closed declaration that omits a property another declaration of
		// the same member declares rejects it, though the merge would admit it.
		{"closedTwice", "closed declaration"},
		{"closedBesideWider", "closed declaration"},
		{"closedAlternatives", "closed declaration"},
	} {
		if _, _, refused := read(check.name, "properties", "value"); !strings.Contains(refused, check.refusal) {
			t.Errorf("%s: was read instead of refused with %q (%q)", check.name, check.refusal, refused)
		}
	}
	// Closed declarations that agree on their properties compose: each one
	// admits exactly what the merged shape does.
	agreeing, ok := set.member(set.root("inline.json#/$defs/closedAgreeing"), "properties", "value")
	if !ok {
		t.Fatal("closedAgreeing: value member was not found")
	}
	if shape := set.shape(agreeing, nil); !shape.closed || !reflect.DeepEqual(sortedKeys(shape.properties), []string{"a", "b"}) {
		t.Errorf("closedAgreeing: shape = %v closed=%v, want [a b] closed=true", sortedKeys(shape.properties), shape.closed)
	}
}

// TestMethodGuardReader pins the deliberately small handler shape used by
// method-less mux registrations. A handler that stops using that shape must be
// made explicit here rather than silently weakening the route drift check.
func TestMethodGuardReader(t *testing.T) {
	t.Parallel()

	const source = `package inline
		import "net/http"
		func post(r *http.Request) { if r.Method != http.MethodPost { return } }
		func read(r *http.Request) { if r.Method != http.MethodGet && r.Method != http.MethodHead { return } }
		func literal(r *http.Request) { if r.Method != "PATCH" { return } }
		func switched(r *http.Request) { switch r.Method {} }
		func equal(r *http.Request) { if r.Method == http.MethodPost { return } }
		func late(r *http.Request) { _ = r.Method; if r.Method != http.MethodPost { return } }
		func noReturn(r *http.Request) { if r.Method != http.MethodPost { _ = r.Method } }
	`
	file, err := parser.ParseFile(token.NewFileSet(), "inline.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]*ast.FuncDecl{}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = fn
		}
	}
	for _, check := range []struct {
		name string
		want []string
	}{
		{"post", []string{"POST"}},
		{"read", []string{"GET", "HEAD"}},
		{"literal", []string{"PATCH"}},
	} {
		got, err := methodGuard(functions[check.name])
		if err != nil {
			t.Errorf("%s: %v", check.name, err)
			continue
		}
		if !reflect.DeepEqual(got, check.want) {
			t.Errorf("%s: accepted methods = %v, want %v", check.name, got, check.want)
		}
	}
	for _, name := range []string{"switched", "equal", "late", "noReturn"} {
		if _, err := methodGuard(functions[name]); err == nil {
			t.Errorf("%s: unsupported method guard was accepted", name)
		}
	}
}

// TestNodeProjectionsValidateAgainstPublishedSchemas checks the composed Node
// schemas semantically with a real validator, on instances built from the Go
// types with every field set, and checks that each still refuses a stray field.
func TestNodeProjectionsValidateAgainstPublishedSchemas(t *testing.T) {
	t.Parallel()

	observed := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", ConnectHost: "node-1.example", RootInstanceID: "root-1",
		OS: "linux", Architecture: "arm64", AgentVersion: "0.1.0",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": false}, CapabilityRevision: 3,
		CapabilityObservedAt: observed, MissingCapabilities: []string{"kind:oci"},
		CapabilityReasonCode: contract.CapabilityReasonCodes()[0], SupersedeCapabilityRevision: true,
	}
	node := l1.Node{
		NodeRegistration: registration, State: contract.NodeAlive, AuthoritativeTags: []string{"gpu"},
		MaxOneshotSlots: 2, MaxServiceSlots: 1, OneshotOccupancy: 1, ServiceOccupancy: 1, Overcommitted: false,
		AuthorityGeneration: 4, ClaimsEnabled: true, IntentRevision: 2, IntentReason: "maintenance done",
		IntentUpdatedAt: &observed, IntentActor: "operator", LastHeartbeatAt: observed,
	}
	heartbeat := l1.HeartbeatResponse{
		Node: node, RemovalDirectives: []l1.RemovalDirective{}, StorageResetDirectives: []l1.ComputerStorageResetDirective{},
		StorageGrowDirectives: []l1.ComputerStorageGrowDirective{}, ReimageDirectives: []l1.ComputerReimagePreflightDirective{},
		BackupDirectives: []l1.ComputerBackupDirective{}, BackupPruneDirectives: []l1.ComputerBackupPruneDirective{},
		StorageCopyDirectives: []l1.ComputerStorageCopyDirective{}, CustodyExportDirectives: []l1.ComputerCustodyExportDirective{},
	}
	for _, check := range []struct {
		schema string
		value  any
	}{
		{"NodeRegistration", registration},
		{"Node", node},
		{"HeartbeatResponse", heartbeat},
	} {
		compiled := compileProtocolSchema(t, "file:///api/openapi/common.v1.json#/components/schemas/"+check.schema)
		payload, err := json.Marshal(check.value)
		if err != nil {
			t.Fatal(err)
		}
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(string(payload)))
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(instance); err != nil {
			t.Errorf("a Go %s is refused by the published %s schema: %v", reflect.TypeOf(check.value), check.schema, err)
		}
		stray, err := jsonschema.UnmarshalJSON(strings.NewReader(strings.TrimSuffix(string(payload), "}") + `,"stray_field":true}`))
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(stray); err == nil {
			t.Errorf("the published %s schema accepts an undeclared field", check.schema)
		}
	}
}

// TestEveryServedRouteIsPublished keeps a handler from going live without an
// OpenAPI operation, which is how the agent's service-binding-proof and
// image-reconciliation-failure routes went unpublished (#601). Registrations
// without a method pattern are held to the methods their handler guard accepts,
// so publishing the right path under the wrong method is drift too (#620).
func TestEveryServedRouteIsPublished(t *testing.T) {
	t.Parallel()

	methods := map[string]bool{
		"get": true, "put": true, "post": true, "delete": true,
		"options": true, "head": true, "patch": true, "trace": true,
	}
	published := func(documents ...string) map[string]bool {
		operations := map[string]bool{}
		for _, name := range documents {
			for route, value := range object(t, readObject(t, name)["paths"], name+" paths") {
				for method := range object(t, value, route) {
					if !methods[method] {
						continue
					}
					operations[strings.ToUpper(method)+" "+route] = true
				}
			}
		}
		return operations
	}
	l1Operations := published(clientDoc, agentDoc)
	l3Operations := published(l3Doc)

	methodRoute := regexp.MustCompile(`\.Handle(?:Func)?\("([A-Z]+) (/[^"]*)"`)
	pathOnlyRegistration := regexp.MustCompile(`\.Handle\("(/[^"]*)", s\.authenticateFabric\(http\.HandlerFunc\(`)
	pathOnlyHandler := regexp.MustCompile(`\.Handle\("(/[^"]*)", s\.authenticateFabric\(http\.HandlerFunc\(s\.(\w+)\)\)\)`)
	type guardedRoute struct {
		path    string
		handler string
		methods []string
	}
	type servedRoutes struct {
		operations []string
		guarded    []guardedRoute
	}
	served := func(source string) servedRoutes {
		raw, err := os.ReadFile(filepath.Join("..", "..", source))
		if err != nil {
			t.Fatal(err)
		}
		var routes servedRoutes
		for _, match := range methodRoute.FindAllStringSubmatch(string(raw), -1) {
			if strings.Contains(match[2], "{$}") {
				continue // the trailing-slash alias of a published collection route
			}
			routes.operations = append(routes.operations, match[1]+" "+match[2])
		}
		declarations := map[string][]*ast.FuncDecl{}
		for _, file := range parseSources(t, filepath.Dir(source)) {
			for _, declaration := range file.Decls {
				if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv != nil {
					declarations[fn.Name.Name] = append(declarations[fn.Name.Name], fn)
				}
			}
		}
		read := map[string]bool{}
		for _, match := range pathOnlyHandler.FindAllStringSubmatch(string(raw), -1) {
			candidates := declarations[match[2]]
			if len(candidates) != 1 {
				t.Fatalf("%s registers handler %s, which names %d method declarations instead of one", source, match[2], len(candidates))
			}
			accepted, err := methodGuard(candidates[0])
			if err != nil {
				t.Fatalf("%s handler %s: %v", source, match[2], err)
			}
			read[match[1]] = true
			routes.guarded = append(routes.guarded, guardedRoute{path: match[1], handler: match[2], methods: accepted})
		}
		// A method-less registration in any other handler form would drop out
		// of the check entirely instead of being matched by path.
		for _, match := range pathOnlyRegistration.FindAllStringSubmatch(string(raw), -1) {
			if !read[match[1]] {
				t.Errorf("%s registers %s without a method pattern through a handler whose method guard the drift test cannot read", source, match[1])
			}
		}
		return routes
	}
	check := func(source string, routes servedRoutes, operations map[string]bool) {
		if len(routes.operations)+len(routes.guarded) == 0 {
			t.Fatalf("found no routes in %s", source)
		}
		for _, route := range routes.operations {
			if !operations[route] {
				t.Errorf("%s serves %s, which no OpenAPI operation publishes", source, route)
			}
		}
		for _, route := range routes.guarded {
			accepted := map[string]bool{}
			for _, method := range route.methods {
				accepted[method] = true
				operation := method + " " + route.path
				if !operations[operation] {
					t.Errorf("%s handler %s accepts %s, which no OpenAPI operation publishes", source, route.handler, operation)
				}
			}
			for operation := range operations {
				method, pattern, _ := strings.Cut(operation, " ")
				if pattern == route.path && !accepted[method] {
					t.Errorf("OpenAPI publishes %s, which %s handler %s refuses", operation, source, route.handler)
				}
			}
		}
	}
	check("l1/server.go", served("l1/server.go"), l1Operations)
	l3Routes := served("l3/server.go")
	for _, route := range l3.ComputerTokenRoutes() {
		l3Routes.operations = append(l3Routes.operations, route.Method+" "+route.Path)
	}
	check("l3/server.go", l3Routes, l3Operations)
}

// ---- schema navigation ----

type schemaSet struct {
	fatalf func(format string, args ...any)
	docs   map[string]any
}

type schemaLocation struct {
	doc     string
	pointer string
	node    map[string]any
}

func (l schemaLocation) key() string { return l.doc + "#" + l.pointer }

// schemaRef is a schema location together with its dynamic scope: the schema
// resources evaluation passed through to reach it, outermost first. A
// $dynamicRef resolves against that scope, which is how one JobSpec property
// means the strict executable under JobSpec and the scrubbed-or-strict one
// under JobRecordSpec.
type schemaRef struct {
	loc      schemaLocation
	scope    []schemaLocation
	composed *composedSchema
}

// composedSchema is a synthetic schema for one member declared in several
// composition arms. Every schema in all applies; one schema in each
// alternatives group applies. An empty composition is an open placeholder for
// an arm that does not declare the member.
type composedSchema struct {
	all          []schemaRef
	alternatives [][]schemaRef
}

func identity(r schemaRef) string {
	var out strings.Builder
	var write func(schemaRef)
	write = func(r schemaRef) {
		out.WriteString(r.loc.key())
		out.WriteByte('@')
		out.WriteString(scopeKey(r.scope))
		if r.composed == nil {
			return
		}
		out.WriteString("[all:")
		for _, member := range r.composed.all {
			write(member)
			out.WriteByte(';')
		}
		out.WriteString("|alternatives:")
		for _, group := range r.composed.alternatives {
			out.WriteByte('(')
			for _, member := range group {
				write(member)
				out.WriteByte(';')
			}
			out.WriteByte(')')
		}
		out.WriteByte(']')
	}
	write(r)
	return out.String()
}

func newSchemaSet(t *testing.T) *schemaSet {
	return &schemaSet{fatalf: t.Fatalf, docs: map[string]any{}}
}

func (s *schemaSet) document(name string) any {
	if document, ok := s.docs[name]; ok {
		return document
	}
	raw, err := os.ReadFile(filepath.FromSlash(name))
	if err != nil {
		s.fatalf("%v", err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		s.fatalf("%s: %v", name, err)
	}
	s.docs[name] = document
	return document
}

// location resolves "document#/json/pointer" without following references.
func (s *schemaSet) location(reference string) schemaLocation {
	document, pointer, _ := strings.Cut(reference, "#")
	var current any = s.document(document)
	if pointer != "" {
		for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
			token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
			switch value := current.(type) {
			case map[string]any:
				current = value[token]
			case []any:
				index, err := strconv.Atoi(token)
				if err != nil || index < 0 || index >= len(value) {
					return schemaLocation{doc: document, pointer: pointer}
				}
				current = value[index]
			default:
				return schemaLocation{doc: document, pointer: pointer}
			}
		}
	}
	node, _ := current.(map[string]any)
	return schemaLocation{doc: document, pointer: pointer, node: node}
}

// root starts a walk at a location, inside its document's resource.
func (s *schemaSet) root(reference string) schemaRef {
	l := s.location(reference)
	return schemaRef{loc: l, scope: []schemaLocation{s.location(l.doc + "#")}}
}

func (s *schemaSet) child(r schemaRef, tokens ...string) schemaRef {
	pointer := r.loc.pointer
	for _, token := range tokens {
		pointer += "/" + escapePointer(token)
	}
	return schemaRef{loc: s.location(r.loc.doc + "#" + pointer), scope: r.scope}
}

func enterResource(scope []schemaLocation, resource schemaLocation) []schemaLocation {
	if len(scope) > 0 && scope[len(scope)-1].key() == resource.key() {
		return scope
	}
	return append(append([]schemaLocation(nil), scope...), resource)
}

// annotationKeywords change no instance's validity. A $ref whose siblings are
// all annotations is a plain alias; any other sibling still applies beside the
// reference (OpenAPI 3.1 / JSON Schema 2020-12), as if allOf[target, siblings].
var annotationKeywords = map[string]bool{
	"$ref": true, "$dynamicRef": true, "$id": true, "$schema": true, "$anchor": true, "$dynamicAnchor": true,
	"$defs": true, "$comment": true, "title": true, "description": true, "default": true, "examples": true,
	"example": true, "deprecated": true, "readOnly": true, "writeOnly": true, "format": true, "externalDocs": true,
}

func pureReference(node map[string]any) bool {
	_, hasRef := node["$ref"]
	_, hasDynamic := node["$dynamicRef"]
	if !hasRef && !hasDynamic {
		return false
	}
	for key := range node {
		if !annotationKeywords[key] && !strings.HasPrefix(key, "x-") {
			return false
		}
	}
	return true
}

// resolve follows plain-alias $ref and $dynamicRef chains, reporting every hop
// to visit. It stops at a reference with applicator siblings; the readers below
// treat that reference as one more allOf arm. A reference kind it cannot follow
// is a test failure, never an empty shape: that is exactly where drift hides.
func (s *schemaSet) resolve(r schemaRef, visit func(schemaLocation)) schemaRef {
	for range 32 {
		if r.loc.node == nil {
			s.fatalf("%s: no schema at this location", r.loc.key())
		}
		if visit != nil {
			visit(r.loc)
		}
		if _, ok := r.loc.node["$id"]; ok {
			r.scope = enterResource(r.scope, r.loc)
		}
		for _, unsupported := range []string{"$recursiveRef", "$recursiveAnchor"} {
			if _, ok := r.loc.node[unsupported]; ok {
				s.fatalf("%s: the drift resolver does not follow %s", r.loc.key(), unsupported)
			}
		}
		if !pureReference(r.loc.node) {
			return r
		}
		r, _ = s.reference(r)
	}
	s.fatalf("%s: reference chain too deep", r.loc.key())
	return r
}

// reference follows the one $ref or $dynamicRef a (resolved) node carries.
func (s *schemaSet) reference(r schemaRef) (schemaRef, bool) {
	ref, hasRef := r.loc.node["$ref"]
	dynamic, hasDynamic := r.loc.node["$dynamicRef"]
	switch {
	case hasRef && hasDynamic:
		s.fatalf("%s: both $ref and $dynamicRef", r.loc.key())
	case hasRef:
		text, ok := ref.(string)
		if !ok {
			s.fatalf("%s: $ref is not a string", r.loc.key())
		}
		return s.follow(r, text), true
	case hasDynamic:
		text, ok := dynamic.(string)
		if !ok {
			s.fatalf("%s: $dynamicRef is not a string", r.loc.key())
		}
		return s.dynamic(r, text), true
	}
	return schemaRef{}, false
}

// arms splits a resolved schema's subschemas by how they apply: every arm of
// all applies (allOf members, and a $ref beside applicator siblings); one arm
// of each alternative group applies (oneOf, anyOf); conditional arms apply
// sometimes (then, else).
func (s *schemaSet) arms(r schemaRef) (all []schemaRef, alternatives [][]schemaRef, conditional []schemaRef) {
	if r.composed != nil {
		return r.composed.all, r.composed.alternatives, nil
	}
	if members, ok := r.loc.node["allOf"].([]any); ok {
		for index := range members {
			all = append(all, s.child(r, "allOf", strconv.Itoa(index)))
		}
	}
	if target, ok := s.reference(r); ok {
		all = append(all, target)
	}
	for _, keyword := range []string{"oneOf", "anyOf"} {
		if members, ok := r.loc.node[keyword].([]any); ok {
			group := make([]schemaRef, 0, len(members))
			for index := range members {
				group = append(group, s.child(r, keyword, strconv.Itoa(index)))
			}
			alternatives = append(alternatives, group)
		}
	}
	for _, keyword := range []string{"then", "else"} {
		if _, ok := r.loc.node[keyword].(map[string]any); ok {
			conditional = append(conditional, s.child(r, keyword))
		}
	}
	return all, alternatives, conditional
}

func (s *schemaSet) follow(r schemaRef, reference string) schemaRef {
	file, fragment, _ := strings.Cut(reference, "#")
	document, scope := r.loc.doc, r.scope
	if file != "" {
		if strings.Contains(file, ":") {
			s.fatalf("%s: the drift resolver does not follow absolute reference %q", r.loc.key(), reference)
		}
		document = path.Clean(path.Join(path.Dir(r.loc.doc), file))
		scope = enterResource(scope, s.location(document+"#"))
	}
	var target schemaLocation
	switch {
	case fragment == "" || strings.HasPrefix(fragment, "/"):
		target = s.location(document + "#" + fragment)
	default:
		found, _, ok := s.anchor(s.location(document+"#"), fragment)
		if !ok {
			s.fatalf("%s: $ref %q names no anchor", r.loc.key(), reference)
		}
		target = found
	}
	if target.node == nil {
		s.fatalf("%s: unresolvable $ref %q", r.loc.key(), reference)
	}
	return schemaRef{loc: target, scope: scope}
}

// dynamic resolves "#name" as JSON Schema 2020-12 does: first inside the
// current resource, and when that lands on a $dynamicAnchor, at the outermost
// resource in the dynamic scope that declares the same $dynamicAnchor.
func (s *schemaSet) dynamic(r schemaRef, reference string) schemaRef {
	name, isFragment := strings.CutPrefix(reference, "#")
	if !isFragment || name == "" || strings.HasPrefix(name, "/") {
		s.fatalf("%s: the drift resolver follows only same-resource \"#name\" $dynamicRef, not %q", r.loc.key(), reference)
	}
	initial, isDynamic, ok := s.anchor(r.scope[len(r.scope)-1], name)
	if !ok {
		s.fatalf("%s: $dynamicRef %q names no anchor in its resource", r.loc.key(), reference)
	}
	if isDynamic {
		for _, resource := range r.scope {
			if found, dynamicAnchor, ok := s.anchor(resource, name); ok && dynamicAnchor {
				return schemaRef{loc: found, scope: r.scope}
			}
		}
	}
	return schemaRef{loc: initial, scope: r.scope}
}

// anchor finds "$anchor" or "$dynamicAnchor" name inside one resource, not
// descending into nested resources (subschemas with their own $id).
func (s *schemaSet) anchor(resource schemaLocation, name string) (schemaLocation, bool, bool) {
	var walk func(value any, pointer string, top bool) (string, bool, bool)
	walk = func(value any, pointer string, top bool) (string, bool, bool) {
		switch node := value.(type) {
		case map[string]any:
			if _, nested := node["$id"]; nested && !top {
				return "", false, false
			}
			if node["$dynamicAnchor"] == name {
				return pointer, true, true
			}
			if node["$anchor"] == name {
				return pointer, false, true
			}
			keys := make([]string, 0, len(node))
			for key := range node {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				if found, dynamicAnchor, ok := walk(node[key], pointer+"/"+escapePointer(key), false); ok {
					return found, dynamicAnchor, true
				}
			}
		case []any:
			for index, item := range node {
				if found, dynamicAnchor, ok := walk(item, pointer+"/"+strconv.Itoa(index), false); ok {
					return found, dynamicAnchor, true
				}
			}
		}
		return "", false, false
	}
	found, dynamicAnchor, ok := walk(resource.node, resource.pointer, true)
	if !ok {
		return schemaLocation{}, false, false
	}
	return s.location(resource.doc + "#" + found), dynamicAnchor, true
}

type objectShape struct {
	properties map[string]bool
	required   map[string]bool
	closed     bool
}

func (shape objectShape) isObject() bool { return len(shape.properties) > 0 || shape.closed }

// shape merges what an object schema declares. Arms that always apply (allOf
// members, a $ref beside siblings) contribute required fields and closure;
// alternative and conditional arms only contribute properties, since what
// they require holds on one arm alone.
func (s *schemaSet) shape(r schemaRef, visit func(schemaLocation)) objectShape {
	shape := objectShape{properties: map[string]bool{}, required: map[string]bool{}}
	seen := map[string]bool{}
	var walk func(schemaRef, bool)
	walk = func(r schemaRef, unconditional bool) {
		r = s.resolve(r, visit)
		key := identity(r)
		if seen[key] {
			return
		}
		seen[key] = true
		if properties, ok := r.loc.node["properties"].(map[string]any); ok {
			names := make([]string, 0, len(properties))
			for name := range properties {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				shape.properties[name] = true
			}
		}
		if unconditional {
			if required, ok := r.loc.node["required"].([]any); ok {
				for _, name := range required {
					if value, ok := name.(string); ok {
						shape.required[value] = true
					}
				}
			}
			if r.loc.node["additionalProperties"] == false || r.loc.node["unevaluatedProperties"] == false {
				shape.closed = true
			}
		}
		all, alternatives, conditional := s.arms(r)
		for _, arm := range all {
			walk(arm, unconditional)
		}
		for _, group := range alternatives {
			for _, arm := range group {
				walk(arm, false)
			}
		}
		for _, arm := range conditional {
			walk(arm, false)
		}
	}
	walk(r, true)
	return shape
}

// declaresFreeFormObjectNode reports a schema that deliberately publishes an
// object without a shape, such as {"type": "object"}.
func declaresFreeFormObjectNode(node map[string]any) bool {
	switch value := node["type"].(type) {
	case string:
		return value == "object"
	case []any:
		for _, item := range value {
			if item == "object" {
				return true
			}
		}
	}
	return false
}

func (s *schemaSet) declaresFreeFormObject(r schemaRef) bool {
	seen := map[string]bool{}
	var walk func(schemaRef) bool
	walk = func(r schemaRef) bool {
		r = s.resolve(r, nil)
		key := identity(r)
		if seen[key] {
			return false
		}
		seen[key] = true
		if declaresFreeFormObjectNode(r.loc.node) {
			return true
		}
		all, _, _ := s.arms(r)
		for _, arm := range all {
			if walk(arm) {
				return true
			}
		}
		return false
	}
	return walk(r)
}

func memberValue(node map[string]any, tokens ...string) (any, bool) {
	var current any = node
	for _, token := range tokens {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func (s *schemaSet) placeholder(r schemaRef) schemaRef {
	r = s.resolve(r, nil)
	r.loc.node = map[string]any{}
	r.composed = &composedSchema{}
	return r
}

func (s *schemaSet) notNarrowsMember(r schemaRef, tokens []string, seen map[string]bool) bool {
	r = s.resolve(r, nil)
	key := identity(r) + "|" + strings.Join(tokens, "/")
	if seen[key] {
		return false
	}
	seen[key] = true
	defer delete(seen, key)
	if value, ok := memberValue(r.loc.node, tokens...); ok && value != true {
		return true
	}
	all, alternatives, conditional := s.arms(r)
	for _, group := range alternatives {
		all = append(all, group...)
	}
	all = append(all, conditional...)
	for _, arm := range all {
		if s.notNarrowsMember(arm, tokens, seen) {
			return true
		}
	}
	if _, ok := r.loc.node["not"].(map[string]any); ok {
		return s.notNarrowsMember(s.child(r, "not"), tokens, seen)
	}
	return false
}

// member combines every declaration of a property, array item, or map value
// according to the composition that makes it apply. Missing alternative arms
// are open placeholders; arms whose type excludes the containing value do not
// participate, and neither do arms that forbid the property (see forbids). A
// negated declaration is refused because its admitted member set cannot be
// recovered by this reader.
//
// This is a structural reader, not a validator: it models the compositions
// the protocol schemas use and refuses the rest rather than guessing. It
// refuses a property every arm of a group, or any arm that always applies,
// forbids; oneOf arms whose admitted values it cannot show to be disjoint,
// since a value two arms admit is one oneOf rejects; and a closed object
// declaration that omits a property another declaration of the same member
// declares, since merging the declarations would admit a value the closed one
// rejects.
func (s *schemaSet) member(r schemaRef, tokens ...string) (schemaRef, bool) {
	container := "object"
	if len(tokens) == 1 && tokens[0] == "items" {
		container = "array"
	}
	property := ""
	if len(tokens) == 2 && tokens[0] == "properties" {
		property = tokens[1]
	}
	// forbidding reports an arm that rules the property out without declaring
	// it (see forbids); absent, an arm that declares it but whose not still
	// requires it to be absent, so the arm admits no value for it.
	forbidding := func(arm schemaRef) bool { return property != "" && s.forbids(arm, property) }
	absent := func(arm schemaRef) bool { return property != "" && s.requiresAbsent(arm, property) }
	seen := map[string]bool{}
	var read func(schemaRef) (schemaRef, bool)
	read = func(r schemaRef) (schemaRef, bool) {
		r = s.resolve(r, nil)
		key := identity(r) + "|" + strings.Join(tokens, "/")
		if seen[key] {
			return schemaRef{}, false
		}
		seen[key] = true
		defer delete(seen, key)

		if _, ok := r.loc.node["not"].(map[string]any); ok &&
			s.notNarrowsMember(s.child(r, "not"), tokens, map[string]bool{}) {
			s.fatalf("%s: the drift member reader does not model %q narrowing %s", r.loc.key(), "not", strings.Join(tokens, "/"))
		}

		var all []schemaRef
		var alternatives [][]schemaRef
		var first schemaRef
		found := false
		remember := func(member schemaRef) {
			if !found {
				first = member
			}
			found = true
		}
		mergeAlways := func(member schemaRef) {
			remember(member)
			if member.composed == nil {
				all = append(all, member)
				return
			}
			all = append(all, member.composed.all...)
			alternatives = append(alternatives, member.composed.alternatives...)
		}

		if value, ok := memberValue(r.loc.node, tokens...); ok {
			switch value := value.(type) {
			case map[string]any:
				mergeAlways(s.child(r, tokens...))
			case bool:
				if !value {
					s.fatalf("%s: the drift member reader does not model a false schema at %s", r.loc.key(), strings.Join(tokens, "/"))
				}
				open := s.child(r, tokens...)
				open.loc.node = map[string]any{}
				open.composed = &composedSchema{}
				mergeAlways(open)
			default:
				s.fatalf("%s: %s is not a schema", r.loc.key(), strings.Join(tokens, "/"))
			}
		}

		// forbidden names an arm or group that rules the property out wherever
		// it applies; it matters only if some other declaration admits it.
		forbidden := ""
		always, groups, conditional := s.arms(r)
		for _, arm := range always {
			member, declared := read(arm)
			switch {
			case declared && !absent(arm):
				mergeAlways(member)
			case declared || forbidding(arm):
				forbidden = s.resolve(arm, nil).loc.key()
			}
		}
		for _, group := range groups {
			exclusive := r.composed == nil && len(group) > 0 && path.Base(path.Dir(group[0].loc.pointer)) == "oneOf"
			var members []schemaRef
			groupDeclares, participating := false, 0
			for _, arm := range group {
				resolved := s.resolve(arm, nil)
				if kind, ok := resolved.loc.node["type"]; ok && !typeAdmits(kind, container) {
					continue
				}
				participating++
				member, declared := read(arm)
				switch {
				case declared && !absent(arm):
					if !groupDeclares {
						remember(member)
					}
					groupDeclares = true
					members = append(members, member)
				case !declared && !forbidding(arm):
					members = append(members, s.placeholder(arm))
				}
			}
			if participating > 0 && len(members) == 0 {
				forbidden = "every arm of " + path.Dir(group[0].loc.pointer)
			}
			if groupDeclares {
				if exclusive {
					s.disjoint(r, tokens, members)
				}
				alternatives = append(alternatives, members)
			}
		}
		if len(conditional) > 0 {
			byKeyword := map[string]schemaRef{}
			for _, arm := range conditional {
				byKeyword[path.Base(arm.loc.pointer)] = arm
			}
			members := make([]schemaRef, 0, 2)
			groupDeclares := false
			for _, keyword := range []string{"then", "else"} {
				arm, ok := byKeyword[keyword]
				if !ok {
					members = append(members, s.placeholder(r))
					continue
				}
				member, declared := read(arm)
				switch {
				case declared && !absent(arm):
					if !groupDeclares {
						remember(member)
					}
					groupDeclares = true
					members = append(members, member)
				case !declared && !forbidding(arm):
					members = append(members, s.placeholder(arm))
				}
			}
			if len(members) == 0 {
				forbidden = "both then and else"
			}
			if groupDeclares {
				alternatives = append(alternatives, members)
			}
		}

		if !found {
			return schemaRef{}, false
		}
		if forbidden != "" {
			s.fatalf("%s: the drift member reader does not model %s, which another declaration admits, being forbidden by %s",
				r.loc.key(), strings.Join(tokens, "/"), forbidden)
		}
		if len(all) == 1 && len(alternatives) == 0 && identity(first) == identity(all[0]) {
			return all[0], true
		}
		combined := s.resolve(first, nil)
		combined.loc.node = map[string]any{}
		combined.composed = &composedSchema{all: all, alternatives: alternatives}
		return combined, true
	}
	member, ok := read(r)
	if ok && absent(r) {
		s.fatalf("%s: the drift member reader does not model %s, which the schema declares, being forbidden by its own not",
			r.loc.key(), strings.Join(tokens, "/"))
	}
	if ok && member.composed != nil {
		s.closures(member)
	}
	return member, ok
}

// forbids reports an arm that rules a property out without declaring it: an
// arm closed by additionalProperties or unevaluatedProperties false, or one
// that requires the property to be absent. The caller has already found that
// the arm declares no schema for the property.
func (s *schemaSet) forbids(arm schemaRef, name string) bool {
	return s.shape(arm, nil).closed || s.requiresAbsent(arm, name)
}

// requiresAbsent reports a schema whose own not (or the not of an arm of it
// that always applies) requires the property alone, directly or as one anyOf
// alternative: whatever the schema declares for the property, it admits no
// instance that carries it.
func (s *schemaSet) requiresAbsent(arm schemaRef, name string) bool {
	requiresOnly := func(node any) bool {
		object, ok := node.(map[string]any)
		if !ok {
			return false
		}
		for key := range object {
			if key != "required" && key != "description" {
				return false
			}
		}
		required, ok := object["required"].([]any)
		return ok && len(required) == 1 && required[0] == name
	}
	seen := map[string]bool{}
	var walk func(schemaRef) bool
	walk = func(r schemaRef) bool {
		r = s.resolve(r, nil)
		key := identity(r)
		if seen[key] {
			return false
		}
		seen[key] = true
		if negated, ok := r.loc.node["not"].(map[string]any); ok {
			if requiresOnly(negated) {
				return true
			}
			if alternatives, ok := negated["anyOf"].([]any); ok && len(negated) == 1 {
				for _, alternative := range alternatives {
					if requiresOnly(alternative) {
						return true
					}
				}
			}
		}
		all, _, _ := s.arms(r)
		for _, arm := range all {
			if walk(arm) {
				return true
			}
		}
		return false
	}
	return walk(arm)
}

// disjoint holds a oneOf group's members of one property to admitted values
// the reader can show are disjoint: closed, non-empty vocabularies that share
// no value. Anything else may admit a value through two arms, which oneOf
// rejects, so it is refused rather than read as a union.
func (s *schemaSet) disjoint(r schemaRef, tokens []string, members []schemaRef) {
	if len(members) < 2 {
		return
	}
	owner := map[string]string{}
	for _, member := range members {
		admitted, closed := s.vocabulary(member)
		if !closed || len(admitted) == 0 {
			s.fatalf("%s: the drift member reader does not model oneOf arms whose %s values it cannot show are disjoint (%s admits an open or non-string set)",
				r.loc.key(), strings.Join(tokens, "/"), member.loc.key())
		}
		for value := range admitted {
			if other, taken := owner[value]; taken {
				s.fatalf("%s: the drift member reader does not model oneOf arms that both admit %s %q (%s and %s)",
					r.loc.key(), strings.Join(tokens, "/"), value, other, member.loc.key())
			}
			owner[value] = member.loc.key()
		}
	}
}

// closures refuses a composed member whose closed object declarations do not
// all declare every property the member declares. Each declaration applies on
// its own, so a closed one rejects a property only another declaration
// declares, which the merged shape would admit.
func (s *schemaSet) closures(member schemaRef) {
	names := sortedKeys(s.shape(member, nil).properties)
	seen := map[string]bool{}
	var walk func(schemaRef)
	walk = func(r schemaRef) {
		key := identity(r)
		if seen[key] {
			return
		}
		seen[key] = true
		if r.composed != nil {
			for _, declaration := range r.composed.all {
				walk(declaration)
			}
			for _, group := range r.composed.alternatives {
				for _, declaration := range group {
					walk(declaration)
				}
			}
			return
		}
		shape := s.shape(r, nil)
		if !shape.closed {
			return
		}
		for _, name := range names {
			if !shape.properties[name] {
				s.fatalf("%s: the drift member reader does not model the closed declaration %s, which omits %q that another declaration of the same member declares",
					member.loc.key(), s.resolve(r, nil).loc.key(), name)
			}
		}
	}
	walk(member)
}

// vocabulary is the set of strings a schema admits, and whether that set is
// closed. Every arm that always applies narrows it (intersection); an
// alternative group admits the union of its arms, and is open when any arm is.
// A oneOf group is read as that union only when no string can match two of
// its arms; otherwise it is refused, since oneOf rejects such a string.
// A schema whose type excludes string admits none. Keywords whose effect on
// the set this reader does not model fail the test.
func (s *schemaSet) vocabulary(r schemaRef) (map[string]bool, bool) {
	r = s.resolve(r, nil)
	for _, unsupported := range []string{"not", "if", "then", "else", "dependentSchemas"} {
		if _, ok := r.loc.node[unsupported]; ok {
			s.fatalf("%s: the drift enum reader does not model %q", r.loc.key(), unsupported)
		}
	}
	var values map[string]bool
	closed := false
	narrow := func(admitted map[string]bool) {
		if !closed {
			values, closed = admitted, true
			return
		}
		kept := map[string]bool{}
		for value := range values {
			if admitted[value] {
				kept[value] = true
			}
		}
		values = kept
	}
	if list, ok := r.loc.node["enum"].([]any); ok {
		admitted := map[string]bool{}
		for _, value := range list {
			if text, ok := value.(string); ok {
				admitted[text] = true
			}
		}
		narrow(admitted)
	}
	if value, ok := r.loc.node["const"]; ok {
		admitted := map[string]bool{}
		if text, ok := value.(string); ok {
			admitted[text] = true
		}
		narrow(admitted)
	}
	if kind, ok := r.loc.node["type"]; ok && !typeAdmits(kind, "string") {
		narrow(map[string]bool{})
	}
	all, alternatives, _ := s.arms(r)
	for _, arm := range all {
		if admitted, armClosed := s.vocabulary(arm); armClosed {
			narrow(admitted)
		}
	}
	for _, group := range alternatives {
		exclusive := r.composed == nil && len(group) > 0 && path.Base(path.Dir(group[0].loc.pointer)) == "oneOf"
		union := map[string]bool{}
		owner := map[string]string{}
		open, admitsSome := "", ""
		for _, arm := range group {
			admitted, armClosed := s.vocabulary(arm)
			if exclusive && (!armClosed || len(admitted) > 0) {
				// An open arm may admit any string, so beside any other arm that
				// admits one, a value could match both and oneOf would reject it.
				if admitsSome != "" && (open != "" || !armClosed) {
					s.fatalf("%s: the drift enum reader does not model oneOf arms that may both admit a string (%s and %s)",
						r.loc.key(), admitsSome, arm.loc.key())
				}
				admitsSome = arm.loc.key()
			}
			if !armClosed {
				open = arm.loc.key()
				if !exclusive {
					break
				}
				continue
			}
			for value := range admitted {
				if other, taken := owner[value]; taken && exclusive {
					s.fatalf("%s: the drift enum reader does not model oneOf arms that both admit %q (%s and %s)",
						r.loc.key(), value, other, arm.loc.key())
				}
				owner[value] = arm.loc.key()
				union[value] = true
			}
		}
		if open == "" {
			narrow(union)
		}
	}
	return values, closed
}

func typeAdmits(kind any, name string) bool {
	switch value := kind.(type) {
	case string:
		return value == name
	case []any:
		for _, item := range value {
			if item == name {
				return true
			}
		}
		return false
	}
	return true
}

// publishedObjectSchemas lists every component and every inline request or
// response body in the protocol files that declares an object shape.
func (s *schemaSet) publishedObjectSchemas() []schemaLocation {
	var out []schemaLocation
	add := func(l schemaLocation) {
		if l.node == nil {
			return
		}
		if pureReference(l.node) && !strings.Contains(l.pointer, "/components/") {
			return // an inline body that points at a component is covered by that component
		}
		if s.shape(s.root(l.key()), nil).isObject() {
			out = append(out, l)
		}
	}
	for _, name := range []string{commonDoc, agentDoc, clientDoc, l3Doc} {
		document, _ := s.document(name).(map[string]any)
		if components, ok := document["components"].(map[string]any); ok {
			if schemas, ok := components["schemas"].(map[string]any); ok {
				for schemaName := range schemas {
					add(s.location(component(name, schemaName)))
				}
			}
		}
		paths, _ := document["paths"].(map[string]any)
		for route, value := range paths {
			operations, _ := value.(map[string]any)
			for method, operationValue := range operations {
				operation, ok := operationValue.(map[string]any)
				if !ok {
					continue
				}
				if _, ok := operation["requestBody"]; ok {
					add(s.location(requestBody(name, method, route)))
				}
				responses, _ := operation["responses"].(map[string]any)
				for status := range responses {
					add(s.location(responseBody(name, method, route, status)))
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// ---- Go side ----

type goField struct {
	name      string
	typ       reflect.Type
	omittable bool
}

// jsonFields lists the fields encoding/json writes for t, following embedded
// structs; a shallower field shadows a deeper one of the same name.
func jsonFields(t reflect.Type) []goField {
	var out []goField
	seen := map[string]bool{}
	level := []reflect.Type{t}
	for len(level) > 0 {
		var next []reflect.Type
		var found []goField
		for _, current := range level {
			for index := range current.NumField() {
				field := current.Field(index)
				tag := field.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, options, _ := strings.Cut(tag, ",")
				fieldType := field.Type
				if field.Anonymous && name == "" {
					embedded := fieldType
					if embedded.Kind() == reflect.Pointer {
						embedded = embedded.Elem()
					}
					if embedded.Kind() == reflect.Struct {
						next = append(next, embedded)
						continue
					}
				}
				if !field.IsExported() {
					continue
				}
				if name == "" {
					name = field.Name
				}
				found = append(found, goField{name: name, typ: fieldType, omittable: omittable(fieldType, options)})
			}
		}
		for _, field := range found {
			if !seen[field.name] {
				seen[field.name] = true
				out = append(out, field)
			}
		}
		level = next
	}
	return out
}

func omittable(t reflect.Type, options string) bool {
	for _, option := range strings.Split(options, ",") {
		switch option {
		case "omitzero":
			return true
		case "omitempty":
			switch t.Kind() {
			case reflect.Struct:
			case reflect.Array:
				if t.Len() == 0 {
					return true
				}
			default:
				return true
			}
		}
	}
	return false
}

var (
	jsonMarshaler = reflect.TypeFor[json.Marshaler]()
	textMarshaler = reflect.TypeFor[encoding.TextMarshaler]()
)

// opaque types write themselves; their struct tags do not describe the wire.
func opaque(t reflect.Type) bool {
	return t == reflect.TypeFor[time.Time]() || t == reflect.TypeFor[json.RawMessage]() ||
		t.Implements(jsonMarshaler) || reflect.PointerTo(t).Implements(jsonMarshaler) ||
		t.Implements(textMarshaler) || reflect.PointerTo(t).Implements(textMarshaler)
}

// ---- comparison ----

type driftChecker struct {
	set                   *schemaSet
	consts                goConstants
	done                  map[string]bool
	compared              map[string]bool
	seen                  map[string]bool
	problems              []string
	usedRequiredOmittable map[string]bool
	usedSchemaOnly        map[string]bool
	usedEnumExceptions    map[string]bool
	usedGoOnly            map[string]bool
}

func (c *driftChecker) problem(format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, args...))
}

func (c *driftChecker) visit(l schemaLocation) { c.seen[l.key()] = true }

func (c *driftChecker) use(table *map[string]bool, key string) {
	if *table == nil {
		*table = map[string]bool{}
	}
	(*table)[key] = true
}

func (c *driftChecker) compare(where string, t reflect.Type, r schemaRef) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.String {
		c.compareEnum(where, t, r)
		return
	}
	if opaque(t) {
		return
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return // base64 bytes
		}
		if items, ok := c.set.member(r, "items"); ok {
			c.compare(where+"[]", t.Elem(), items)
		}
		return
	case reflect.Map:
		if values, ok := c.set.member(r, "additionalProperties"); ok {
			c.compare(where+"{}", t.Elem(), values)
		}
		return
	case reflect.Struct:
	default:
		return
	}

	shape := c.set.shape(r, c.visit)
	resolved := c.set.resolve(r, nil)
	pair := t.String() + "@" + resolved.loc.key()
	c.compared[pair] = true
	// The same location can mean different schemas under different dynamic
	// scopes, so a pair is done only for the scope it was compared in.
	scoped := t.String() + "@" + identity(resolved)
	if c.done[scoped] {
		return
	}
	c.done[scoped] = true
	if !shape.isObject() {
		if !c.set.declaresFreeFormObject(resolved) {
			c.problem("%s: schema %s gives Go %s no object shape and does not declare a free-form object",
				where, resolved.loc.key(), t)
		}
		return
	}

	fields := jsonFields(t)
	byName := make(map[string]goField, len(fields))
	for _, field := range fields {
		byName[field.name] = field
	}
	for _, field := range fields {
		if _, declared := shape.properties[field.name]; !declared && shape.closed {
			key := t.String() + "." + field.name + "@" + resolved.loc.key()
			if _, allowed := driftGoOnly[key]; allowed {
				c.use(&c.usedGoOnly, key)
				continue
			}
			c.problem("%s: Go %s writes %q, which the closed schema %s does not declare",
				where, t, field.name, resolved.loc.key())
		}
	}
	for name := range shape.required {
		field, present := byName[name]
		key := t.String() + "." + name
		switch {
		case !present:
			c.problem("%s: schema %s requires %q, which Go %s never writes", where, resolved.loc.key(), name, t)
		case field.omittable:
			if _, allowed := driftRequiredOmittable[key]; allowed {
				c.use(&c.usedRequiredOmittable, key)
				continue
			}
			c.problem("%s: schema %s requires %q, but Go %s may omit it (omitempty/omitzero)",
				where, resolved.loc.key(), name, t)
		}
	}
	names := make([]string, 0, len(shape.properties))
	for name := range shape.properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field, present := byName[name]
		if !present {
			key := t.String() + "." + name
			if _, allowed := driftSchemaOnly[key]; allowed {
				c.use(&c.usedSchemaOnly, key)
				continue
			}
			c.problem("%s: schema %s declares %q, which Go %s does not carry", where, resolved.loc.key(), name, t)
			continue
		}
		member, ok := c.set.member(r, "properties", name)
		if !ok {
			c.set.fatalf("%s: property %q was found in the shape but has no member schema", resolved.loc.key(), name)
		}
		c.compare(where+"."+name, field.typ, member)
	}
}

// compareEnum holds a Go closed vocabulary (a named string type with
// constants) to the schema at that location. A schema that publishes no enum
// there is drift too: it admits values the server never writes or refuses.
func (c *driftChecker) compareEnum(where string, t reflect.Type, r schemaRef) {
	if t.PkgPath() == "" {
		return // a plain string has no closed vocabulary to compare
	}
	vocabulary := c.consts.typed[t.PkgPath()+"."+t.Name()]
	if len(vocabulary) == 0 {
		return
	}
	resolved := c.set.resolve(r, nil)
	key := t.String() + "@" + resolved.loc.key()
	published, closed := c.set.vocabulary(r)
	if exception, allowed := driftEnumExceptions[key]; allowed {
		c.use(&c.usedEnumExceptions, key)
		if exception.subset == nil {
			if closed {
				c.problem("%s: schema %s is listed as deliberately open but publishes enum %v",
					where, resolved.loc.key(), sortedKeys(published))
			}
			return
		}
		subset := map[string]bool{}
		for _, value := range exception.subset {
			if !vocabulary[value] {
				c.problem("%s: the allowed subset for %s names %q, which Go %s does not define", where, key, value, t)
			}
			subset[value] = true
		}
		if !closed {
			c.problem("%s: schema %s publishes no enum; it must admit exactly %v", where, resolved.loc.key(), exception.subset)
			return
		}
		if missing, extra := setDifference(subset, published); len(missing)+len(extra) > 0 {
			c.problem("%s: schema %s enum differs from the allowed subset: subset-only %v, schema-only %v",
				where, resolved.loc.key(), missing, extra)
		}
		return
	}
	if !closed {
		c.problem("%s: Go %s is a closed vocabulary %v, but schema %s publishes no enum",
			where, t, sortedKeys(vocabulary), resolved.loc.key())
		return
	}
	if missing, extra := setDifference(vocabulary, published); len(missing)+len(extra) > 0 {
		c.problem("%s: Go %s vocabulary and schema %s enum differ: Go-only %v, schema-only %v",
			where, t, resolved.loc.key(), missing, extra)
	}
}

func scopeKey(scope []schemaLocation) string {
	keys := make([]string, len(scope))
	for index, resource := range scope {
		keys[index] = resource.key()
	}
	return strings.Join(keys, ",")
}

func sortedKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func setDifference(left, right map[string]bool) (leftOnly, rightOnly []string) {
	for value := range left {
		if !right[value] {
			leftOnly = append(leftOnly, value)
		}
	}
	for value := range right {
		if !left[value] {
			rightOnly = append(rightOnly, value)
		}
	}
	sort.Strings(leftOnly)
	sort.Strings(rightOnly)
	return leftOnly, rightOnly
}

// ---- Go constants ----

const modulePath = "github.com/Derek-X-Wang/wefty/"

type goConstants struct {
	typed map[string]map[string]bool // "<pkg path>.<type>" -> values
	named map[string]string          // "<dir>.<const name>" -> value
}

// loadGoConstants reads the string constants of the wire packages from
// source; reflection cannot enumerate a type's constants.
func loadGoConstants(t *testing.T) goConstants {
	t.Helper()
	constants := goConstants{typed: map[string]map[string]bool{}, named: map[string]string{}}
	for _, dir := range []string{"l1", "l3", "contract"} {
		for _, file := range parseSources(t, dir) {
			for _, declaration := range file.Decls {
				general, ok := declaration.(*ast.GenDecl)
				if !ok || general.Tok != token.CONST {
					continue
				}
				for _, spec := range general.Specs {
					valueSpec := spec.(*ast.ValueSpec)
					for index, name := range valueSpec.Names {
						if index >= len(valueSpec.Values) {
							continue
						}
						typeName := ""
						if ident, ok := valueSpec.Type.(*ast.Ident); ok {
							typeName = ident.Name
						}
						expression := valueSpec.Values[index]
						if call, ok := expression.(*ast.CallExpr); ok && len(call.Args) == 1 {
							if ident, ok := call.Fun.(*ast.Ident); ok && typeName == "" {
								typeName = ident.Name
								expression = call.Args[0]
							}
						}
						literal, ok := expression.(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							continue
						}
						value, err := strconv.Unquote(literal.Value)
						if err != nil {
							t.Fatal(err)
						}
						constants.named[dir+"."+name.Name] = value
						if typeName != "" && typeName != "string" {
							key := modulePath + dir + "." + typeName
							if constants.typed[key] == nil {
								constants.typed[key] = map[string]bool{}
							}
							constants.typed[key][value] = true
						}
					}
				}
			}
		}
	}
	return constants
}

func parseSources(t *testing.T, dir string) []*ast.File {
	t.Helper()
	fileSet := token.NewFileSet()
	matches, err := filepath.Glob(filepath.Join("..", "..", dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, match := range matches {
		if strings.HasSuffix(match, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, match, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	return files
}

// methodGuard reads the fail-closed method guard used by a handler registered
// without a method-bearing mux pattern. The first statement must return for
// every method other than the listed alternatives.
func methodGuard(fn *ast.FuncDecl) ([]string, error) {
	if fn == nil || fn.Body == nil || len(fn.Body.List) == 0 {
		return nil, fmt.Errorf("has no first-statement method guard")
	}
	guard, ok := fn.Body.List[0].(*ast.IfStmt)
	if !ok {
		return nil, fmt.Errorf("first statement is not a method guard")
	}
	if guard.Init != nil || guard.Else != nil {
		return nil, fmt.Errorf("method guard has an init or else branch")
	}
	if len(guard.Body.List) == 0 {
		return nil, fmt.Errorf("method guard body is empty")
	}
	if _, ok := guard.Body.List[len(guard.Body.List)-1].(*ast.ReturnStmt); !ok {
		return nil, fmt.Errorf("method guard does not end in return")
	}

	httpMethods := map[string]string{
		"MethodConnect": "CONNECT",
		"MethodDelete":  "DELETE",
		"MethodGet":     "GET",
		"MethodHead":    "HEAD",
		"MethodOptions": "OPTIONS",
		"MethodPatch":   "PATCH",
		"MethodPost":    "POST",
		"MethodPut":     "PUT",
		"MethodTrace":   "TRACE",
	}
	request := ""
	accepted := map[string]bool{}
	var read func(ast.Expr) error
	read = func(expression ast.Expr) error {
		if parenthesized, ok := expression.(*ast.ParenExpr); ok {
			return read(parenthesized.X)
		}
		binary, ok := expression.(*ast.BinaryExpr)
		if !ok {
			return fmt.Errorf("method guard condition contains %T", expression)
		}
		if binary.Op == token.LAND {
			if err := read(binary.X); err != nil {
				return err
			}
			return read(binary.Y)
		}
		if binary.Op != token.NEQ {
			return fmt.Errorf("method guard comparison uses %s instead of !=", binary.Op)
		}
		left, ok := binary.X.(*ast.SelectorExpr)
		if !ok || left.Sel.Name != "Method" {
			return fmt.Errorf("method guard comparison does not read a request Method")
		}
		receiver, ok := left.X.(*ast.Ident)
		if !ok {
			return fmt.Errorf("method guard request is not an identifier")
		}
		if request == "" {
			request = receiver.Name
		} else if request != receiver.Name {
			return fmt.Errorf("method guard compares more than one request")
		}

		var method string
		switch value := binary.Y.(type) {
		case *ast.SelectorExpr:
			pkg, ok := value.X.(*ast.Ident)
			if !ok || pkg.Name != "http" {
				return fmt.Errorf("method guard comparison is not against net/http")
			}
			method, ok = httpMethods[value.Sel.Name]
			if !ok {
				return fmt.Errorf("method guard uses unknown http.%s", value.Sel.Name)
			}
		case *ast.BasicLit:
			if value.Kind != token.STRING {
				return fmt.Errorf("method guard comparison uses a non-string literal")
			}
			var err error
			method, err = strconv.Unquote(value.Value)
			if err != nil || method == "" {
				return fmt.Errorf("method guard comparison has an invalid string literal")
			}
		default:
			return fmt.Errorf("method guard comparison uses %T", binary.Y)
		}
		accepted[method] = true
		return nil
	}
	if err := read(guard.Cond); err != nil {
		return nil, err
	}
	methods := sortedKeys(accepted)
	if len(methods) == 0 {
		return nil, fmt.Errorf("method guard accepts no methods")
	}
	return methods, nil
}

// validatorCases returns the string values a validator's switch accepts.
func validatorCases(t *testing.T, dir, function string, constants goConstants) map[string]bool {
	t.Helper()
	for _, file := range parseSources(t, dir) {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function {
				continue
			}
			values := map[string]bool{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				clause, ok := node.(*ast.CaseClause)
				if !ok {
					return true
				}
				for _, expression := range clause.List {
					switch value := expression.(type) {
					case *ast.BasicLit:
						text, err := strconv.Unquote(value.Value)
						if err != nil {
							t.Fatal(err)
						}
						values[text] = true
					case *ast.Ident:
						resolved, ok := constants.named[dir+"."+value.Name]
						if !ok {
							t.Fatalf("%s.%s: cannot resolve case %s", dir, function, value.Name)
						}
						values[resolved] = true
					case *ast.SelectorExpr:
						pkg, _ := value.X.(*ast.Ident)
						if pkg == nil {
							t.Fatalf("%s.%s: unsupported case expression", dir, function)
						}
						resolved, ok := constants.named[pkg.Name+"."+value.Sel.Name]
						if !ok {
							t.Fatalf("%s.%s: cannot resolve case %s.%s", dir, function, pkg.Name, value.Sel.Name)
						}
						values[resolved] = true
					default:
						t.Fatalf("%s.%s: unsupported case expression %T", dir, function, expression)
					}
				}
				return true
			})
			if len(values) == 0 {
				t.Fatalf("%s.%s has no switch cases", dir, function)
			}
			return values
		}
	}
	t.Fatalf("validator %s.%s not found", dir, function)
	return nil
}
