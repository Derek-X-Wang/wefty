// --------------------------------------------------------------------------
// Inline run-mailbox writer
//
// This workflow reports by writing event files into $WEFTY_RUN_DIR, the run
// mailbox; the node agent publishes them to the run ledger with the credential
// it already holds. The writer is inlined so the workflow depends on no
// package. Protocol: docs/contracts/run-execution-context.md, "Run mailbox".
//
// For the same call it writes the same bytes as the `wefty run` subcommands:
// the same headers in the same order, the same folding, the same payload
// format and the same bounds. Only the created-at second differs, because the
// two ran at different times. wefty's conformance tests hold that claim
// (agent/run_mailbox_helper_conformance_test.go).
//
// Like `wefty run`, it refuses what the agent would refuse, creates each file
// exclusively with mode 0600 (never following a planted link), flushes it
// before the rename, and refuses a tmp/ or events/ that is not a real
// directory. What it cannot do is resolve every path under one opened root:
// Node has no openat, so a directory swapped between the check and the open is
// not caught. A workflow that has to defend its mailbox against a hostile
// co-tenant should report through `wefty run` instead.
//
// This is erasable TypeScript only -- no enums, namespaces or parameter
// properties -- so the conformance test can run it with Node's type stripping.
// --------------------------------------------------------------------------

import { closeSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

type EnvelopeStatus = "succeeded" | "failed" | "partial";
type StepStatus = "started" | "ended";
type GateOutcome = "pass" | "fail" | "error" | "skipped";
type EventKind = "envelope" | "step" | "gate" | "result";
type PayloadFormat = "text" | "json";

interface MailboxEvent {
  kind: EventKind;
  name: string;
  step: string;
  status: string;
  outcome: string;
  summary: string;
  key: string;
  payload: Uint8Array;
  payloadFormat: PayloadFormat;
}

// The protocol's bounds, as `wefty run` applies them. Past the identifier
// bound the agent would rewrite a name, and a rewritten key is a different
// idempotency identity, so these are refused rather than trimmed. A payload is
// truncated instead: a truncated payload still carries the verdict.
const WEFTY_MAX_EVENT_BYTES = 64 << 10;
const WEFTY_MAX_PAYLOAD_BYTES = 56 << 10;
const WEFTY_MAX_IDENTIFIER_BYTES = 255;
const WEFTY_MAX_SUMMARY_BYTES = 2048;
const WEFTY_MAX_RESULT_BYTES = 64 << 20;
const WEFTY_MAX_PARAMS_BYTES = 64 << 10;
const WEFTY_MAX_EVENT_NAME_BYTES = 128;
const WEFTY_MAX_SLUG_BYTES = 32;
const WEFTY_TRUNCATION_NOTICE =
  "\n[truncated by the inline run-mailbox writer: the payload exceeded the run mailbox event bound]\n";

const WEFTY_KINDS: readonly string[] = ["envelope", "step", "gate", "result"];
const WEFTY_ENVELOPE_STATUSES: readonly string[] = ["succeeded", "failed", "partial"];
const WEFTY_STEP_STATUSES: readonly string[] = ["started", "ended"];
const WEFTY_GATE_OUTCOMES: readonly string[] = ["pass", "fail", "error", "skipped"];

let weftySequence = 0;
let weftyLastStamp = 0n;

// The directory every report goes to. Its absence is a refusal, not a silent
// no-op: a workflow that believes it reported and did not is the failure the
// mailbox exists to prevent.
function weftyRunDir(): string {
  const directory = (process.env["WEFTY_RUN_DIR"] ?? "").trim();
  if (directory === "") {
    throw new Error(
      "WEFTY_RUN_DIR is not set: this job has no run mailbox to report through. " +
        "The mailbox is delivered to every one-shot L3 dispatched. " +
        'See docs/contracts/run-execution-context.md, "Run mailbox"',
    );
  }
  return directory;
}

// Fold a value onto the one line a header can carry, byte for byte as
// `wefty run` does: newlines and tabs become spaces, every other control byte
// is dropped, and surrounding spaces are trimmed.
function weftyHeaderValue(value: string): string {
  const folded: number[] = [];
  for (const byte of Buffer.from(value, "utf8")) {
    if (byte === 0x0a || byte === 0x0d || byte === 0x09) {
      folded.push(0x20);
    } else if (byte >= 0x20 && byte !== 0x7f) {
      folded.push(byte);
    }
  }
  let start = 0;
  let end = folded.length;
  while (start < end && folded[start] === 0x20) start++;
  while (end > start && folded[end - 1] === 0x20) end--;
  return Buffer.from(folded.slice(start, end)).toString("utf8");
}

function weftyIsJSON(payload: Uint8Array): boolean {
  try {
    JSON.parse(Buffer.from(payload).toString("utf8"));
    return true;
  } catch {
    return false;
  }
}

// Reject what the agent's parser would refuse, where the message can still
// name the problem. The parser stays the authority; this is the same rule
// stated early.
function weftyValidate(input: MailboxEvent): MailboxEvent {
  const event: MailboxEvent = {
    ...input,
    name: weftyHeaderValue(input.name),
    step: weftyHeaderValue(input.step),
    summary: weftyHeaderValue(input.summary),
    key: weftyHeaderValue(input.key),
  };
  if (!WEFTY_KINDS.includes(event.kind)) {
    throw new Error(`kind "${event.kind}" is not one of ${WEFTY_KINDS.join(", ")}`);
  }
  switch (event.kind) {
    case "gate":
      if (event.name === "") throw new Error("a gate requires a name");
      if (!WEFTY_GATE_OUTCOMES.includes(event.outcome)) {
        throw new Error(`gate outcome "${event.outcome}" is not one of ${WEFTY_GATE_OUTCOMES.join(", ")}`);
      }
      break;
    case "step":
      if (event.name === "") throw new Error("a step requires a name");
      if (event.status === "") event.status = "started";
      if (!WEFTY_STEP_STATUSES.includes(event.status)) {
        throw new Error(`step status "${event.status}" is not one of ${WEFTY_STEP_STATUSES.join(", ")}`);
      }
      break;
    default:
      if (event.kind === "envelope" && event.step === "") throw new Error("an envelope requires a step");
      if (event.status === "") event.status = "succeeded";
      if (!WEFTY_ENVELOPE_STATUSES.includes(event.status)) {
        throw new Error(`status "${event.status}" is not one of ${WEFTY_ENVELOPE_STATUSES.join(", ")}`);
      }
  }
  if (event.outcome !== "" && event.kind !== "gate") {
    throw new Error(`kind "${event.kind}" does not carry an outcome`);
  }
  const bounds: [string, string, number][] = [
    ["step", event.step, WEFTY_MAX_IDENTIFIER_BYTES],
    ["name", event.name, WEFTY_MAX_IDENTIFIER_BYTES],
    ["key", event.key, WEFTY_MAX_IDENTIFIER_BYTES],
    ["summary", event.summary, WEFTY_MAX_SUMMARY_BYTES],
  ];
  for (const [field, value, limit] of bounds) {
    const size = Buffer.byteLength(value, "utf8");
    if (size > limit) {
      throw new Error(`${field} is ${size} bytes; the run mailbox bounds it at ${limit}`);
    }
  }
  if (event.payload.length > WEFTY_MAX_PAYLOAD_BYTES) {
    event.payload = Buffer.concat([
      Buffer.from(event.payload.subarray(0, WEFTY_MAX_PAYLOAD_BYTES)),
      Buffer.from(WEFTY_TRUNCATION_NOTICE, "utf8"),
    ]);
    // A truncated JSON document is no longer JSON, and the agent refuses a
    // json payload it cannot decode -- the whole event, verdict included.
    event.payloadFormat = "text";
  }
  if (event.payloadFormat === "json" && !weftyIsJSON(event.payload)) {
    throw new Error("the payload is marked json but is not a JSON document");
  }
  return event;
}

// The protocol's line-oriented header block, a "--" separator and the raw
// payload, never escaped. The header order is fixed, which is what lets two
// independent producers write the same bytes.
function weftyEncode(event: MailboxEvent, createdAt: Date): Buffer {
  let header = `wefty-protocol: 1\nkind: ${event.kind}\n`;
  const optional: [string, string][] = [
    ["name", event.name],
    ["step", event.step],
    ["status", event.status],
    ["outcome", event.outcome],
    ["summary", event.summary],
    ["key", event.key],
  ];
  for (const [name, value] of optional) {
    if (value !== "") header += `${name}: ${value}\n`;
  }
  const created = createdAt.toISOString().replace(/\.\d{3}Z$/, "Z");
  header += `payload: ${event.payloadFormat}\ncreated-at: ${created}\n--\n`;
  return Buffer.concat([Buffer.from(header, "utf8"), event.payload]);
}

// Nanoseconds since the epoch. The agent publishes a sweep in the lexical
// order of the file names, so the name is the only ordering signal a producer
// has. Node's wall clock reads milliseconds; the stamp is forced to increase
// within this process, so two events written back to back keep their order.
function weftyNanos(): bigint {
  let stamp = BigInt(Date.now()) * 1_000_000n;
  if (stamp <= weftyLastStamp) stamp = weftyLastStamp + 1n;
  weftyLastStamp = stamp;
  return stamp;
}

function weftySlug(value: string): string {
  const slug: number[] = [];
  for (const byte of Buffer.from(value, "utf8")) {
    const allowed =
      (byte >= 0x41 && byte <= 0x5a) ||
      (byte >= 0x61 && byte <= 0x7a) ||
      (byte >= 0x30 && byte <= 0x39) ||
      byte === 0x2e ||
      byte === 0x5f ||
      byte === 0x2d;
    slug.push(allowed ? byte : 0x2d);
  }
  const text = Buffer.from(slug.slice(0, WEFTY_MAX_SLUG_BYTES)).toString("latin1");
  return text === "" ? "event" : text;
}

function weftyEventFileName(event: MailboxEvent): string {
  weftySequence += 1;
  const name = [
    weftyNanos().toString().padStart(19, "0"),
    (weftySequence % 10000).toString().padStart(4, "0"),
    event.kind,
    weftySlug(event.name || event.step || event.kind),
    process.pid.toString(16).padStart(8, "0"),
  ].join("-");
  return name.slice(0, WEFTY_MAX_EVENT_NAME_BYTES);
}

// A mailbox subdirectory must be a real directory. lstat never follows a
// link, so a planted symlink is refused rather than written through.
function weftyEnsureDirectory(parent: string, name: string): string {
  const path = join(parent, name);
  try {
    mkdirSync(path, { mode: 0o700 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  if (!lstatSync(path).isDirectory()) {
    throw new Error(`${name} in ${parent} is not a directory; the run mailbox layout is fixed`);
  }
  return path;
}

// Stage a fresh 0600 file, flush it, then rename it into place. "wx" is
// O_CREAT|O_EXCL: it never reuses an existing file and never follows a link.
function weftyWriteExclusive(staged: string, published: string, content: Uint8Array): void {
  const descriptor = openSync(staged, "wx", 0o600);
  try {
    writeFileSync(descriptor, content);
    fsyncSync(descriptor);
  } catch (error) {
    closeSync(descriptor);
    rmSync(staged, { force: true });
    throw error;
  }
  closeSync(descriptor);
  try {
    renameSync(staged, published);
  } catch (error) {
    rmSync(staged, { force: true });
    throw error;
  }
}

// Flush the rename itself, best effort: not every platform lets a directory
// be opened for sync, and a published but unsynced event beats none.
function weftySyncDirectory(path: string): void {
  try {
    const descriptor = openSync(path, "r");
    try {
      fsyncSync(descriptor);
    } finally {
      closeSync(descriptor);
    }
  } catch {
    // best effort
  }
}

// Write one event and return the published path. The rename into events/ is
// the only "done writing" signal the protocol has.
function weftyWriteEvent(input: MailboxEvent): string {
  const runDir = weftyRunDir();
  const event = weftyValidate(input);
  const document = weftyEncode(event, new Date());
  if (document.length > WEFTY_MAX_EVENT_BYTES) {
    throw new Error(
      `this event encodes to ${document.length} bytes; the run mailbox bounds one event at ${WEFTY_MAX_EVENT_BYTES}`,
    );
  }
  mkdirSync(runDir, { recursive: true, mode: 0o700 });
  const staging = weftyEnsureDirectory(runDir, "tmp");
  const events = weftyEnsureDirectory(runDir, "events");
  const name = weftyEventFileName(event);
  const published = join(events, name);
  weftyWriteExclusive(join(staging, name), published, document);
  weftySyncDirectory(events);
  return published;
}

function weftyEvent(kind: EventKind, fields: Partial<Omit<MailboxEvent, "kind">>): MailboxEvent {
  return {
    kind,
    name: "",
    step: "",
    status: "",
    outcome: "",
    summary: "",
    key: "",
    payload: new Uint8Array(),
    payloadFormat: "text",
    ...fields,
  };
}

// ---- The reporting API this workflow calls --------------------------------

interface ReportOptions {
  // One line of free text; newlines and tabs are folded to spaces.
  summary?: string;
  // The event's idempotency identity; the agent defaults it to the file name.
  key?: string;
}

// reportStep brackets a unit of work: "started" before it, "ended" after it.
function reportStep(name: string, status: StepStatus, options: ReportOptions = {}): string {
  return weftyWriteEvent(weftyEvent("step", { name, status, summary: options.summary ?? "", key: options.key ?? "" }));
}

// reportEnvelope records a step's outcome. A string detail is carried as text;
// an object is carried as a JSON document, and must fit in one event whole.
function reportEnvelope(
  step: string,
  status: EnvelopeStatus,
  options: ReportOptions & { detail?: string | object } = {},
): string {
  let payload: Uint8Array = new Uint8Array();
  let payloadFormat: PayloadFormat = "text";
  if (typeof options.detail === "string") {
    payload = Buffer.from(options.detail, "utf8");
  } else if (options.detail !== undefined) {
    payload = Buffer.from(JSON.stringify(options.detail), "utf8");
    payloadFormat = "json";
    if (payload.length > WEFTY_MAX_PAYLOAD_BYTES) {
      throw new Error(
        `the JSON detail is ${payload.length} bytes; a JSON payload must fit in one run mailbox event (${WEFTY_MAX_PAYLOAD_BYTES})`,
      );
    }
  }
  return weftyWriteEvent(
    weftyEvent("envelope", {
      step,
      status,
      summary: options.summary ?? "",
      key: options.key ?? "",
      payload,
      payloadFormat,
    }),
  );
}

// reportGate records a verdict, with optional evidence as text.
function reportGate(
  name: string,
  outcome: GateOutcome,
  options: ReportOptions & { evidence?: string } = {},
): string {
  return weftyWriteEvent(
    weftyEvent("gate", {
      name,
      outcome,
      summary: options.summary ?? "",
      key: options.key ?? "",
      payload: Buffer.from(options.evidence ?? "", "utf8"),
    }),
  );
}

// reportResult records the run's final document twice: whole, as result.json
// in $WEFTY_HANDOFF_DIR (what an operator reads, and what the node uploads),
// and as a result event, whose payload is bounded like any other. A string is
// taken as the document verbatim; anything else is serialized as JSON.
function reportResult(document: unknown, status: EnvelopeStatus, options: ReportOptions = {}): string {
  const text = typeof document === "string" ? document : JSON.stringify(document);
  if (text === undefined) throw new Error("the result document does not serialize to JSON");
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length > WEFTY_MAX_RESULT_BYTES) {
    throw new Error(
      `the result document is ${bytes.length} bytes; one that size belongs in an artifact, not in the handoff directory`,
    );
  }
  const handoff = (process.env["WEFTY_HANDOFF_DIR"] ?? "").trim();
  if (handoff !== "") {
    // A failed handoff copy must not lose the event: the ledger entry is the
    // copy that survives the handoff directory being removed.
    try {
      mkdirSync(handoff, { recursive: true, mode: 0o700 });
      const staged = join(handoff, `.result.json.${process.pid}.${weftyNanos()}`);
      weftyWriteExclusive(staged, join(handoff, "result.json"), bytes);
      weftySyncDirectory(handoff);
    } catch (error) {
      console.error(`could not publish result.json into ${handoff}: ${String(error)}`);
    }
  }
  const truncated = bytes.length > WEFTY_MAX_PAYLOAD_BYTES;
  return weftyWriteEvent(
    weftyEvent("result", {
      status,
      summary: options.summary ?? "",
      key: options.key ?? "",
      payload: bytes,
      payloadFormat: !truncated && weftyIsJSON(bytes) ? "json" : "text",
    }),
  );
}

// runParams returns the params the run was submitted with. A job never
// receives them in the environment: the agent writes params.json into the
// mailbox. A run submitted without params has none, which is not an error.
function runParams(): Record<string, unknown> {
  let raw: Buffer;
  try {
    raw = readFileSync(join(weftyRunDir(), "params.json"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
  if (raw.length > WEFTY_MAX_PARAMS_BYTES) {
    throw new Error(`params.json is larger than the ${WEFTY_MAX_PARAMS_BYTES} byte params bound`);
  }
  const params: unknown = JSON.parse(raw.toString("utf8"));
  if (params === null || typeof params !== "object" || Array.isArray(params)) {
    throw new Error("params.json is not a JSON object");
  }
  return params as Record<string, unknown>;
}

// runParam reads one param the way `wefty run params NAME` prints it: a string
// as its text, anything else as its JSON, and an absent param as "".
function runParam(name: string): string {
  const value = runParams()[name];
  if (value === undefined) return "";
  return typeof value === "string" ? value : JSON.stringify(value);
}
