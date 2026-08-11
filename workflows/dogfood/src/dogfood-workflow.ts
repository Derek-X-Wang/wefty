import { createHash } from "node:crypto";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import type * as Sandcastle from "@ai-hero/sandcastle";
import type * as NoSandbox from "@ai-hero/sandcastle/sandboxes/no-sandbox";

type Step = "plan" | "implement" | "review";

interface Context {
  runId: string;
  l3Endpoint: string;
  runToken: string;
  handoffDir: string;
}

interface DogfoodParams {
  step?: Step;
  task: string;
  repo_path: string;
  branch?: string;
  base_branch?: string;
  plan?: string;
  claude_model?: string;
  codex_model?: string;
  child_max_runtime_seconds?: number;
  workflow_package_path?: string;
}

interface RunRecord {
  run_id: string;
  status: string;
  params: DogfoodParams;
  tags?: string[];
}

interface RunAccepted {
  run_id: string;
  status_url: string;
  logs_url: string;
}

interface Artifact {
  name: string;
  uri: string;
  media_type?: string;
  sha256?: string;
}

class ReportedFailure extends Error {}

interface SandcastleRuntime {
  sandcastle: typeof Sandcastle;
  noSandbox: typeof NoSandbox.noSandbox;
}

async function loadSandcastle(params: DogfoodParams): Promise<SandcastleRuntime> {
  const packagePath = params.workflow_package_path ?? join(params.repo_path, "workflows", "dogfood");
  const sandcastlePackage = join(packagePath, "node_modules", "@ai-hero", "sandcastle", "dist");
  const sandcastlePath = join(sandcastlePackage, "index.js");
  const noSandboxPath = join(sandcastlePackage, "sandboxes", "no-sandbox.js");
  const [sandcastle, noSandboxModule] = await Promise.all([
    import(pathToFileURL(sandcastlePath).href) as Promise<typeof Sandcastle>,
    import(pathToFileURL(noSandboxPath).href) as Promise<typeof NoSandbox>,
  ]);
  return { sandcastle, noSandbox: noSandboxModule.noSandbox };
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function loadContext(): Context {
  const context = {
    runId: requiredEnvironment("WEFTY_RUN_ID"),
    l3Endpoint: requiredEnvironment("WEFTY_L3_ENDPOINT"),
    runToken: requiredEnvironment("WEFTY_RUN_TOKEN"),
    handoffDir: requiredEnvironment("WEFTY_HANDOFF_DIR"),
  };
  for (const [name, value] of [["WEFTY_L3_ENDPOINT", context.l3Endpoint]] as const) {
    const endpoint = new URL(value);
    if (endpoint.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname)) {
      throw new Error(`${name} must be a job-local loopback HTTP URL`);
    }
  }
  return context;
}

function apiURL(context: Context, path: string): string {
  return `${context.l3Endpoint.replace(/\/$/, "")}${path}`;
}

async function l3Request<T>(
  context: Context,
  path: string,
  init: RequestInit,
  acceptedStatuses: number[],
): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Authorization", `Bearer ${context.runToken}`);
  if (init.body !== undefined) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(apiURL(context, path), { ...init, headers });
  const body = await response.text();
  if (!acceptedStatuses.includes(response.status)) {
    throw new Error(`L3 ${init.method ?? "GET"} ${path} returned HTTP ${response.status}: ${body.slice(0, 1000)}`);
  }
  return JSON.parse(body) as T;
}

async function getRun(context: Context, runId = context.runId): Promise<RunRecord> {
  return l3Request<RunRecord>(context, `/v1/runs/${encodeURIComponent(runId)}`, { method: "GET" }, [200]);
}

function validateParams(params: DogfoodParams): DogfoodParams {
  if (!params || typeof params !== "object") {
    throw new Error("run params must be an object");
  }
  if (typeof params.task !== "string" || params.task.trim() === "") {
    throw new Error("params.task is required");
  }
  if (typeof params.repo_path !== "string" || params.repo_path.trim() === "") {
    throw new Error("params.repo_path is required");
  }
  if (params.step && !["plan", "implement", "review"].includes(params.step)) {
    throw new Error(`unknown dogfood step ${String(params.step)}`);
  }
  return params;
}

async function assertRepository(path: string): Promise<void> {
  const info = await stat(path);
  if (!info.isDirectory()) {
    throw new Error(`repo_path is not a directory: ${path}`);
  }
  const git = await stat(`${path}/.git`);
  if (!git.isDirectory() && !git.isFile()) {
    throw new Error(`repo_path is not a Git repository: ${path}`);
  }
}

function stepIdentity(context: Context, step: Step, kind: "envelope" | "gate"): string {
  return `${context.runId}-${step}-${kind}`;
}

async function appendEnvelope(
  context: Context,
  step: Step,
  status: "succeeded" | "failed" | "partial",
  summary: string,
  options: { artifacts?: Artifact[]; notes?: string; extensions?: Record<string, unknown> } = {},
): Promise<void> {
  await l3Request(
    context,
    `/v1/runs/${encodeURIComponent(context.runId)}/envelopes`,
    {
      method: "POST",
      body: JSON.stringify({
        schema_version: 1,
        envelope_id: stepIdentity(context, step, "envelope"),
        idempotency_key: stepIdentity(context, step, "envelope"),
        run_id: context.runId,
        step_id: step,
        status,
        summary,
        ...(options.artifacts ? { artifacts: options.artifacts } : {}),
        ...(options.notes ? { notes_for_next_agent: options.notes } : {}),
        extensions: { "dev.wefty.dogfood": options.extensions ?? {} },
        created_at: new Date().toISOString(),
      }),
    },
    [200, 201],
  );
}

async function appendGate(
  context: Context,
  step: Step,
  name: string,
  outcome: "pass" | "fail" | "error" | "skipped",
  evidence: Array<{ kind: string; value: string }>,
): Promise<void> {
  await l3Request(
    context,
    `/v1/runs/${encodeURIComponent(context.runId)}/gates`,
    {
      method: "POST",
      body: JSON.stringify({
        schema_version: 1,
        gate_id: stepIdentity(context, step, "gate"),
        idempotency_key: stepIdentity(context, step, "gate"),
        run_id: context.runId,
        step_id: step,
        name,
        outcome,
        evidence,
        evaluated_at: new Date().toISOString(),
      }),
    },
    [200, 201],
  );
}

async function reportFailure(context: Context, step: Step, error: unknown): Promise<void> {
  const summary = error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000);
  try {
    await appendEnvelope(context, step, "failed", `${step} failed: ${summary}`);
  } catch (appendError) {
    console.error(`failed to append ${step} failure envelope`, appendError);
  }
  try {
    await appendGate(context, step, `${step}-completed`, "error", [{ kind: "error", value: summary || "unknown error" }]);
  } catch (appendError) {
    console.error(`failed to append ${step} error gate`, appendError);
  }
}

function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}

async function dispatchChild(
  context: Context,
  record: RunRecord,
  step: "implement" | "review",
  params: DogfoodParams,
): Promise<RunAccepted> {
  const scriptPath = process.argv[1];
  if (!scriptPath) {
    throw new Error("workflow executable path is unavailable");
  }
  const content = await readFile(scriptPath, "utf8");
  const request = {
    inline_script: {
      content,
      sha256: sha256(content),
      interpreter: ["node"],
      mode: 0o755,
    },
    params: { ...params, step },
    tags: record.tags ?? [],
    limits: { max_runtime_seconds: params.child_max_runtime_seconds ?? 3600 },
    required_envelope: true,
    parent_run_id: context.runId,
  };
  const accepted = await l3Request<RunAccepted>(
    context,
    "/v1/runs",
    {
      method: "POST",
      headers: { "Idempotency-Key": `${context.runId}-${step}` },
      body: JSON.stringify(request),
    },
    [200, 201],
  );
  console.log(`dispatched ${step} child run ${accepted.run_id}`);
  return accepted;
}

function workflowBranch(context: Context, params: DogfoodParams): string {
  return params.branch ?? `wefty/dogfood/${context.runId}`;
}

async function runPlan(context: Context, record: RunRecord, params: DogfoodParams): Promise<void> {
  const { sandcastle, noSandbox } = await loadSandcastle(params);
  const result = await sandcastle.run({
    name: "dogfood-plan",
    cwd: params.repo_path,
    sandbox: noSandbox(),
    branchStrategy: { type: "head" },
    agent: sandcastle.claudeCode(params.claude_model ?? "claude-sonnet-4-6", { captureSessions: false }),
    maxIterations: 1,
    logging: { type: "stdout" },
    prompt: [
      "Plan the coding task below without editing files.",
      "Inspect the repository and produce an implementation plan with verification steps.",
      "Return the plan inside literal <plan> and </plan> tags.",
      "",
      params.task,
    ].join("\n"),
    output: sandcastle.Output.string({ tag: "plan" }),
  });
  const plan = result.output.trim();
  if (!plan) {
    throw new Error("Claude returned an empty plan");
  }
  await mkdir(context.handoffDir, { recursive: true, mode: 0o700 });
  const planPath = `${context.handoffDir}/plan.md`;
  await writeFile(planPath, `${plan}\n`, { mode: 0o600 });
  await appendEnvelope(context, "plan", "succeeded", "Claude produced the implementation plan", {
    artifacts: [{ name: "plan", uri: pathToFileURL(planPath).href, media_type: "text/markdown", sha256: sha256(`${plan}\n`) }],
    notes: "The implementation child receives this plan in its immutable run params.",
    extensions: { model: params.claude_model ?? "claude-sonnet-4-6" },
  });
  await appendGate(context, "plan", "plan-produced", "pass", [{ kind: "bytes", value: String(Buffer.byteLength(plan)) }]);
  await dispatchChild(context, record, "implement", { ...params, branch: workflowBranch(context, params), plan });
}

async function runImplement(context: Context, record: RunRecord, params: DogfoodParams): Promise<void> {
  if (!params.plan?.trim()) {
    throw new Error("implement child is missing params.plan");
  }
  const { sandcastle, noSandbox } = await loadSandcastle(params);
  const branch = workflowBranch(context, params);
  const result = await sandcastle.run({
    name: "dogfood-implement",
    cwd: params.repo_path,
    sandbox: noSandbox(),
    branchStrategy: { type: "branch", branch },
    agent: sandcastle.codex(params.codex_model ?? "gpt-5.4", { captureSessions: false }),
    maxIterations: 1,
    logging: { type: "stdout" },
    prompt: [
      "Implement the task using the approved plan below.",
      "Run appropriate checks, commit all intended changes, and finish with <promise>COMPLETE</promise>.",
      `Task: ${params.task}`,
      "",
      "Plan:",
      params.plan,
    ].join("\n"),
  });
  const passed = result.commits.length > 0;
  await appendEnvelope(context, "implement", passed ? "succeeded" : "failed", passed ? `Codex produced ${result.commits.length} commit(s)` : "Codex produced no commits", {
    artifacts: result.commits.map((commit) => ({ name: "commit", uri: `git:${commit.sha}` })),
    notes: `Review branch ${branch} against ${params.base_branch ?? "main"}.`,
    extensions: { branch, model: params.codex_model ?? "gpt-5.4" },
  });
  await appendGate(context, "implement", "implementation-committed", passed ? "pass" : "fail", [
    { kind: "commit-count", value: String(result.commits.length) },
  ]);
  if (!passed) {
    throw new ReportedFailure("implementation gate failed: no commits");
  }
  await dispatchChild(context, record, "review", { ...params, branch });
}

async function runReview(context: Context, params: DogfoodParams): Promise<void> {
  const { sandcastle, noSandbox } = await loadSandcastle(params);
  const branch = workflowBranch(context, params);
  const result = await sandcastle.run({
    name: "dogfood-review",
    cwd: params.repo_path,
    sandbox: noSandbox(),
    branchStrategy: { type: "branch", branch },
    agent: sandcastle.claudeCode(params.claude_model ?? "claude-sonnet-4-6", { captureSessions: false }),
    maxIterations: 1,
    logging: { type: "stdout" },
    prompt: [
      `Cross-review branch ${branch} against ${params.base_branch ?? "main"}.`,
      "Inspect correctness, scope, tests, and the original plan. Fix and commit worthwhile issues.",
      "Run appropriate verification. Return exactly <review>PASS</review> only when the branch is acceptable; otherwise return <review>FAIL</review>.",
      `Task: ${params.task}`,
      "",
      "Plan:",
      params.plan ?? "(plan unavailable)",
    ].join("\n"),
    output: sandcastle.Output.string({ tag: "review" }),
  });
  const verdict = result.output.trim().toUpperCase();
  const passed = verdict === "PASS";
  await appendEnvelope(context, "review", passed ? "succeeded" : "failed", passed ? "Claude cross-review passed" : `Claude cross-review verdict: ${verdict || "empty"}`, {
    artifacts: result.commits.map((commit) => ({ name: "review-fix", uri: `git:${commit.sha}` })),
    notes: passed ? `Branch ${branch} is ready for human integration.` : `Branch ${branch} requires another cold run.`,
    extensions: { branch, verdict, model: params.claude_model ?? "claude-sonnet-4-6" },
  });
  await appendGate(context, "review", "cross-review", passed ? "pass" : "fail", [{ kind: "verdict", value: verdict || "EMPTY" }]);
  if (!passed) {
    throw new ReportedFailure(`cross-review gate failed: ${verdict || "empty verdict"}`);
  }
}

async function main(): Promise<void> {
  const context = loadContext();
  let step: Step = "plan";
  try {
    const record = await getRun(context);
    const params = validateParams(record.params);
    step = params.step ?? "plan";
    await assertRepository(params.repo_path);
    console.log(`starting dogfood ${step} step for run ${context.runId}`);
    if (step === "plan") {
      await runPlan(context, record, params);
    } else if (step === "implement") {
      await runImplement(context, record, params);
    } else {
      await runReview(context, params);
    }
    console.log(`completed dogfood ${step} step for run ${context.runId}`);
  } catch (error) {
    if (!(error instanceof ReportedFailure)) {
      await reportFailure(context, step, error);
    }
    throw error;
  }
}

await main();
