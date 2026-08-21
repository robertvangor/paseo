import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, extname, join } from "node:path";
import type { Logger } from "pino";

import type { AgentProvider, AgentStreamEvent } from "../../agent-sdk-types.js";
import { PiHistoryMapper } from "./history-mapper.js";
import type { PiAgentMessage } from "./rpc-types.js";
import {
  buildUsageSubtitle,
  eventTimestamp,
  JsonlTail,
  mergeUsage,
  parsePiAgentMessage,
  readAssistantUsage,
  type PiSubagentUsage,
} from "./subagent-timeline.js";
import {
  extractTextFromToolResult,
  type PiToolResult,
  type PiTrackedToolCall,
} from "./tool-call-mapper.js";

const DEFAULT_SESSION_POLL_INTERVAL_MS = 250;
const DEFAULT_SESSION_DRAIN_MS = 1_000;

type PiForegroundToolStatus = "running" | "completed" | "failed";
type PiProviderSubagentStatus = "running" | "completed" | "failed" | "canceled";

interface PiForegroundSubagentState {
  id: string;
  index: number;
  mapper: PiHistoryMapper;
  title?: string;
  description?: string;
  status: PiProviderSubagentStatus;
  model?: string;
  thinking?: string;
  usage?: PiSubagentUsage;
  session?: JsonlTail;
  lastSubtitle?: string;
  stopRequested: boolean;
  timelineCounts: Map<string, { result: number; session: number }>;
  lastProgress: string;
  finalMessagesMapped: boolean;
}

interface PiForegroundSubagentIndexOptions {
  provider: AgentProvider;
  emit?: (event: AgentStreamEvent) => void;
  logger?: Pick<Logger, "debug">;
  parentSessionFile?: () => string | undefined;
  contextWindowForModel?: (model: string) => number | undefined;
  onObserve?: () => void;
  pollIntervalMs?: number;
  terminalDrainMs?: number;
}

interface PiForegroundSessionWatcher {
  root: string;
  baseline: Set<string>;
  args: Record<string, unknown>;
  terminalAt: number | null;
}

interface PiForegroundSessionIdentity {
  agent?: string;
  task?: string;
}

interface PiForegroundSubagentSnapshot {
  toolCallId: string;
  args: Record<string, unknown>;
  status: PiForegroundToolStatus;
  result: PiToolResult;
  results: Record<string, unknown>[];
  progress: Record<string, unknown>[];
}

export interface PiAsyncSubagentRun {
  id: string;
  asyncDir: string;
  title: string;
  description?: string;
  subtitle?: string;
  toolCallId: string;
  cwd?: string;
}

export class PiForegroundSubagentIndex {
  private readonly states = new Map<string, Map<number, PiForegroundSubagentState>>();
  private readonly ignoredToolCalls = new Set<string>();
  private readonly sessionWatchers = new Map<string, PiForegroundSessionWatcher>();
  private readonly assignedSessionFiles = new Map<string, string>();
  private readonly pollIntervalMs: number;
  private readonly terminalDrainMs: number;
  private sessionTimer: NodeJS.Timeout | null = null;
  private pollingSessions = false;

  constructor(private readonly options: PiForegroundSubagentIndexOptions) {
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_SESSION_POLL_INTERVAL_MS;
    this.terminalDrainMs = options.terminalDrainMs ?? DEFAULT_SESSION_DRAIN_MS;
  }

  handle(
    toolCallId: string,
    toolCall: PiTrackedToolCall,
    status: PiForegroundToolStatus,
    result: PiToolResult,
  ): AgentStreamEvent[] {
    if (!isSubagentExecution(toolCall) || this.ignoredToolCalls.has(toolCallId)) return [];
    if (isAsyncExecution(toolCall)) {
      this.ignoredToolCalls.add(toolCallId);
      return [];
    }
    const details = resultDetails(result);
    if (typeof details?.asyncDir === "string" || typeof details?.asyncId === "string") {
      this.ignoredToolCalls.add(toolCallId);
      return this.removeToolCall(toolCallId);
    }
    if (status === "running") {
      this.observeSessions(toolCallId, isRecord(toolCall.args) ? toolCall.args : {});
    }
    const results = recordArray(details?.results);
    const progress = recordArray(details?.progress);
    const snapshot: PiForegroundSubagentSnapshot = {
      toolCallId,
      args: isRecord(toolCall.args) ? toolCall.args : {},
      status,
      result,
      results,
      progress,
    };
    return childIndices(results, progress).flatMap((index) => this.handleChild(snapshot, index));
  }

  terminalizeRunning(status: "failed" | "canceled"): AgentStreamEvent[] {
    const events: AgentStreamEvent[] = [];
    for (const [toolCallId, children] of this.states) {
      let terminalized = false;
      for (const state of children.values()) {
        if (state.status !== "running") continue;
        state.status = status;
        state.stopRequested = true;
        terminalized = true;
        events.push(
          providerEvent(this.options.provider, {
            type: "upsert",
            id: state.id,
            status,
            canStop: false,
          }),
        );
      }
      const watcher = this.sessionWatchers.get(toolCallId);
      if (watcher && terminalized) watcher.terminalAt = Date.now() + this.terminalDrainMs;
      if (!watcher && terminalized) this.states.delete(toolCallId);
    }
    this.ignoredToolCalls.clear();
    return events;
  }

  clearToolCall(toolCallId: string): void {
    this.ignoredToolCalls.delete(toolCallId);
    const watcher = this.sessionWatchers.get(toolCallId);
    if (!watcher) {
      this.states.delete(toolCallId);
      return;
    }
    watcher.terminalAt = Date.now() + this.terminalDrainMs;
    void this.flushSessions();
  }

  refreshSubtitles(): void {
    for (const children of this.states.values()) {
      for (const state of children.values()) this.emitSubtitle(state);
    }
  }

  requestStop(id: string): AgentStreamEvent[] | null {
    for (const children of this.states.values()) {
      if (![...children.values()].some((state) => state.id === id)) continue;
      const events: AgentStreamEvent[] = [];
      for (const state of children.values()) {
        if (state.status !== "running" || state.stopRequested) continue;
        state.stopRequested = true;
        events.push(
          providerEvent(this.options.provider, {
            type: "upsert",
            id: state.id,
            canStop: false,
          }),
        );
      }
      return events;
    }
    return null;
  }

  close(): void {
    if (this.sessionTimer) clearInterval(this.sessionTimer);
    this.sessionTimer = null;
    for (const children of this.states.values()) {
      for (const state of children.values()) state.session?.close();
    }
    this.states.clear();
    this.ignoredToolCalls.clear();
    this.sessionWatchers.clear();
    this.assignedSessionFiles.clear();
  }

  async flushSessions(): Promise<void> {
    if (this.pollingSessions) return;
    this.pollingSessions = true;
    try {
      const now = Date.now();
      for (const [toolCallId, watcher] of this.sessionWatchers) {
        this.discoverSessions(toolCallId, watcher);
        const children = this.states.get(toolCallId);
        if (children) {
          await Promise.all([...children.values()].map((state) => this.readSession(state)));
        }
        if (watcher.terminalAt !== null && watcher.terminalAt <= now) {
          for (const state of children?.values() ?? []) state.session?.close();
          this.states.delete(toolCallId);
          this.sessionWatchers.delete(toolCallId);
        }
      }
      if (this.sessionWatchers.size === 0 && this.sessionTimer) {
        clearInterval(this.sessionTimer);
        this.sessionTimer = null;
      }
    } finally {
      this.pollingSessions = false;
    }
  }

  private removeToolCall(toolCallId: string): AgentStreamEvent[] {
    const children = this.states.get(toolCallId);
    for (const state of children?.values() ?? []) state.session?.close();
    this.states.delete(toolCallId);
    this.sessionWatchers.delete(toolCallId);
    if (!children) return [];
    return [...children.values()].map((state) =>
      providerEvent(this.options.provider, { type: "remove", id: state.id }),
    );
  }

  private stateFor(toolCallId: string, index: number): PiForegroundSubagentState {
    const children = this.states.get(toolCallId) ?? new Map<number, PiForegroundSubagentState>();
    const existing = children.get(index);
    if (existing) return existing;
    const id = `${toolCallId}:${index}`;
    const state: PiForegroundSubagentState = {
      id,
      index,
      mapper: new PiHistoryMapper(this.options.provider, [], {
        resolveToolCallId: (childToolCallId) => `${id}:${childToolCallId}`,
      }),
      status: "running",
      stopRequested: false,
      timelineCounts: new Map(),
      lastProgress: "",
      finalMessagesMapped: false,
    };
    children.set(index, state);
    this.states.set(toolCallId, children);
    return state;
  }

  private observeSessions(toolCallId: string, args: Record<string, unknown>): void {
    if (this.sessionWatchers.has(toolCallId)) return;
    const parentSessionFile = this.options.parentSessionFile?.();
    if (!parentSessionFile) return;
    const root = join(
      dirname(parentSessionFile),
      basename(parentSessionFile, extname(parentSessionFile)),
    );
    this.sessionWatchers.set(toolCallId, {
      root,
      baseline: new Set(discoverSessionFiles(root)),
      args,
      terminalAt: null,
    });
    this.options.onObserve?.();
    if (!this.sessionTimer) {
      this.sessionTimer = setInterval(() => void this.flushSessions(), this.pollIntervalMs);
      this.sessionTimer.unref?.();
    }
    void this.flushSessions();
  }

  private discoverSessions(toolCallId: string, watcher: PiForegroundSessionWatcher): void {
    for (const sessionFile of discoverSessionFiles(watcher.root)) {
      if (watcher.baseline.has(sessionFile) || this.assignedSessionFiles.has(sessionFile)) continue;
      const identity = readSessionIdentity(sessionFile);
      if (!this.matchesWatcher(toolCallId, watcher, identity)) continue;
      const state = this.stateForSession(toolCallId, identity);
      state.session = new JsonlTail(sessionFile);
      this.assignedSessionFiles.set(sessionFile, toolCallId);
      if (!state.title || state.title === "Pi subagent" || state.title === "Pi workflow") {
        state.title = identity.agent ?? readString(watcher.args.agent) ?? "Pi subagent";
        state.description = cleanDescription(identity.task ?? readString(watcher.args.task));
        this.options.emit?.(
          providerEvent(this.options.provider, {
            type: "upsert",
            id: state.id,
            title: state.title,
            ...(state.description ? { description: state.description } : {}),
            status: state.status,
            canStop: state.status === "running" && !state.stopRequested,
            toolCallId,
          }),
        );
      }
    }
  }

  private matchesWatcher(
    toolCallId: string,
    watcher: PiForegroundSessionWatcher,
    identity: PiForegroundSessionIdentity,
  ): boolean {
    const children = this.states.get(toolCallId);
    const expectedAgents = new Set(
      [
        readString(watcher.args.agent),
        ...[...(children?.values() ?? [])].map((state) => state.title),
      ]
        .filter((value): value is string => Boolean(value))
        .map(normalizeIdentity),
    );
    if (identity.agent && expectedAgents.has(normalizeIdentity(identity.agent))) return true;
    const task = readString(watcher.args.task);
    if (task && identity.task?.includes(task)) return true;
    return this.sessionWatchers.size === 1;
  }

  private stateForSession(
    toolCallId: string,
    identity: PiForegroundSessionIdentity,
  ): PiForegroundSubagentState {
    const children = this.states.get(toolCallId);
    const agent = identity.agent ? normalizeIdentity(identity.agent) : undefined;
    const matching = [...(children?.values() ?? [])].find(
      (state) => !state.session && agent && state.title && normalizeIdentity(state.title) === agent,
    );
    if (matching) return matching;
    const unassigned = [...(children?.values() ?? [])].find((state) => !state.session);
    if (unassigned) return unassigned;
    const index = children?.size ?? 0;
    return this.stateFor(toolCallId, index);
  }

  private async readSession(state: PiForegroundSubagentState): Promise<void> {
    if (!state.session) return;
    let lines: string[];
    try {
      lines = await state.session.readLines();
    } catch (error) {
      if (!isMissingFileError(error)) {
        this.options.logger?.debug(
          { err: error, filePath: state.session.filePath, subagentId: state.id },
          "Pi foreground child session read failed",
        );
      }
      return;
    }
    let subtitleChanged = false;
    for (const line of lines) {
      let record: Record<string, unknown>;
      try {
        const parsed = JSON.parse(line) as unknown;
        if (!isRecord(parsed)) continue;
        record = parsed;
      } catch (error) {
        this.options.logger?.debug(
          { err: error, filePath: state.session.filePath, subagentId: state.id },
          "Pi foreground child session line is not valid JSON",
        );
        continue;
      }
      if (record.type === "model_change") {
        const provider = readString(record.provider);
        const model = readString(record.modelId);
        if (model) {
          state.model = provider ? `${provider}/${model}` : model;
          subtitleChanged = true;
        }
        continue;
      }
      if (record.type === "thinking_level_change") {
        const thinking = readString(record.thinkingLevel);
        if (thinking) {
          state.thinking = thinking;
          subtitleChanged = true;
        }
        continue;
      }
      if (record.type !== "message") continue;
      const message = parsePiAgentMessage(record.message);
      if (!message) continue;
      for (const event of this.mapMessages(state, [message], "session", eventTimestamp(record))) {
        this.options.emit?.(event);
      }
    }
    if (subtitleChanged) this.emitSubtitle(state);
  }

  private updateModel(
    state: PiForegroundSubagentState,
    result: Record<string, unknown> | undefined,
    progress: Record<string, unknown> | undefined,
  ): void {
    state.model = readString(result?.model) ?? readString(progress?.model) ?? state.model;
    state.thinking =
      readString(result?.thinking) ?? readString(progress?.thinking) ?? state.thinking;
  }

  private subtitleFor(state: PiForegroundSubagentState): string | undefined {
    const base =
      [state.model, state.thinking]
        .filter((value): value is string => Boolean(value))
        .join(" · ") || undefined;
    const contextWindow = state.model
      ? this.options.contextWindowForModel?.(state.model)
      : undefined;
    return buildUsageSubtitle(base, state.usage, contextWindow);
  }

  private emitSubtitle(state: PiForegroundSubagentState): void {
    const subtitle = this.subtitleFor(state);
    if (!subtitle || subtitle === state.lastSubtitle) return;
    state.lastSubtitle = subtitle;
    this.options.emit?.(
      providerEvent(this.options.provider, { type: "upsert", id: state.id, subtitle }),
    );
  }

  private mapMessages(
    state: PiForegroundSubagentState,
    messages: PiAgentMessage[],
    source: "result" | "session",
    timestamp?: string,
  ): AgentStreamEvent[] {
    return messages.flatMap((message) => {
      const timelineEvents = state.mapper
        .mapMessages([message])
        .filter(
          (event): event is Extract<AgentStreamEvent, { type: "timeline" }> =>
            event.type === "timeline",
        );
      const events = timelineEvents.flatMap((event) => {
        const fingerprint = timelineFingerprint(event.item);
        const counts = state.timelineCounts.get(fingerprint) ?? { result: 0, session: 0 };
        const previous = Math.max(counts.result, counts.session);
        counts[source] += 1;
        state.timelineCounts.set(fingerprint, counts);
        if (Math.max(counts.result, counts.session) <= previous) return [];
        return [
          providerEvent(this.options.provider, {
            type: "timeline",
            id: state.id,
            item: event.item,
            ...(timestamp ? { timestamp } : {}),
          }),
        ];
      });
      const usage = events.length > 0 ? readAssistantUsage(message) : null;
      if (usage) {
        state.usage = mergeUsage(state.usage, usage);
        this.emitSubtitle(state);
      }
      return events;
    });
  }

  private handleChild(snapshot: PiForegroundSubagentSnapshot, index: number): AgentStreamEvent[] {
    const childResult = findChild(snapshot.results, index);
    const childProgress = findChild(snapshot.progress, index);
    const state = this.stateFor(snapshot.toolCallId, index);
    const childStatus = resolveChildStatus(snapshot.status, childResult, childProgress);
    const description = resolveDescription(snapshot.args, childResult, childProgress);
    const title = resolveTitle(snapshot.args, childResult, childProgress);
    state.title = title;
    state.description = description;
    state.status = childStatus;
    this.updateModel(state, childResult, childProgress);
    const subtitle = this.subtitleFor(state);
    state.lastSubtitle = subtitle;
    return [
      providerEvent(this.options.provider, {
        type: "upsert",
        id: state.id,
        title,
        ...(description ? { description } : {}),
        status: childStatus,
        canStop: childStatus === "running" && !state.stopRequested,
        toolCallId: snapshot.toolCallId,
        ...(subtitle ? { subtitle } : {}),
      }),
      ...this.progressEvents(state, childStatus, childResult, childProgress),
      ...this.finalEvents(state, snapshot, childResult),
    ];
  }

  private progressEvents(
    state: PiForegroundSubagentState,
    status: PiProviderSubagentStatus,
    result: Record<string, unknown> | undefined,
    progress: Record<string, unknown> | undefined,
  ): AgentStreamEvent[] {
    const text = renderProgress(result, progress);
    if (!text || text === state.lastProgress) return [];
    state.lastProgress = text;
    return [
      providerEvent(this.options.provider, {
        type: "timeline",
        id: state.id,
        item: {
          type: "tool_call",
          callId: `${state.id}:live-progress`,
          name: "subagent_progress",
          status: status === "running" ? "running" : "completed",
          detail: { type: "plain_text", label: "Live activity", text },
          error: null,
        },
      }),
    ];
  }

  private finalEvents(
    state: PiForegroundSubagentState,
    snapshot: PiForegroundSubagentSnapshot,
    result: Record<string, unknown> | undefined,
  ): AgentStreamEvent[] {
    if (snapshot.status === "running" || state.finalMessagesMapped) return [];
    state.finalMessagesMapped = true;
    const messages = parseMessages(result?.messages);
    if (messages.length > 0) {
      return this.mapMessages(state, messages, "result");
    }
    if (state.index !== 0) return [];
    const text =
      readString(result?.finalOutput) ?? extractTextFromToolResult(snapshot.result)?.trim();
    return text
      ? this.mapMessages(
          state,
          [{ role: "assistant", content: [{ type: "text", text }] }],
          "result",
        )
      : [];
  }
}

export function readPiAsyncSubagentRun(
  toolCallId: string,
  toolCall: PiTrackedToolCall,
  result: PiToolResult,
): PiAsyncSubagentRun | null {
  if (!isSubagentExecution(toolCall)) return null;
  const details = resultDetails(result);
  const id = readString(details?.asyncId) ?? readString(details?.runId);
  const asyncDir = readString(details?.asyncDir);
  if (!id || !asyncDir) return null;
  const args = isRecord(toolCall.args) ? toolCall.args : {};
  const description = resolveDescription(args, undefined, undefined);
  const subtitle = resolveSubtitle(args, details ?? undefined);
  const cwd = readString(details?.cwd) ?? readString(args.cwd);
  return {
    id,
    asyncDir,
    title: resolveTitle(args, undefined, undefined),
    ...(description ? { description } : {}),
    ...(subtitle ? { subtitle } : {}),
    toolCallId,
    ...(cwd ? { cwd } : {}),
  };
}

function providerEvent(
  provider: AgentProvider,
  event: Extract<AgentStreamEvent, { type: "provider_subagent" }>["event"],
): AgentStreamEvent {
  return { type: "provider_subagent", provider, event };
}

function isSubagentExecution(toolCall: PiTrackedToolCall): boolean {
  if (toolCall.toolName === "task") return true;
  if (toolCall.toolName !== "subagent" || !isRecord(toolCall.args)) return false;
  return (
    toolCall.args.action === undefined &&
    (toolCall.args.agent !== undefined ||
      toolCall.args.task !== undefined ||
      toolCall.args.workflowScript !== undefined)
  );
}

function isAsyncExecution(toolCall: PiTrackedToolCall): boolean {
  return isRecord(toolCall.args) && toolCall.args.async === true;
}

function resultDetails(result: PiToolResult): Record<string, unknown> | null {
  if (!result || typeof result === "string" || !isRecord(result.details)) return null;
  return result.details;
}

function resolveChildStatus(
  parentStatus: PiForegroundToolStatus,
  result: Record<string, unknown> | undefined,
  progress: Record<string, unknown> | undefined,
): PiProviderSubagentStatus {
  const childStatus = readString(progress?.status) ?? readString(result?.status);
  const exitCode = result?.exitCode;
  if (
    childStatus === "failed" ||
    (typeof exitCode === "number" && exitCode !== 0) ||
    readString(result?.error)
  ) {
    return "failed";
  }
  if (
    childStatus === "detached" ||
    childStatus === "canceled" ||
    childStatus === "stopped" ||
    result?.interrupted === true ||
    result?.stopped === true
  ) {
    return "canceled";
  }
  if (childStatus === "completed" || parentStatus === "completed") return "completed";
  if (parentStatus === "failed") return "failed";
  return "running";
}

function resolveSubtitle(
  result: Record<string, unknown> | undefined,
  progress: Record<string, unknown> | undefined,
): string | undefined {
  const model = readString(result?.model) ?? readString(progress?.model);
  const thinking = readString(result?.thinking) ?? readString(progress?.thinking);
  return (
    [model, thinking].filter((value): value is string => Boolean(value)).join(" · ") || undefined
  );
}

function resolveTitle(
  args: Record<string, unknown>,
  result: Record<string, unknown> | undefined,
  progress: Record<string, unknown> | undefined,
): string {
  return (
    readString(result?.agent) ??
    readString(progress?.agent) ??
    readString(args.agent) ??
    (args.workflowScript === undefined ? "Pi subagent" : "Pi workflow")
  );
}

function resolveDescription(
  args: Record<string, unknown>,
  result: Record<string, unknown> | undefined,
  progress: Record<string, unknown> | undefined,
): string | undefined {
  return cleanDescription(
    readString(result?.task) ?? readString(progress?.task) ?? readString(args.task),
  );
}

function renderProgress(
  result: Record<string, unknown> | undefined,
  progress: Record<string, unknown> | undefined,
): string {
  const lines: string[] = [];
  const currentTool = readString(progress?.currentTool);
  const currentToolArgs = readString(progress?.currentToolArgs);
  if (currentTool) lines.push(currentToolArgs ? `${currentTool}: ${currentToolArgs}` : currentTool);
  for (const toolCall of recordArray(result?.toolCalls)) {
    const text = readString(toolCall.expandedText) ?? readString(toolCall.text);
    if (text) lines.push(text);
  }
  for (const output of stringArray(progress?.recentOutput)) lines.push(output);
  return deduplicateLines(lines).join("\n").trim();
}

function parseMessages(value: unknown): PiAgentMessage[] {
  if (!Array.isArray(value)) return [];
  return value.filter(isPiAgentMessage);
}

function isPiAgentMessage(value: unknown): value is PiAgentMessage {
  if (!isRecord(value) || typeof value.role !== "string") return false;
  if (value.role === "user" || value.role === "custom") return value.content !== undefined;
  if (value.role === "assistant") return Array.isArray(value.content);
  if (value.role === "toolResult") {
    return typeof value.toolCallId === "string" && typeof value.toolName === "string";
  }
  return (
    value.role === "bashExecution" &&
    typeof value.command === "string" &&
    typeof value.timestamp === "number"
  );
}

function cleanDescription(value: string | undefined): string | undefined {
  return value && value !== "[prompt redacted]" ? value : undefined;
}

function readIndex(value: Record<string, unknown>, fallback: number): number {
  return typeof value.index === "number" && Number.isInteger(value.index) ? value.index : fallback;
}

function childIndices(
  results: readonly Record<string, unknown>[],
  progress: readonly Record<string, unknown>[],
): number[] {
  const indices = new Set<number>();
  results.forEach((entry, index) => indices.add(readIndex(entry, index)));
  progress.forEach((entry, index) => indices.add(readIndex(entry, index)));
  if (indices.size === 0) indices.add(0);
  return [...indices].sort((left, right) => left - right);
}

function findChild(
  entries: readonly Record<string, unknown>[],
  index: number,
): Record<string, unknown> | undefined {
  return entries.find((entry, position) => readIndex(entry, position) === index);
}

function recordArray(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string")
    : [];
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function deduplicateLines(lines: readonly string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const line of lines) {
    const normalized = line.trim();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function discoverSessionFiles(root: string): string[] {
  if (!existsSync(root)) return [];
  const files: string[] = [];
  try {
    for (const run of readdirSync(root, { withFileTypes: true })) {
      if (!run.isDirectory()) continue;
      const runRoot = join(root, run.name);
      for (const child of readdirSync(runRoot, { withFileTypes: true })) {
        if (!child.isDirectory() || !child.name.startsWith("run-")) continue;
        const sessionFile = join(runRoot, child.name, "session.jsonl");
        if (existsSync(sessionFile)) files.push(sessionFile);
      }
    }
  } catch {
    return files;
  }
  return files.sort((left, right) => sessionSortTimestamp(left) - sessionSortTimestamp(right));
}

function sessionSortTimestamp(filePath: string): number {
  try {
    const stat = statSync(filePath);
    return stat.birthtimeMs || stat.mtimeMs;
  } catch {
    return 0;
  }
}

function readSessionIdentity(sessionFile: string): PiForegroundSessionIdentity {
  let text: string;
  try {
    text = readFileSync(sessionFile, "utf8").slice(0, 256 * 1024);
  } catch {
    return {};
  }
  const runId = basename(dirname(dirname(sessionFile)));
  let agent: string | undefined;
  let task: string | undefined;
  for (const line of text.split("\n")) {
    const record = parseJsonRecord(line);
    if (!record) continue;
    agent ??= sessionAgent(record, runId);
    task ??= sessionTask(record);
    if (agent && task) break;
  }
  return { ...(agent ? { agent } : {}), ...(task ? { task } : {}) };
}

function parseJsonRecord(line: string): Record<string, unknown> | null {
  if (!line.trim()) return null;
  try {
    const parsed = JSON.parse(line) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function sessionAgent(record: Record<string, unknown>, runId: string): string | undefined {
  if (record.type !== "session_info") return undefined;
  const name = readString(record.name);
  const prefix = "subagent-";
  const suffix = `-${runId}-`;
  const suffixIndex = name?.lastIndexOf(suffix) ?? -1;
  if (!name?.startsWith(prefix) || suffixIndex <= prefix.length) return undefined;
  return name.slice(prefix.length, suffixIndex);
}

function sessionTask(record: Record<string, unknown>): string | undefined {
  if (record.type !== "message" || !isRecord(record.message)) return undefined;
  if (record.message.role !== "user") return undefined;
  const content = record.message.content;
  let userText = "";
  if (typeof content === "string") {
    userText = content;
  } else if (Array.isArray(content)) {
    userText = content
      .filter(isRecord)
      .filter((part) => part.type === "text")
      .map((part) => readString(part.text))
      .filter((part): part is string => Boolean(part))
      .join("\n");
  }
  return userText.replace(/^Task:\s*/i, "").trim() || undefined;
}

function normalizeIdentity(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function timelineFingerprint(
  item: Extract<AgentStreamEvent, { type: "timeline" }>["item"],
): string {
  if (item.type === "assistant_message") {
    return JSON.stringify({ type: item.type, text: item.text });
  }
  return JSON.stringify(item);
}

function isMissingFileError(error: unknown): boolean {
  return isRecord(error) && error.code === "ENOENT";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
