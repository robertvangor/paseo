import type { AgentProvider, AgentStreamEvent } from "../../agent-sdk-types.js";
import { PiHistoryMapper } from "./history-mapper.js";
import type { PiAgentMessage } from "./rpc-types.js";
import {
  extractTextFromToolResult,
  type PiToolResult,
  type PiTrackedToolCall,
} from "./tool-call-mapper.js";

type PiForegroundToolStatus = "running" | "completed" | "failed";
type PiProviderSubagentStatus = "running" | "completed" | "failed" | "canceled";

interface PiForegroundSubagentState {
  id: string;
  index: number;
  mapper: PiHistoryMapper;
  lastProgress: string;
  finalMessagesMapped: boolean;
}

interface PiForegroundSubagentIndexOptions {
  provider: AgentProvider;
}

interface PiForegroundSubagentSnapshot {
  toolCallId: string;
  args: Record<string, unknown>;
  status: PiForegroundToolStatus;
  result: PiToolResult;
  results: Record<string, unknown>[];
  progress: Record<string, unknown>[];
}

export class PiForegroundSubagentIndex {
  private readonly states = new Map<string, Map<number, PiForegroundSubagentState>>();
  private readonly ignoredToolCalls = new Set<string>();

  constructor(private readonly options: PiForegroundSubagentIndexOptions) {}

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
    for (const children of this.states.values()) {
      for (const state of children.values()) {
        events.push(
          providerEvent(this.options.provider, {
            type: "upsert",
            id: state.id,
            status,
          }),
        );
      }
    }
    this.states.clear();
    this.ignoredToolCalls.clear();
    return events;
  }

  clearToolCall(toolCallId: string): void {
    this.states.delete(toolCallId);
    this.ignoredToolCalls.delete(toolCallId);
  }

  private removeToolCall(toolCallId: string): AgentStreamEvent[] {
    const children = this.states.get(toolCallId);
    this.states.delete(toolCallId);
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
      lastProgress: "",
      finalMessagesMapped: false,
    };
    children.set(index, state);
    this.states.set(toolCallId, children);
    return state;
  }

  private handleChild(snapshot: PiForegroundSubagentSnapshot, index: number): AgentStreamEvent[] {
    const childResult = findChild(snapshot.results, index);
    const childProgress = findChild(snapshot.progress, index);
    const state = this.stateFor(snapshot.toolCallId, index);
    const childStatus = resolveChildStatus(snapshot.status, childResult, childProgress);
    const description = resolveDescription(snapshot.args, childResult, childProgress);
    const subtitle = resolveSubtitle(childResult, childProgress);
    return [
      providerEvent(this.options.provider, {
        type: "upsert",
        id: state.id,
        title: resolveTitle(snapshot.args, childResult, childProgress),
        ...(description ? { description } : {}),
        status: childStatus,
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
      return state.mapper
        .mapMessages(messages)
        .filter(
          (mapped): mapped is Extract<AgentStreamEvent, { type: "timeline" }> =>
            mapped.type === "timeline",
        )
        .map((mapped) =>
          providerEvent(this.options.provider, {
            type: "timeline",
            id: state.id,
            item: mapped.item,
          }),
        );
    }
    if (state.index !== 0) return [];
    const text =
      readString(result?.finalOutput) ?? extractTextFromToolResult(snapshot.result)?.trim();
    return text
      ? [
          providerEvent(this.options.provider, {
            type: "timeline",
            id: state.id,
            item: { type: "assistant_message", text },
          }),
        ]
      : [];
  }
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
