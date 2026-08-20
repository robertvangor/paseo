import { open, readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Logger } from "pino";

import type { AgentProvider, AgentStreamEvent } from "../../agent-sdk-types.js";
import { PiHistoryMapper } from "./history-mapper.js";
import type { PiAgentMessage } from "./rpc-types.js";

const READ_BUFFER_BYTES = 64 * 1024;
const DEFAULT_POLL_INTERVAL_MS = 250;
const DEFAULT_TERMINAL_DRAIN_MS = 1_000;

type PiSubagentTimelineLogger = Pick<Logger, "debug">;
type PiSubagentStatus = "running" | "completed" | "failed" | "canceled";
type PiMessageSource = "event" | "session";

interface PiSessionTail {
  id: string;
  stepKey: string;
  tail: JsonlTail;
}

interface PiMessageCounts {
  event: number;
  session: number;
}

interface PiSubagentUsage {
  totalTokens?: number;
  totalCostUsd?: number;
}

export interface PiSubagentEventReaderOptions {
  id: string;
  asyncDir: string;
  provider: AgentProvider;
  emit: (event: AgentStreamEvent) => void;
  logger: PiSubagentTimelineLogger;
}

export class PiSubagentEventReader {
  private readonly eventsPath: string;
  private readonly statusPath: string;
  private readonly eventTail: JsonlTail;
  private readonly sessionTails = new Map<string, PiSessionTail>();
  private readonly mappers = new Map<string, PiHistoryMapper>();
  private readonly messageCounts = new Map<string, PiMessageCounts>();
  private readonly lastProgress = new Map<string, string>();
  private readonly lastDescriptors = new Map<string, string>();
  private readonly baseSubtitles = new Map<string, string>();
  private readonly usage = new Map<string, PiSubagentUsage>();
  private lastStatusText = "";
  private usesStepDescriptors = false;
  private rootRemoved = false;
  private reading = false;
  private closed = false;
  private terminal = false;

  constructor(private readonly options: PiSubagentEventReaderOptions) {
    this.eventsPath = join(options.asyncDir, "events.jsonl");
    this.statusPath = join(options.asyncDir, "status.json");
    this.eventTail = new JsonlTail(this.eventsPath);
  }

  async readAvailable(): Promise<void> {
    if (this.closed || this.reading) return;
    this.reading = true;
    try {
      await this.readStatus();
      await this.readEvents();
      for (const session of this.sessionTails.values()) {
        await this.readSession(session);
      }
    } finally {
      this.reading = false;
    }
  }

  isTerminal(): boolean {
    return this.terminal;
  }

  close(): void {
    this.closed = true;
    this.eventTail.close();
    for (const session of this.sessionTails.values()) session.tail.close();
    this.sessionTails.clear();
    this.mappers.clear();
    this.messageCounts.clear();
    this.baseSubtitles.clear();
    this.usage.clear();
  }

  private async readEvents(): Promise<void> {
    let lines: string[];
    try {
      lines = await this.eventTail.readLines();
    } catch (error) {
      this.logReadError(error, this.eventsPath, "Pi subagent event read failed");
      return;
    }
    for (const line of lines) {
      const event = this.parseLine(line, this.eventsPath);
      if (!event || event.subagentSource !== "child") continue;
      if (event.type !== "message_end" && event.type !== "tool_result_end") continue;
      const message = parsePiAgentMessage(event.message);
      if (!message) continue;
      const stepKey = resolveStepKey(event);
      const id = this.usesStepDescriptors
        ? stepSubagentId(this.options.id, stepKey)
        : this.options.id;
      this.emitMessage(id, stepKey, message, "event", eventTimestamp(event));
    }
  }

  private async readStatus(): Promise<void> {
    let text: string;
    try {
      text = await readFile(this.statusPath, "utf8");
    } catch (error) {
      this.logReadError(error, this.statusPath, "Pi subagent status read failed");
      return;
    }
    if (text === this.lastStatusText) return;
    this.lastStatusText = text;
    let status: Record<string, unknown>;
    try {
      const parsed = JSON.parse(text) as unknown;
      if (!isRecord(parsed)) return;
      status = parsed;
    } catch (error) {
      this.options.logger.debug(
        { err: error, filePath: this.statusPath, subagentId: this.options.id },
        "Pi subagent status is not valid JSON",
      );
      return;
    }
    this.consumeStatus(status);
  }

  private consumeStatus(status: Record<string, unknown>): void {
    const lifecycle = statusValue(status.state);
    this.terminal = lifecycle !== "running";
    const steps = recordArray(status.steps);
    if (steps.length === 0) {
      this.consumeRootStatus(status, lifecycle);
      return;
    }

    this.usesStepDescriptors = true;
    if (!this.rootRemoved) {
      this.rootRemoved = true;
      this.lastDescriptors.delete(this.options.id);
      this.options.emit({
        type: "provider_subagent",
        provider: this.options.provider,
        event: { type: "remove", id: this.options.id },
      });
    }

    const cwd = readString(status.cwd);
    const activeIds = new Set<string>();
    for (const [position, step] of steps.entries()) {
      const stepKey = statusStepKey(step, position);
      const id = stepSubagentId(this.options.id, stepKey);
      activeIds.add(id);
      const title =
        readString(step.label) ??
        readString(step.workflowKey) ??
        readString(step.agent) ??
        "Pi subagent";
      const description = cleanDescription(readString(step.description));
      const subtitle = this.resolveSubtitle(id, step);
      const timestamp = statusTimestamp(step) ?? statusTimestamp(status);
      this.emitDescriptor({
        type: "upsert",
        id,
        status: statusValue(step.status ?? status.state),
        title,
        ...(description ? { description } : {}),
        ...(subtitle ? { subtitle } : {}),
        ...(cwd ? { cwd } : {}),
        ...(timestamp ? { timestamp } : {}),
      });
      const sessionFile = readString(step.sessionFile);
      if (sessionFile && !this.sessionTails.has(sessionFile)) {
        this.sessionTails.set(sessionFile, { id, stepKey, tail: new JsonlTail(sessionFile) });
      }
      if (!sessionFile) this.emitProgress(id, step);
    }
    for (const id of this.lastDescriptors.keys()) {
      if (activeIds.has(id)) continue;
      this.lastDescriptors.delete(id);
      this.options.emit({
        type: "provider_subagent",
        provider: this.options.provider,
        event: { type: "remove", id },
      });
    }
  }

  private consumeRootStatus(status: Record<string, unknown>, lifecycle: PiSubagentStatus): void {
    const title = readString(status.agent);
    const description = cleanDescription(readString(status.description));
    const subtitle = this.resolveSubtitle(this.options.id, status);
    const cwd = readString(status.cwd);
    const timestamp = statusTimestamp(status);
    this.emitDescriptor({
      type: "upsert",
      id: this.options.id,
      status: lifecycle,
      ...(title ? { title } : {}),
      ...(description ? { description } : {}),
      ...(subtitle ? { subtitle } : {}),
      ...(cwd ? { cwd } : {}),
      ...(timestamp ? { timestamp } : {}),
    });
    const sessionFile = readString(status.sessionFile);
    if (sessionFile && !this.sessionTails.has(sessionFile)) {
      this.sessionTails.set(sessionFile, {
        id: this.options.id,
        stepKey: "0:agent",
        tail: new JsonlTail(sessionFile),
      });
    }
    if (!sessionFile) this.emitProgress(this.options.id, status);
  }

  private emitDescriptor(
    descriptor: Extract<AgentStreamEvent, { type: "provider_subagent" }>["event"],
  ): void {
    if (descriptor.type !== "upsert") return;
    const signature = JSON.stringify(descriptor);
    if (this.lastDescriptors.get(descriptor.id) === signature) return;
    this.lastDescriptors.set(descriptor.id, signature);
    this.options.emit({
      type: "provider_subagent",
      provider: this.options.provider,
      event: descriptor,
    });
  }

  private emitProgress(id: string, step: Record<string, unknown>): void {
    const text = renderProgress(step);
    if (!text || this.lastProgress.get(id) === text) return;
    this.lastProgress.set(id, text);
    this.options.emit({
      type: "provider_subagent",
      provider: this.options.provider,
      event: {
        type: "timeline",
        id,
        item: {
          type: "tool_call",
          callId: `${id}:live-progress`,
          name: "subagent_progress",
          status: statusValue(step.status) === "running" ? "running" : "completed",
          detail: { type: "plain_text", label: "Live activity", text },
          error: null,
        },
        ...(statusTimestamp(step) ? { timestamp: statusTimestamp(step) } : {}),
      },
    });
  }

  private async readSession(session: PiSessionTail): Promise<void> {
    let lines: string[];
    try {
      lines = await session.tail.readLines();
    } catch (error) {
      this.logReadError(error, session.tail.filePath, "Pi child session read failed");
      return;
    }
    for (const line of lines) {
      const record = this.parseLine(line, session.tail.filePath);
      if (!record || record.type !== "message") continue;
      const message = parsePiAgentMessage(record.message);
      if (!message) continue;
      this.consumeUsage(session.id, record.message);
      this.emitMessage(session.id, session.stepKey, message, "session", eventTimestamp(record));
    }
  }

  private resolveSubtitle(id: string, value: Record<string, unknown>): string | undefined {
    const base = joinSubtitle(value);
    if (base) this.baseSubtitles.set(id, base);
    else this.baseSubtitles.delete(id);
    return buildUsageSubtitle(base, this.usage.get(id));
  }

  private consumeUsage(id: string, message: unknown): void {
    const increment = readAssistantUsage(message);
    if (!increment) return;
    const previous = this.usage.get(id);
    const next = mergeUsage(previous, increment);
    this.usage.set(id, next);
    const subtitle = buildUsageSubtitle(this.baseSubtitles.get(id), next);
    if (subtitle) this.emitDescriptor({ type: "upsert", id, subtitle });
  }

  private emitMessage(
    id: string,
    stepKey: string,
    message: PiAgentMessage,
    source: PiMessageSource,
    timestamp: string | undefined,
  ): void {
    if (!this.shouldEmitMessage(id, stepKey, message, source)) return;
    for (const mapped of this.mapperFor(id, stepKey).mapMessages([message])) {
      if (mapped.type !== "timeline") continue;
      this.options.emit({
        type: "provider_subagent",
        provider: this.options.provider,
        event: {
          type: "timeline",
          id,
          item: mapped.item,
          ...(timestamp ? { timestamp } : {}),
        },
      });
    }
  }

  private shouldEmitMessage(
    id: string,
    stepKey: string,
    message: PiAgentMessage,
    source: PiMessageSource,
  ): boolean {
    const fingerprint = `${id}:${stepKey}:${JSON.stringify(message)}`;
    const counts = this.messageCounts.get(fingerprint) ?? { event: 0, session: 0 };
    const previous = Math.max(counts.event, counts.session);
    counts[source] += 1;
    this.messageCounts.set(fingerprint, counts);
    return Math.max(counts.event, counts.session) > previous;
  }

  private mapperFor(id: string, stepKey: string): PiHistoryMapper {
    const mapperKey = `${id}\0${stepKey}`;
    const existing = this.mappers.get(mapperKey);
    if (existing) return existing;
    const toolCallPrefix = id === this.options.id ? `${id}:${stepKey}` : id;
    const mapper = new PiHistoryMapper(this.options.provider, [], {
      resolveToolCallId: (toolCallId) => `${toolCallPrefix}:${toolCallId}`,
    });
    this.mappers.set(mapperKey, mapper);
    return mapper;
  }

  private parseLine(line: string, filePath: string): Record<string, unknown> | null {
    if (!line.trim()) return null;
    try {
      const parsed = JSON.parse(line) as unknown;
      return isRecord(parsed) ? parsed : null;
    } catch (error) {
      this.options.logger.debug(
        { err: error, filePath, subagentId: this.options.id },
        "Pi subagent artifact line is not valid JSON",
      );
      return null;
    }
  }

  private logReadError(error: unknown, filePath: string, message: string): void {
    if (isMissingFileError(error)) return;
    this.options.logger.debug({ err: error, filePath, subagentId: this.options.id }, message);
  }
}

interface PiSubagentTimelineBridgeOptions {
  provider: AgentProvider;
  emit: (event: AgentStreamEvent) => void;
  logger: PiSubagentTimelineLogger;
  pollIntervalMs?: number;
  terminalDrainMs?: number;
}

interface PiObservedSubagent {
  reader: PiSubagentEventReader;
  terminalAt: number | null;
}

export class PiSubagentTimelineBridge {
  private readonly runs = new Map<string, PiObservedSubagent>();
  private readonly observedIds = new Set<string>();
  private readonly pollIntervalMs: number;
  private readonly terminalDrainMs: number;
  private timer: NodeJS.Timeout | null = null;
  private polling = false;

  constructor(private readonly options: PiSubagentTimelineBridgeOptions) {
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.terminalDrainMs = options.terminalDrainMs ?? DEFAULT_TERMINAL_DRAIN_MS;
  }

  observe(id: string, asyncDir: string): void {
    this.observedIds.add(id);
    if (this.runs.has(id)) return;
    const reader = new PiSubagentEventReader({
      id,
      asyncDir,
      provider: this.options.provider,
      emit: this.options.emit,
      logger: this.options.logger,
    });
    this.runs.set(id, { reader, terminalAt: null });
    void reader.readAvailable();
    this.ensureTimer();
  }

  complete(id: string): boolean {
    const run = this.runs.get(id);
    if (run) {
      run.terminalAt = Date.now() + this.terminalDrainMs;
      void this.poll();
    }
    return this.observedIds.has(id);
  }

  close(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const run of this.runs.values()) run.reader.close();
    this.runs.clear();
    this.observedIds.clear();
  }

  async flush(id?: string): Promise<void> {
    if (id) {
      await this.runs.get(id)?.reader.readAvailable();
      return;
    }
    await Promise.all([...this.runs.values()].map((run) => run.reader.readAvailable()));
  }

  private ensureTimer(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.poll(), this.pollIntervalMs);
    this.timer.unref?.();
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    try {
      const now = Date.now();
      for (const [id, run] of this.runs) {
        await run.reader.readAvailable();
        if (run.reader.isTerminal() && run.terminalAt === null) {
          run.terminalAt = now + this.terminalDrainMs;
        }
        if (run.terminalAt !== null && run.terminalAt <= now) {
          run.reader.close();
          this.runs.delete(id);
        }
      }
      if (this.runs.size === 0 && this.timer) {
        clearInterval(this.timer);
        this.timer = null;
      }
    } finally {
      this.polling = false;
    }
  }
}

class JsonlTail {
  private decoder = new TextDecoder();
  private pendingText = "";
  private offset = 0;

  constructor(readonly filePath: string) {}

  async readLines(): Promise<string[]> {
    const handle = await open(this.filePath, "r");
    try {
      const stat = await handle.stat();
      if (stat.size < this.offset) this.reset();
      const lines: string[] = [];
      const buffer = Buffer.allocUnsafe(READ_BUFFER_BYTES);
      while (this.offset < stat.size) {
        const length = Math.min(buffer.byteLength, stat.size - this.offset);
        const { bytesRead } = await handle.read(buffer, 0, length, this.offset);
        if (bytesRead === 0) break;
        this.offset += bytesRead;
        this.pendingText += this.decoder.decode(buffer.subarray(0, bytesRead), { stream: true });
        const chunks = this.pendingText.split("\n");
        this.pendingText = chunks.pop() ?? "";
        lines.push(...chunks);
      }
      return lines;
    } finally {
      await handle.close();
    }
  }

  close(): void {
    this.pendingText = "";
  }

  private reset(): void {
    this.offset = 0;
    this.pendingText = "";
    this.decoder = new TextDecoder();
  }
}

function statusStepKey(step: Record<string, unknown>, position: number): string {
  const index = typeof step.index === "number" ? step.index : position;
  const agent = readString(step.agent) ?? readString(step.label) ?? "agent";
  return `${index}:${agent}`;
}

function stepSubagentId(runId: string, stepKey: string): string {
  return `${runId}:${stepKey}`;
}

function statusValue(value: unknown): PiSubagentStatus {
  const status = readString(value);
  if (status === "complete" || status === "completed") return "completed";
  if (status === "failed" || status === "rejected") return "failed";
  if (
    status === "canceled" ||
    status === "detached" ||
    status === "paused" ||
    status === "stopped"
  ) {
    return "canceled";
  }
  return "running";
}

function joinSubtitle(value: Record<string, unknown>): string | undefined {
  const model = readString(value.model);
  const thinking = readString(value.thinking);
  return (
    [model, thinking].filter((entry): entry is string => Boolean(entry)).join(" · ") || undefined
  );
}

function readAssistantUsage(message: unknown): PiSubagentUsage | null {
  if (!isRecord(message) || message.role !== "assistant" || !isRecord(message.usage)) return null;
  const usage = message.usage;
  const input = readPositiveNumber(usage.input);
  const output = readPositiveNumber(usage.output);
  const totalTokens = readPositiveNumber(usage.totalTokens) ?? sumNumbers(input, output);
  const cost = usage.cost;
  const totalCostUsd = isRecord(cost) ? readPositiveNumber(cost.total) : readPositiveNumber(cost);
  if (totalTokens === undefined && totalCostUsd === undefined) return null;
  return {
    ...(totalTokens !== undefined ? { totalTokens } : {}),
    ...(totalCostUsd !== undefined ? { totalCostUsd } : {}),
  };
}

function mergeUsage(
  previous: PiSubagentUsage | undefined,
  increment: PiSubagentUsage,
): PiSubagentUsage {
  const totalTokens = sumNumbers(previous?.totalTokens, increment.totalTokens);
  const totalCostUsd = sumNumbers(previous?.totalCostUsd, increment.totalCostUsd);
  return {
    ...(totalTokens !== undefined ? { totalTokens } : {}),
    ...(totalCostUsd !== undefined ? { totalCostUsd } : {}),
  };
}

function buildUsageSubtitle(
  base: string | undefined,
  usage: PiSubagentUsage | undefined,
): string | undefined {
  const parts = [base, formatTokens(usage?.totalTokens), formatCost(usage?.totalCostUsd)].filter(
    (part): part is string => Boolean(part),
  );
  return parts.join(" · ") || undefined;
}

function formatTokens(totalTokens: number | undefined): string | undefined {
  if (totalTokens === undefined) return undefined;
  if (totalTokens < 1_000) return `${Math.round(totalTokens)} tokens`;
  return `${Math.round(totalTokens / 100) / 10}k tokens`;
}

function formatCost(totalCostUsd: number | undefined): string | undefined {
  if (totalCostUsd === undefined) return undefined;
  return `$${totalCostUsd < 0.01 ? totalCostUsd.toFixed(4) : totalCostUsd.toFixed(2)}`;
}

function readPositiveNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : undefined;
}

function sumNumbers(left: number | undefined, right: number | undefined): number | undefined {
  if (left === undefined) return right;
  if (right === undefined) return left;
  return left + right;
}

function renderProgress(step: Record<string, unknown>): string {
  const lines: string[] = [];
  const currentTool = readString(step.currentTool);
  const currentToolArgs = readString(step.currentToolArgs);
  if (currentTool) lines.push(currentToolArgs ? `${currentTool}: ${currentToolArgs}` : currentTool);
  for (const recentTool of recordArray(step.recentTools)) {
    const tool = readString(recentTool.tool);
    const args = readString(recentTool.args);
    if (tool) lines.push(args ? `${tool}: ${args}` : tool);
  }
  lines.push(...stringArray(step.recentOutput));
  return deduplicateLines(lines).join("\n");
}

function parsePiAgentMessage(value: unknown): PiAgentMessage | null {
  if (!isRecord(value) || typeof value.role !== "string") return null;
  if ((value.role === "user" || value.role === "custom") && value.content !== undefined) {
    return value as unknown as PiAgentMessage;
  }
  if (value.role === "assistant" && Array.isArray(value.content)) {
    return value as unknown as PiAgentMessage;
  }
  if (
    value.role === "toolResult" &&
    typeof value.toolCallId === "string" &&
    typeof value.toolName === "string"
  ) {
    return value as unknown as PiAgentMessage;
  }
  if (
    value.role === "bashExecution" &&
    typeof value.command === "string" &&
    typeof value.timestamp === "number"
  ) {
    return value as unknown as PiAgentMessage;
  }
  return null;
}

function resolveStepKey(event: Record<string, unknown>): string {
  let index = 0;
  if (typeof event.subagentStepIndex === "number") index = event.subagentStepIndex;
  else if (typeof event.stepIndex === "number") index = event.stepIndex;
  let agent = "agent";
  if (typeof event.subagentAgent === "string") agent = event.subagentAgent;
  else if (typeof event.agent === "string") agent = event.agent;
  return `${index}:${agent}`;
}

function eventTimestamp(event: Record<string, unknown>): string | undefined {
  const value = event.timestamp ?? event.observedAt ?? event.ts;
  if (typeof value === "string") {
    const timestamp = Date.parse(value);
    return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : undefined;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return new Date(value).toISOString();
}

function statusTimestamp(status: Record<string, unknown>): string | undefined {
  return eventTimestamp({
    timestamp: status.lastUpdate ?? status.endedAt ?? status.startedAt,
  });
}

function cleanDescription(value: string | undefined): string | undefined {
  return value && value !== "[prompt redacted]" ? value : undefined;
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
  return lines.flatMap((line) => {
    const value = line.trim();
    if (!value || seen.has(value)) return [];
    seen.add(value);
    return [value];
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMissingFileError(error: unknown): boolean {
  return isRecord(error) && error.code === "ENOENT";
}
