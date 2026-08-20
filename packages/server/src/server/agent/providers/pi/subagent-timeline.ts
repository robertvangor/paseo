import { open } from "node:fs/promises";
import { join } from "node:path";
import type { Logger } from "pino";

import type { AgentProvider, AgentStreamEvent } from "../../agent-sdk-types.js";
import { PiHistoryMapper } from "./history-mapper.js";
import type { PiAgentMessage } from "./rpc-types.js";

const READ_BUFFER_BYTES = 64 * 1024;
const DEFAULT_POLL_INTERVAL_MS = 250;
const DEFAULT_TERMINAL_DRAIN_MS = 1_000;

type PiSubagentTimelineLogger = Pick<Logger, "debug">;

export interface PiSubagentEventReaderOptions {
  id: string;
  asyncDir: string;
  provider: AgentProvider;
  emit: (event: AgentStreamEvent) => void;
  logger: PiSubagentTimelineLogger;
}

export class PiSubagentEventReader {
  private readonly filePath: string;
  private readonly mappers = new Map<string, PiHistoryMapper>();
  private decoder = new TextDecoder();
  private pendingText = "";
  private offset = 0;
  private reading = false;
  private closed = false;

  constructor(private readonly options: PiSubagentEventReaderOptions) {
    this.filePath = join(options.asyncDir, "events.jsonl");
  }

  async readAvailable(): Promise<void> {
    if (this.closed || this.reading) return;
    this.reading = true;
    try {
      await this.readFile();
    } catch (error) {
      if (!isMissingFileError(error)) {
        this.options.logger.debug(
          { err: error, filePath: this.filePath, subagentId: this.options.id },
          "Pi subagent event read failed",
        );
      }
    } finally {
      this.reading = false;
    }
  }

  close(): void {
    this.closed = true;
    this.pendingText = "";
    this.mappers.clear();
  }

  private async readFile(): Promise<void> {
    const handle = await open(this.filePath, "r");
    try {
      const stat = await handle.stat();
      if (stat.size < this.offset) {
        this.offset = 0;
        this.pendingText = "";
        this.decoder = new TextDecoder();
      }
      const buffer = Buffer.allocUnsafe(READ_BUFFER_BYTES);
      while (this.offset < stat.size) {
        const length = Math.min(buffer.byteLength, stat.size - this.offset);
        const { bytesRead } = await handle.read(buffer, 0, length, this.offset);
        if (bytesRead === 0) break;
        this.offset += bytesRead;
        this.consumeText(this.decoder.decode(buffer.subarray(0, bytesRead), { stream: true }));
      }
    } finally {
      await handle.close();
    }
  }

  private consumeText(text: string): void {
    this.pendingText += text;
    const lines = this.pendingText.split("\n");
    this.pendingText = lines.pop() ?? "";
    for (const line of lines) {
      this.consumeLine(line);
    }
  }

  private consumeLine(line: string): void {
    if (!line.trim()) return;
    let event: Record<string, unknown>;
    try {
      const parsed = JSON.parse(line) as unknown;
      if (!isRecord(parsed)) return;
      event = parsed;
    } catch (error) {
      this.options.logger.debug(
        { err: error, filePath: this.filePath, subagentId: this.options.id },
        "Pi subagent event line is not valid JSON",
      );
      return;
    }
    if (event.subagentSource !== "child") return;
    if (event.type !== "message_end" && event.type !== "tool_result_end") return;
    const message = parsePiAgentMessage(event.message);
    if (!message) return;
    const stepKey = resolveStepKey(event);
    const mapper = this.mapperFor(stepKey);
    const timestamp = eventTimestamp(event);
    for (const mapped of mapper.mapMessages([message])) {
      if (mapped.type !== "timeline") continue;
      this.options.emit({
        type: "provider_subagent",
        provider: this.options.provider,
        event: {
          type: "timeline",
          id: this.options.id,
          item: mapped.item,
          ...(timestamp ? { timestamp } : {}),
        },
      });
    }
  }

  private mapperFor(stepKey: string): PiHistoryMapper {
    const existing = this.mappers.get(stepKey);
    if (existing) return existing;
    const mapper = new PiHistoryMapper(this.options.provider, [], {
      resolveToolCallId: (toolCallId) => `${this.options.id}:${stepKey}:${toolCallId}`,
    });
    this.mappers.set(stepKey, mapper);
    return mapper;
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
  private readonly pollIntervalMs: number;
  private readonly terminalDrainMs: number;
  private timer: NodeJS.Timeout | null = null;
  private polling = false;

  constructor(private readonly options: PiSubagentTimelineBridgeOptions) {
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.terminalDrainMs = options.terminalDrainMs ?? DEFAULT_TERMINAL_DRAIN_MS;
  }

  observe(id: string, asyncDir: string): void {
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

  complete(id: string): void {
    const run = this.runs.get(id);
    if (!run) return;
    run.terminalAt = Date.now() + this.terminalDrainMs;
    void this.poll();
  }

  close(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const run of this.runs.values()) run.reader.close();
    this.runs.clear();
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
  let value: number | undefined;
  if (typeof event.observedAt === "number") value = event.observedAt;
  else if (typeof event.ts === "number") value = event.ts;
  if (value === undefined || !Number.isFinite(value)) return undefined;
  return new Date(value).toISOString();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMissingFileError(error: unknown): boolean {
  return isRecord(error) && error.code === "ENOENT";
}
