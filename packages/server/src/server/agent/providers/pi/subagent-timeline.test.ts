import { appendFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Logger } from "pino";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { AgentStreamEvent } from "../../agent-sdk-types.js";
import { PiSubagentEventReader } from "./subagent-timeline.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("PiSubagentEventReader", () => {
  test("streams child messages, reasoning, and tool calls from async artifacts", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const eventsPath = join(asyncDir, "events.jsonl");
    const emitted: AgentStreamEvent[] = [];
    const logger = { debug: vi.fn() } as unknown as Logger;
    const reader = new PiSubagentEventReader({
      id: "run-1",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger,
    });

    writeFileSync(
      eventsPath,
      `${JSON.stringify({
        type: "message_end",
        message: {
          role: "assistant",
          responseId: "response-1",
          content: [
            { type: "thinking", thinking: "Inspect the implementation" },
            { type: "text", text: "I found the relevant file." },
            {
              type: "toolCall",
              id: "tool-1",
              name: "read",
              arguments: { path: "src/index.ts" },
            },
          ],
        },
        subagentSource: "child",
        subagentStepIndex: 0,
        subagentAgent: "reviewer",
        observedAt: 1_700_000_000_000,
      })}\n`,
    );

    await reader.readAvailable();

    appendFileSync(
      eventsPath,
      `${JSON.stringify({
        type: "tool_result_end",
        message: {
          role: "toolResult",
          toolCallId: "tool-1",
          toolName: "read",
          content: [{ type: "text", text: "export const value = 1;" }],
          isError: false,
        },
        subagentSource: "child",
        subagentStepIndex: 0,
        subagentAgent: "reviewer",
        observedAt: 1_700_000_001_000,
      })}\n`,
    );

    await reader.readAvailable();

    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" && event.event.type === "timeline"
          ? [event.event.item]
          : [],
      ),
    ).toEqual([
      { type: "reasoning", text: "Inspect the implementation" },
      {
        type: "assistant_message",
        text: "I found the relevant file.",
        messageId: "response-1",
      },
      {
        type: "tool_call",
        callId: "run-1:0:reviewer:tool-1",
        name: "read",
        status: "running",
        detail: { type: "read", filePath: "src/index.ts" },
        error: null,
      },
      {
        type: "tool_call",
        callId: "run-1:0:reviewer:tool-1",
        name: "read",
        status: "completed",
        detail: {
          type: "read",
          filePath: "src/index.ts",
          content: "export const value = 1;",
        },
        error: null,
      },
    ]);
    expect(logger.debug).not.toHaveBeenCalled();

    reader.close();
  });

  test("waits for a complete JSONL line", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const eventsPath = join(asyncDir, "events.jsonl");
    const emitted: AgentStreamEvent[] = [];
    const reader = new PiSubagentEventReader({
      id: "run-2",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
    });
    const line = JSON.stringify({
      type: "message_end",
      message: { role: "assistant", content: [{ type: "text", text: "done" }] },
      subagentSource: "child",
    });
    const split = Math.floor(line.length / 2);
    writeFileSync(eventsPath, line.slice(0, split));

    await reader.readAvailable();
    expect(emitted).toEqual([]);

    appendFileSync(eventsPath, `${line.slice(split)}\n`);
    await reader.readAvailable();

    expect(emitted).toHaveLength(1);
    reader.close();
  });
});
