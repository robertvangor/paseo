import {
  appendFileSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
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
  test("streams workflow child sessions discovered through status.json", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const sessionPath = join(asyncDir, "child-session.jsonl");
    const emitted: AgentStreamEvent[] = [];
    const reader = new PiSubagentEventReader({
      id: "run-workflow",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
      contextWindowForModel: () => 1_000_000,
    });

    writeFileSync(
      join(asyncDir, "events.jsonl"),
      `${JSON.stringify({ type: "subagent.workflow.started", runId: "run-workflow" })}\n`,
    );
    writeFileSync(
      sessionPath,
      [
        { type: "session", timestamp: "2026-08-20T12:00:00.000Z" },
        {
          type: "message",
          timestamp: "2026-08-20T12:00:01.000Z",
          message: { role: "user", content: [{ type: "text", text: "Inspect the adapter" }] },
        },
        {
          type: "message",
          timestamp: "2026-08-20T12:00:02.000Z",
          message: {
            role: "assistant",
            responseId: "response-workflow",
            usage: {
              input: 1_000,
              output: 234,
              totalTokens: 1_234,
              cost: { total: 0.0042 },
            },
            content: [
              { type: "thinking", thinking: "Read the implementation" },
              { type: "text", text: "The adapter is correct." },
            ],
          },
        },
      ]
        .map((entry) => JSON.stringify(entry))
        .join("\n") + "\n",
    );
    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "run-workflow",
        mode: "workflow",
        state: "complete",
        cwd: "/workspace",
        startedAt: Date.parse("2026-08-20T12:00:00.000Z"),
        lastUpdate: Date.parse("2026-08-20T12:00:03.000Z"),
        steps: [
          {
            index: 0,
            agent: "delegate",
            description: "Inspect the adapter",
            status: "completed",
            model: "gemini-3.7-flash",
            thinking: "medium",
            sessionFile: sessionPath,
          },
        ],
      }),
    );

    await reader.readAvailable();

    expect(emitted[0]).toEqual({
      type: "provider_subagent",
      provider: "pi",
      event: { type: "remove", id: "run-workflow" },
    });
    expect(emitted[1]).toEqual({
      type: "provider_subagent",
      provider: "pi",
      event: {
        type: "upsert",
        id: "run-workflow:0:delegate",
        status: "completed",
        canStop: false,
        title: "delegate",
        description: "Inspect the adapter",
        subtitle: "gemini-3.7-flash · medium · 1m context",
        cwd: "/workspace",
        timestamp: "2026-08-20T12:00:03.000Z",
      },
    });
    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" && event.event.type === "timeline"
          ? [event.event.item]
          : [],
      ),
    ).toEqual([
      { type: "user_message", text: "Inspect the adapter" },
      { type: "reasoning", text: "Read the implementation" },
      {
        type: "assistant_message",
        text: "The adapter is correct.",
        messageId: "response-workflow",
      },
    ]);
    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" &&
        event.event.type === "upsert" &&
        event.event.subtitle?.includes("tokens")
          ? [event.event.subtitle]
          : [],
      ),
    ).toEqual(["gemini-3.7-flash · medium · 1.2k / 1m context · 1.2k tokens · $0.0042"]);
    expect(reader.isTerminal()).toBe(true);
    reader.close();
  });

  test("creates one descriptor and timeline for each parallel workflow child", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const emitted: AgentStreamEvent[] = [];
    const steps = ["gemini-flash-1", "gemini-flash-2", "gemini-flash-3"].map((label, index) => {
      const sessionFile = join(asyncDir, `${label}.jsonl`);
      writeFileSync(
        sessionFile,
        `${JSON.stringify({
          type: "message",
          timestamp: `2026-08-20T12:00:0${index + 1}.000Z`,
          message: {
            role: "assistant",
            responseId: `response-${index + 1}`,
            usage: {
              input: 3_728,
              output: 452 + index * 66,
              totalTokens: 4_180 + index * 66,
              cost: { total: [0.008982, 0.009477, 0.0099945][index] },
            },
            content: [{ type: "text", text: `Answer ${index + 1}` }],
          },
        })}\n`,
      );
      return {
        agent: "delegate",
        label,
        workflowKey: label,
        status: "completed",
        model: "gemini-3.6-flash",
        thinking: "medium",
        sessionFile,
      };
    });
    writeFileSync(join(asyncDir, "events.jsonl"), "");
    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "parallel-run",
        mode: "workflow",
        state: "complete",
        cwd: "/workspace",
        lastUpdate: Date.parse("2026-08-20T12:00:04.000Z"),
        steps,
      }),
    );
    const reader = new PiSubagentEventReader({
      id: "parallel-run",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
      contextWindowForModel: () => 1_000_000,
    });

    await reader.readAvailable();

    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" && event.event.type === "upsert" && event.event.title
          ? [{ id: event.event.id, title: event.event.title }]
          : [],
      ),
    ).toEqual([
      { id: "parallel-run:0:delegate", title: "gemini-flash-1" },
      { id: "parallel-run:1:delegate", title: "gemini-flash-2" },
      { id: "parallel-run:2:delegate", title: "gemini-flash-3" },
    ]);
    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" &&
        event.event.type === "timeline" &&
        event.event.item.type === "assistant_message"
          ? [{ id: event.event.id, text: event.event.item.text }]
          : [],
      ),
    ).toEqual([
      { id: "parallel-run:0:delegate", text: "Answer 1" },
      { id: "parallel-run:1:delegate", text: "Answer 2" },
      { id: "parallel-run:2:delegate", text: "Answer 3" },
    ]);
    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" &&
        event.event.type === "upsert" &&
        event.event.subtitle?.includes("tokens")
          ? [{ id: event.event.id, subtitle: event.event.subtitle }]
          : [],
      ),
    ).toEqual([
      {
        id: "parallel-run:0:delegate",
        subtitle: "gemini-3.6-flash · medium · 4.2k / 1m context · 4.2k tokens · $0.0090",
      },
      {
        id: "parallel-run:1:delegate",
        subtitle: "gemini-3.6-flash · medium · 4.2k / 1m context · 4.2k tokens · $0.0095",
      },
      {
        id: "parallel-run:2:delegate",
        subtitle: "gemini-3.6-flash · medium · 4.3k / 1m context · 4.3k tokens · $0.0100",
      },
    ]);
    reader.close();
  });

  test("requests a portable stop for a running async run", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const emitted: AgentStreamEvent[] = [];
    writeFileSync(join(asyncDir, "events.jsonl"), "");
    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "run-stop",
        mode: "single",
        state: "running",
        agent: "delegate",
        model: "gemini-3.7-flash",
        pid: 42,
      }),
    );
    const reader = new PiSubagentEventReader({
      id: "run-stop",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
    });

    await reader.readAvailable();
    await reader.stop("run-stop");

    expect(JSON.parse(readFileSync(join(asyncDir, "control", "stop.json"), "utf8"))).toMatchObject({
      type: "stop",
      source: "paseo",
    });
    expect(emitted.at(-1)).toEqual({
      type: "provider_subagent",
      provider: "pi",
      event: { type: "upsert", id: "run-stop", canStop: false },
    });

    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "run-stop",
        mode: "single",
        state: "stopped",
        agent: "delegate",
        model: "gemini-3.7-flash",
        pid: 42,
      }),
    );
    await reader.readAvailable();

    expect(emitted.at(-1)).toEqual({
      type: "provider_subagent",
      provider: "pi",
      event: expect.objectContaining({
        type: "upsert",
        id: "run-stop",
        status: "canceled",
        canStop: false,
      }),
    });
    reader.close();
  });

  test("uses the Pi control bridge when stopping a managed workflow", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const emitted: AgentStreamEvent[] = [];
    const requestStop = vi.fn(async () => undefined);
    writeFileSync(join(asyncDir, "events.jsonl"), "");
    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "workflow-stop",
        mode: "workflow",
        state: "running",
        pid: 42,
        steps: [{ key: "main", agent: "delegate", status: "running" }],
      }),
    );
    const reader = new PiSubagentEventReader({
      id: "workflow-stop",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
      requestStop,
    });

    await reader.readAvailable();
    await reader.stop("workflow-stop:0:delegate");

    expect(requestStop).toHaveBeenCalledWith("workflow-stop", asyncDir);
    expect(existsSync(join(asyncDir, "control", "stop.json"))).toBe(false);
    expect(emitted.at(-1)).toEqual({
      type: "provider_subagent",
      provider: "pi",
      event: { type: "upsert", id: "workflow-stop:0:delegate", canStop: false },
    });
    reader.close();
  });

  test("keeps current context separate from cumulative billed tokens", async () => {
    const asyncDir = mkdtempSync(join(tmpdir(), "paseo-pi-subagent-test-"));
    tempDirs.push(asyncDir);
    const sessionPath = join(asyncDir, "child-session.jsonl");
    const emitted: AgentStreamEvent[] = [];
    writeFileSync(join(asyncDir, "events.jsonl"), "");
    writeFileSync(
      sessionPath,
      [
        { totalTokens: 2_000, cost: 0.01, text: "First" },
        { totalTokens: 3_000, cost: 0.02, text: "Second" },
      ]
        .map(({ totalTokens, cost, text }, index) =>
          JSON.stringify({
            type: "message",
            message: {
              role: "assistant",
              responseId: `response-${index}`,
              usage: { totalTokens, cost: { total: cost } },
              content: [{ type: "text", text }],
            },
          }),
        )
        .join("\n") + "\n",
    );
    writeFileSync(
      join(asyncDir, "status.json"),
      JSON.stringify({
        runId: "run-context",
        mode: "single",
        state: "complete",
        agent: "delegate",
        model: "gemini-3.7-flash",
        sessionFile: sessionPath,
      }),
    );
    const reader = new PiSubagentEventReader({
      id: "run-context",
      asyncDir,
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() } as unknown as Logger,
      contextWindowForModel: () => 1_000_000,
    });

    await reader.readAvailable();

    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" &&
        event.event.type === "upsert" &&
        event.event.subtitle?.includes("tokens")
          ? [event.event.subtitle]
          : [],
      ),
    ).toEqual([
      "gemini-3.7-flash · 2k / 1m context · 2k tokens · $0.01",
      "gemini-3.7-flash · 3k / 1m context · 5k tokens · $0.03",
    ]);
    reader.close();
  });

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
