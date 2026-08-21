import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test, vi } from "vitest";

import type { AgentStreamEvent } from "../../agent-sdk-types.js";

import { PiForegroundSubagentIndex, readPiAsyncSubagentRun } from "./foreground-subagents.js";
import { parseToolArgs, parseToolResult } from "./tool-call-mapper.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("PiForegroundSubagentIndex", () => {
  test("opens a live child timeline and hydrates its final transcript", () => {
    const index = new PiForegroundSubagentIndex({ provider: "pi" });
    const toolCall = parseToolArgs("subagent", {
      agent: "reviewer",
      task: "Review the adapter",
    });

    expect(index.handle("parent-tool", toolCall, "running", null)).toEqual([
      {
        type: "provider_subagent",
        provider: "pi",
        event: {
          type: "upsert",
          id: "parent-tool:0",
          title: "reviewer",
          description: "Review the adapter",
          status: "running",
          canStop: true,
          toolCallId: "parent-tool",
        },
      },
    ]);

    const progressEvents = index.handle(
      "parent-tool",
      toolCall,
      "running",
      parseToolResult({
        content: [{ type: "text", text: "(running...)" }],
        details: {
          runId: "run-1",
          results: [
            {
              index: 0,
              agent: "reviewer",
              task: "[prompt redacted]",
              model: "openai/gpt-5",
              thinking: "high",
              toolCalls: [{ text: "read src/a.ts", expandedText: "read: src/a.ts" }],
            },
          ],
          progress: [
            {
              index: 0,
              agent: "reviewer",
              status: "running",
              currentTool: "grep",
              currentToolArgs: "adapter",
              recentOutput: ["Found two matches"],
            },
          ],
        },
      }),
    );

    expect(progressEvents).toContainEqual({
      type: "provider_subagent",
      provider: "pi",
      event: {
        type: "timeline",
        id: "parent-tool:0",
        item: {
          type: "tool_call",
          callId: "parent-tool:0:live-progress",
          name: "subagent_progress",
          status: "running",
          detail: {
            type: "plain_text",
            label: "Live activity",
            text: "grep: adapter\nread: src/a.ts\nFound two matches",
          },
          error: null,
        },
      },
    });
    expect(progressEvents).toContainEqual(
      expect.objectContaining({
        event: expect.objectContaining({ subtitle: "openai/gpt-5 · high" }),
      }),
    );

    const completedEvents = index.handle(
      "parent-tool",
      toolCall,
      "completed",
      parseToolResult({
        content: [{ type: "text", text: "Review complete" }],
        details: {
          runId: "run-1",
          results: [
            {
              index: 0,
              agent: "reviewer",
              exitCode: 0,
              messages: [
                {
                  role: "assistant",
                  responseId: "child-response",
                  content: [
                    { type: "thinking", thinking: "Check the edge case" },
                    { type: "text", text: "The adapter is correct." },
                  ],
                },
              ],
            },
          ],
        },
      }),
    );

    expect(completedEvents).toContainEqual({
      type: "provider_subagent",
      provider: "pi",
      event: {
        type: "timeline",
        id: "parent-tool:0",
        item: { type: "reasoning", text: "Check the edge case" },
      },
    });
    expect(completedEvents).toContainEqual({
      type: "provider_subagent",
      provider: "pi",
      event: {
        type: "timeline",
        id: "parent-tool:0",
        item: {
          type: "assistant_message",
          text: "The adapter is correct.",
          messageId: "child-response",
        },
      },
    });
    expect(completedEvents).toContainEqual(
      expect.objectContaining({
        event: expect.objectContaining({ status: "completed" }),
      }),
    );
  });

  test("streams a foreground child session before the tool completes", async () => {
    const sessionDir = mkdtempSync(join(tmpdir(), "paseo-pi-foreground-session-test-"));
    tempDirs.push(sessionDir);
    const parentSessionFile = join(sessionDir, "parent.jsonl");
    writeFileSync(parentSessionFile, "");
    const emitted: AgentStreamEvent[] = [];
    const index = new PiForegroundSubagentIndex({
      provider: "pi",
      emit: (event) => emitted.push(event),
      logger: { debug: vi.fn() },
      parentSessionFile: () => parentSessionFile,
      contextWindowForModel: () => 1_000_000,
      pollIntervalMs: 60_000,
    });
    const toolCall = parseToolArgs("subagent", {
      agent: "reviewer",
      task: "Inspect the adapter",
    });
    const assistantMessage = {
      role: "assistant",
      responseId: "child-response",
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
    };

    index.handle("parent-tool", toolCall, "running", null);

    const childDir = join(sessionDir, "parent", "run-1", "run-0");
    mkdirSync(childDir, { recursive: true });
    writeFileSync(
      join(childDir, "session.jsonl"),
      [
        {
          type: "session",
          id: "child-session",
          timestamp: "2026-08-21T12:00:00.000Z",
          cwd: "/workspace",
        },
        {
          type: "model_change",
          timestamp: "2026-08-21T12:00:00.100Z",
          provider: "google",
          modelId: "gemini-3.1-pro-preview",
        },
        {
          type: "thinking_level_change",
          timestamp: "2026-08-21T12:00:00.200Z",
          thinkingLevel: "high",
        },
        {
          type: "session_info",
          timestamp: "2026-08-21T12:00:00.300Z",
          name: "subagent-reviewer-run-1-1",
        },
        {
          type: "message",
          timestamp: "2026-08-21T12:00:01.000Z",
          message: { role: "user", content: [{ type: "text", text: "Task: Inspect the adapter" }] },
        },
        {
          type: "message",
          timestamp: "2026-08-21T12:00:02.000Z",
          message: assistantMessage,
        },
      ]
        .map((entry) => JSON.stringify(entry))
        .join("\n") + "\n",
    );

    await index.flushSessions();

    expect(
      emitted.flatMap((event) =>
        event.type === "provider_subagent" && event.event.type === "timeline"
          ? [event.event.item]
          : [],
      ),
    ).toEqual([
      { type: "user_message", text: "Task: Inspect the adapter" },
      { type: "reasoning", text: "Read the implementation" },
      {
        type: "assistant_message",
        text: "The adapter is correct.",
        messageId: "child-response",
      },
    ]);
    expect(emitted).toContainEqual({
      type: "provider_subagent",
      provider: "pi",
      event: {
        type: "upsert",
        id: "parent-tool:0",
        subtitle:
          "google/gemini-3.1-pro-preview · high · 1.2k / 1m context · 1.2k tokens · $0.0042",
      },
    });

    const completed = index.handle(
      "parent-tool",
      toolCall,
      "completed",
      parseToolResult({
        details: {
          results: [
            {
              index: 0,
              agent: "reviewer",
              finalOutput: "The adapter is correct.",
            },
          ],
        },
      }),
    );
    expect(completed).not.toContainEqual(
      expect.objectContaining({ event: expect.objectContaining({ type: "timeline" }) }),
    );
    index.close();
  });

  test("disables a foreground child when its workflow is stopped", () => {
    const index = new PiForegroundSubagentIndex({ provider: "pi" });
    const toolCall = parseToolArgs("subagent", {
      agent: "reviewer",
      task: "Review one",
    });

    index.handle("parent-tool", toolCall, "running", null);

    expect(index.requestStop("parent-tool:0")).toEqual([
      {
        type: "provider_subagent",
        provider: "pi",
        event: { type: "upsert", id: "parent-tool:0", canStop: false },
      },
    ]);
    expect(index.requestStop("parent-tool:0")).toEqual([]);
    expect(index.requestStop("missing")).toBeNull();
  });

  test("leaves asynchronous calls to the artifact-backed bridge", () => {
    const index = new PiForegroundSubagentIndex({ provider: "pi" });
    const toolCall = parseToolArgs("subagent", {
      agent: "reviewer",
      task: "Review the adapter",
      async: true,
    });

    expect(index.handle("parent-tool", toolCall, "running", null)).toEqual([]);
  });

  test("reads the artifact location from an asynchronous launch result", () => {
    const toolCall = parseToolArgs("subagent", {
      agent: "delegate",
      task: "Inspect the adapter",
      model: "github-copilot/gemini-3.7-flash",
    });

    expect(
      readPiAsyncSubagentRun(
        "parent-tool",
        toolCall,
        parseToolResult({
          details: {
            mode: "workflow",
            runId: "run-1",
            asyncId: "run-1",
            asyncDir: "/tmp/run-1",
            results: [],
          },
        }),
      ),
    ).toEqual({
      id: "run-1",
      asyncDir: "/tmp/run-1",
      title: "delegate",
      description: "Inspect the adapter",
      subtitle: "github-copilot/gemini-3.7-flash",
      toolCallId: "parent-tool",
    });
  });

  test("marks any nonzero child exit code as failed", () => {
    const index = new PiForegroundSubagentIndex({ provider: "pi" });
    const toolCall = parseToolArgs("task", { agent: "reviewer", task: "Review" });
    const events = index.handle(
      "parent-tool",
      toolCall,
      "completed",
      parseToolResult({
        details: { results: [{ index: 0, agent: "reviewer", exitCode: 2 }] },
      }),
    );

    expect(events).toContainEqual(
      expect.objectContaining({
        event: expect.objectContaining({ status: "failed" }),
      }),
    );
  });
});
