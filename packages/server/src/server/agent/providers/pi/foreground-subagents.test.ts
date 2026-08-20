import { describe, expect, test } from "vitest";

import { PiForegroundSubagentIndex } from "./foreground-subagents.js";
import { parseToolArgs, parseToolResult } from "./tool-call-mapper.js";

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

  test("leaves asynchronous calls to the artifact-backed bridge", () => {
    const index = new PiForegroundSubagentIndex({ provider: "pi" });
    const toolCall = parseToolArgs("subagent", {
      agent: "reviewer",
      task: "Review the adapter",
      async: true,
    });

    expect(index.handle("parent-tool", toolCall, "running", null)).toEqual([]);
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
