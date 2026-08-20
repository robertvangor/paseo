import { i18n as testI18n } from "@/i18n/i18next";
import { WorkspaceNewAgentButton } from "@/screens/workspace/workspace-new-agent-button";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

void testI18n;

afterEach(cleanup);

describe("WorkspaceNewAgentButton", () => {
  it("creates an agent directly when the plus button is clicked", () => {
    const onCreateAgentTab = vi.fn();

    render(
      <WorkspaceNewAgentButton
        shortcutKeys={null}
        onCreateAgentTab={onCreateAgentTab}
        onLayout={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "New agent" }));

    expect(onCreateAgentTab).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("workspace-new-tab-menu-agent")).toBeNull();
  });
});
