import { describe, expect, it, vi } from "vitest";

import type { ClawdbotConfig } from "../config/config.js";
import type { MsgContext } from "../auto-reply/templating.js";
import { runLinkUnderstanding } from "./runner.js";

vi.mock("./runner.js", () => ({
  runLinkUnderstanding: vi.fn(),
}));

describe("applyLinkUnderstanding", () => {
  it("keeps command parsing bodies unchanged when link output is applied", async () => {
    const mockedRunLinkUnderstanding = vi.mocked(runLinkUnderstanding);
    mockedRunLinkUnderstanding.mockResolvedValue({
      urls: ["https://example.com"],
      outputs: [{ url: "https://example.com", text: "Summary", source: "link-cli" }],
      decisions: [],
    });

    const { applyLinkUnderstanding } = await import("./apply.js");
    const ctx: MsgContext = {
      Body: "check https://example.com",
      RawBody: "raw override",
      CommandBody: "/think low check https://example.com",
    };
    const cfg: ClawdbotConfig = {
      tools: { links: { models: [{ command: "link-cli" }] } },
    };

    await applyLinkUnderstanding({ ctx, cfg });

    expect(ctx.Body).toContain("[Link]");
    expect(ctx.CommandBody).toBe("/think low check https://example.com");
    expect(ctx.RawBody).toBe("raw override");
    expect(ctx.BodyForCommands).toBe("/think low check https://example.com");
    expect(ctx.BodyForCommands).not.toContain("[Link]");
  });

  it("preserves original body for command parsing when no overrides exist", async () => {
    const mockedRunLinkUnderstanding = vi.mocked(runLinkUnderstanding);
    mockedRunLinkUnderstanding.mockResolvedValue({
      urls: ["https://example.com"],
      outputs: [{ url: "https://example.com", text: "Summary", source: "link-cli" }],
      decisions: [],
    });

    const { applyLinkUnderstanding } = await import("./apply.js");
    const ctx: MsgContext = {
      Body: "check https://example.com",
    };
    const cfg: ClawdbotConfig = {
      tools: { links: { models: [{ command: "link-cli" }] } },
    };

    await applyLinkUnderstanding({ ctx, cfg });

    expect(ctx.Body).toContain("[Link]");
    expect(ctx.RawBody).toBe("check https://example.com");
    expect(ctx.BodyForCommands).toBe("check https://example.com");
  });
});
