import { beforeEach, describe, expect, it, vi } from "vitest";

import type { MsgContext } from "../auto-reply/templating.js";
import type { ClawdbotConfig } from "../config/config.js";
import { assertPublicHostname } from "../infra/net/ssrf.js";
import { runExec } from "../process/exec.js";

vi.mock("../process/exec.js", () => ({
  runExec: vi.fn(),
}));
vi.mock("../infra/net/ssrf.js", async () => {
  const actual =
    await vi.importActual<typeof import("../infra/net/ssrf.js")>("../infra/net/ssrf.js");
  return {
    ...actual,
    assertPublicHostname: vi.fn(),
  };
});

async function loadRunner() {
  return await import("./runner.js");
}

describe("runLinkUnderstanding", () => {
  const mockedRunExec = vi.mocked(runExec);
  const mockedAssertPublicHostname = vi.mocked(assertPublicHostname);

  beforeEach(() => {
    mockedRunExec.mockReset();
    mockedAssertPublicHostname.mockReset();
    mockedAssertPublicHostname.mockResolvedValue(undefined);
  });

  it("falls back to the next model when a CLI entry fails", async () => {
    const { runLinkUnderstanding } = await loadRunner();
    mockedRunExec.mockImplementation(async (command) => {
      if (command === "fail") throw new Error("boom");
      return { stdout: "summary", stderr: "" };
    });

    const cfg: ClawdbotConfig = {
      tools: {
        links: {
          enabled: true,
          models: [{ command: "fail" }, { command: "ok" }],
        },
      },
    };
    const ctx: MsgContext = {
      Body: "see https://example.com",
      SessionKey: "session-1",
      Surface: "discord",
      ChatType: "direct",
    };

    const result = await runLinkUnderstanding({ cfg, ctx });
    expect(result.urls).toEqual(["https://example.com"]);
    expect(result.outputs).toEqual([{ url: "https://example.com", text: "summary", source: "ok" }]);
  });

  it("skips links that fail the public hostname check", async () => {
    const { runLinkUnderstanding } = await loadRunner();
    mockedAssertPublicHostname.mockRejectedValueOnce(new Error("Blocked: private/internal IP"));

    const cfg: ClawdbotConfig = {
      tools: {
        links: {
          enabled: true,
          models: [{ command: "ok" }],
        },
      },
    };
    const ctx: MsgContext = {
      Body: "see https://intranet.example.com",
      SessionKey: "session-1",
      Surface: "discord",
      ChatType: "direct",
    };

    const result = await runLinkUnderstanding({ cfg, ctx });
    expect(result.outputs).toEqual([]);
    expect(result.decisions[0]?.urls[0]?.attempts[0]?.reason).toContain("Blocked");
  });
});
