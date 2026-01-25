import { describe, expect, it } from "vitest";

import { formatLinkUnderstandingBody } from "./format.js";

describe("formatLinkUnderstandingBody", () => {
  it("appends a structured link summary with source", () => {
    const body = formatLinkUnderstandingBody({
      body: "Check this",
      outputs: [{ url: "https://example.com", text: "Summary here", source: "link-cli" }],
    });
    expect(body).toBe(
      "Check this\n\n[Link]\nURL: https://example.com\nSource: link-cli\nSummary:\nSummary here",
    );
  });

  it("numbers multiple links and omits empty body", () => {
    const body = formatLinkUnderstandingBody({
      outputs: [
        { url: "https://a.example", text: "First", source: "cli-a" },
        { url: "https://b.example", text: "Second", source: "cli-b" },
      ],
    });
    expect(body).toBe(
      "[Link 1/2]\nURL: https://a.example\nSource: cli-a\nSummary:\nFirst\n\n[Link 2/2]\nURL: https://b.example\nSource: cli-b\nSummary:\nSecond",
    );
  });
});
