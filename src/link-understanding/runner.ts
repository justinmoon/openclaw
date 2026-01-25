import type { ClawdbotConfig } from "../config/config.js";
import type { MsgContext } from "../auto-reply/templating.js";
import { applyTemplate } from "../auto-reply/templating.js";
import type { LinkModelConfig, LinkToolsConfig } from "../config/types.tools.js";
import { logVerbose, shouldLogVerbose } from "../globals.js";
import { runExec } from "../process/exec.js";
import { CLI_OUTPUT_MAX_BUFFER } from "../media-understanding/defaults.js";
import { resolveTimeoutMs } from "../media-understanding/resolve.js";
import {
  normalizeMediaUnderstandingChatType,
  resolveMediaUnderstandingScope,
} from "../media-understanding/scope.js";
import { assertPublicHostname } from "../infra/net/ssrf.js";
import { DEFAULT_LINK_TIMEOUT_SECONDS } from "./defaults.js";
import { extractLinksFromMessage } from "./detect.js";
import type {
  LinkUnderstandingDecision,
  LinkUnderstandingModelDecision,
  LinkUnderstandingOutput,
  LinkUnderstandingUrlDecision,
} from "./types.js";

export type LinkUnderstandingResult = {
  urls: string[];
  outputs: LinkUnderstandingOutput[];
  decisions: LinkUnderstandingDecision[];
};

function resolveScopeDecision(params: {
  config?: LinkToolsConfig;
  ctx: MsgContext;
}): "allow" | "deny" {
  return resolveMediaUnderstandingScope({
    scope: params.config?.scope,
    sessionKey: params.ctx.SessionKey,
    channel: params.ctx.Surface ?? params.ctx.Provider,
    chatType: normalizeMediaUnderstandingChatType(params.ctx.ChatType),
  });
}

function resolveTimeoutMsFromConfig(params: {
  config?: LinkToolsConfig;
  entry: LinkModelConfig;
}): number {
  const configured = params.entry.timeoutSeconds ?? params.config?.timeoutSeconds;
  return resolveTimeoutMs(configured, DEFAULT_LINK_TIMEOUT_SECONDS);
}

async function runCliEntry(params: {
  entry: LinkModelConfig;
  ctx: MsgContext;
  url: string;
  config?: LinkToolsConfig;
}): Promise<string | null> {
  if ((params.entry.type ?? "cli") !== "cli") return null;
  const command = params.entry.command.trim();
  if (!command) return null;
  const args = params.entry.args ?? [];
  const timeoutMs = resolveTimeoutMsFromConfig({ config: params.config, entry: params.entry });
  const templCtx = {
    ...params.ctx,
    LinkUrl: params.url,
  };
  const argv = [command, ...args].map((part, index) =>
    index === 0 ? part : applyTemplate(part, templCtx),
  );

  if (shouldLogVerbose()) {
    logVerbose(`Link understanding via CLI: ${argv.join(" ")}`);
  }

  const { stdout } = await runExec(argv[0], argv.slice(1), {
    timeoutMs,
    maxBuffer: CLI_OUTPUT_MAX_BUFFER,
  });
  const trimmed = stdout.trim();
  return trimmed || null;
}

function buildModelDecision(params: {
  entry: LinkModelConfig;
  outcome: LinkUnderstandingModelDecision["outcome"];
  reason?: string;
}): LinkUnderstandingModelDecision {
  const command = params.entry.command?.trim();
  return {
    type: "cli",
    command: command || undefined,
    outcome: params.outcome,
    reason: params.reason,
  };
}

async function assertUrlIsPublic(
  url: string,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  try {
    const parsed = new URL(url);
    await assertPublicHostname(parsed.hostname);
    return { ok: true };
  } catch (err) {
    return { ok: false, reason: String(err) };
  }
}

async function runLinkEntries(params: {
  entries: LinkModelConfig[];
  ctx: MsgContext;
  url: string;
  config?: LinkToolsConfig;
}): Promise<{
  output: LinkUnderstandingOutput | null;
  attempts: LinkUnderstandingModelDecision[];
}> {
  const attempts: LinkUnderstandingModelDecision[] = [];
  for (const entry of params.entries) {
    try {
      const output = await runCliEntry({
        entry,
        ctx: params.ctx,
        url: params.url,
        config: params.config,
      });
      if (output) {
        const decision = buildModelDecision({ entry, outcome: "success" });
        attempts.push(decision);
        return {
          output: {
            url: params.url,
            text: output,
            source: entry.command?.trim() || undefined,
          },
          attempts,
        };
      }
      attempts.push(buildModelDecision({ entry, outcome: "skipped", reason: "empty output" }));
    } catch (err) {
      if (shouldLogVerbose()) {
        logVerbose(`Link understanding failed for ${params.url}: ${String(err)}`);
      }
      attempts.push(
        buildModelDecision({
          entry,
          outcome: "failed",
          reason: String(err),
        }),
      );
    }
  }
  return { output: null, attempts };
}

export async function runLinkUnderstanding(params: {
  cfg: ClawdbotConfig;
  ctx: MsgContext;
  message?: string;
}): Promise<LinkUnderstandingResult> {
  const config = params.cfg.tools?.links;
  if (!config || config.enabled === false) {
    return { urls: [], outputs: [], decisions: [{ outcome: "disabled", urls: [] }] };
  }

  const scopeDecision = resolveScopeDecision({ config, ctx: params.ctx });
  if (scopeDecision === "deny") {
    if (shouldLogVerbose()) {
      logVerbose("Link understanding disabled by scope policy.");
    }
    return { urls: [], outputs: [], decisions: [{ outcome: "scope-deny", urls: [] }] };
  }

  const message = params.message ?? params.ctx.CommandBody ?? params.ctx.RawBody ?? params.ctx.Body;
  const links = extractLinksFromMessage(message ?? "", { maxLinks: config?.maxLinks });
  if (links.length === 0) {
    return { urls: [], outputs: [], decisions: [{ outcome: "no-links", urls: [] }] };
  }

  const entries = config?.models ?? [];
  if (entries.length === 0) {
    const urlDecisions: LinkUnderstandingUrlDecision[] = links.map((url) => ({
      url,
      attempts: [],
    }));
    return {
      urls: links,
      outputs: [],
      decisions: [{ outcome: "skipped", urls: urlDecisions }],
    };
  }

  const outputs: LinkUnderstandingOutput[] = [];
  const urlDecisions: LinkUnderstandingUrlDecision[] = [];
  for (const url of links) {
    const ssrfCheck = await assertUrlIsPublic(url);
    if (!ssrfCheck.ok) {
      urlDecisions.push({
        url,
        attempts: [
          {
            type: "cli",
            command: "ssrf",
            outcome: "skipped",
            reason: ssrfCheck.reason,
          },
        ],
      });
      continue;
    }
    const { output, attempts } = await runLinkEntries({
      entries,
      ctx: params.ctx,
      url,
      config,
    });
    const chosen = attempts.find((attempt) => attempt.outcome === "success");
    urlDecisions.push({ url, attempts, chosen });
    if (output) outputs.push(output);
  }

  const outcome = outputs.length > 0 ? "success" : "skipped";
  return { urls: links, outputs, decisions: [{ outcome, urls: urlDecisions }] };
}
