import type { ClawdbotConfig } from "../config/config.js";
import type { MsgContext } from "../auto-reply/templating.js";
import { finalizeInboundContext } from "../auto-reply/reply/inbound-context.js";
import { formatLinkUnderstandingBody, formatLinkUnderstandingSections } from "./format.js";
import { runLinkUnderstanding } from "./runner.js";
import type { LinkUnderstandingDecision, LinkUnderstandingOutput } from "./types.js";

export type ApplyLinkUnderstandingResult = {
  outputs: LinkUnderstandingOutput[];
  urls: string[];
  decisions: LinkUnderstandingDecision[];
};

export async function applyLinkUnderstanding(params: {
  ctx: MsgContext;
  cfg: ClawdbotConfig;
}): Promise<ApplyLinkUnderstandingResult> {
  const result = await runLinkUnderstanding({
    cfg: params.cfg,
    ctx: params.ctx,
  });

  if (result.decisions.length > 0) {
    params.ctx.LinkUnderstandingDecisions = [
      ...(params.ctx.LinkUnderstandingDecisions ?? []),
      ...result.decisions,
    ];
  }

  if (result.outputs.length === 0) {
    return result;
  }

  const originalBody = params.ctx.Body;
  const sections = formatLinkUnderstandingSections(result.outputs);
  if (sections.length > 0) {
    params.ctx.LinkUnderstanding = [...(params.ctx.LinkUnderstanding ?? []), ...sections];
  }
  params.ctx.Body = formatLinkUnderstandingBody({
    body: params.ctx.Body,
    outputs: result.outputs,
  });
  if (!params.ctx.CommandBody && !params.ctx.RawBody && typeof originalBody === "string") {
    params.ctx.RawBody = originalBody;
  }
  finalizeInboundContext(params.ctx, {
    forceBodyForAgent: true,
    forceBodyForCommands: true,
  });

  return result;
}
