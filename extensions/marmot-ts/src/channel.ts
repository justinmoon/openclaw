import fs from "node:fs";
import path from "node:path";
import { nip19 } from "nostr-tools";
import {
  buildChannelConfigSchema,
  createReplyPrefixOptions,
  DEFAULT_ACCOUNT_ID,
  formatPairingApproveHint,
  type ChannelPlugin,
  type ReplyPayload,
} from "openclaw/plugin-sdk";
import { MarmotTsConfigSchema } from "./config-schema.js";
import { startMarmotTsBus, type MarmotTsBusHandle } from "./marmot-bus.js";
import { getMarmotTsRuntime } from "./runtime.js";
import {
  listMarmotTsAccountIds,
  resolveDefaultMarmotTsAccountId,
  resolveMarmotTsAccount,
  type ResolvedMarmotTsAccount,
} from "./types.js";

const activeBuses = new Map<string, MarmotTsBusHandle>();
const peerToGroup = new Map<string, string>();
const groupToPeer = new Map<string, string>();
const selfKeys = new Map<string, { pubkeyHex: string; npub: string }>();

function resolveAccountStateDir(coreStateDir: string, accountId: string): string {
  return path.join(coreStateDir, "channels", "marmot-ts", accountId);
}

function extractExactReply(content: string): string | null {
  const m = content.match(/openclaw:\s*reply exactly\s+"([^"]*)"/i);
  return m?.[1] ?? null;
}

export function normalizePubkey(input: string): string {
  const trimmed = input.replace(/^marmot-ts:/i, "").trim();
  if (/^npub1/i.test(trimmed)) {
    const decoded = nip19.decode(trimmed);
    if (decoded.type !== "npub") {
      throw new Error("expected npub");
    }
    return String(decoded.data).toLowerCase();
  }
  if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
    return trimmed.toLowerCase();
  }
  throw new Error("invalid pubkey");
}

function isSenderAllowed(senderId: string, allowFrom: string[]): boolean {
  if (allowFrom.includes("*")) {
    return true;
  }
  const normalizedSender = senderId.trim().toLowerCase();
  return allowFrom.some((entry) => entry.trim().toLowerCase() === normalizedSender);
}

function normalizeAllowFromEntry(raw: string): string {
  const trimmed = raw.replace(/^marmot-ts:/i, "").trim();
  if (!trimmed) {
    return "";
  }
  if (trimmed === "*") {
    return "*";
  }
  try {
    // Prefer a stable normalized representation (hex pubkey).
    return normalizePubkey(trimmed);
  } catch {
    // Fall back to a plain string match (e.g. hex pubkey already).
    return trimmed.toLowerCase();
  }
}

export const marmotTsPlugin: ChannelPlugin<ResolvedMarmotTsAccount> = {
  id: "marmot-ts",
  meta: {
    id: "marmot-ts",
    label: "Marmot (TS)",
    selectionLabel: "Marmot (TypeScript MLS)",
    docsPath: "/channels/marmot",
    docsLabel: "marmot",
    blurb: "MLS-encrypted DMs over Nostr relays (Marmot Protocol) via marmot-ts.",
    order: 102,
  },
  capabilities: {
    chatTypes: ["direct"],
    media: false,
  },
  reload: { configPrefixes: ["channels.marmot-ts"] },
  configSchema: buildChannelConfigSchema(MarmotTsConfigSchema),

  config: {
    listAccountIds: (cfg) => listMarmotTsAccountIds(cfg),
    resolveAccount: (cfg, accountId) => resolveMarmotTsAccount({ cfg, accountId }),
    defaultAccountId: (cfg) => resolveDefaultMarmotTsAccountId(cfg),
    isConfigured: (account) => account.configured,
    describeAccount: (account) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: account.configured,
      publicKey: account.publicKey ?? null,
      npub: account.npub ?? null,
    }),
    resolveAllowFrom: ({ cfg, accountId }) =>
      (resolveMarmotTsAccount({ cfg, accountId }).config.allowFrom ?? []).map((entry) =>
        String(entry),
      ),
    formatAllowFrom: ({ allowFrom }) =>
      allowFrom
        .map((entry) => String(entry).trim())
        .filter(Boolean)
        .map((entry) => {
          if (entry === "*") {
            return "*";
          }
          try {
            return normalizePubkey(entry);
          } catch {
            return entry;
          }
        })
        .filter(Boolean),
  },

  pairing: {
    idLabel: "marmotTsPubkey",
    normalizeAllowEntry: (entry) => {
      try {
        return normalizePubkey(entry);
      } catch {
        return entry.replace(/^marmot-ts:/i, "").trim();
      }
    },
    notifyApproval: async ({ id }) => {
      const bus = activeBuses.get(DEFAULT_ACCOUNT_ID);
      if (!bus) {
        return;
      }
      const peer = id
        .replace(/^marmot-ts:/i, "")
        .trim()
        .toLowerCase();
      const groupId = peerToGroup.get(peer);
      if (!groupId) {
        return;
      }
      await bus.sendDmToGroup({
        mlsGroupId: groupId,
        content: "Your pairing request has been approved!",
      });
    },
  },

  security: {
    resolveDmPolicy: ({ account }) => ({
      policy: account.config.dmPolicy ?? "pairing",
      allowFrom: account.config.allowFrom ?? [],
      policyPath: "channels.marmot-ts.dmPolicy",
      allowFromPath: "channels.marmot-ts.allowFrom",
      approveHint: formatPairingApproveHint("marmot-ts"),
      normalizeEntry: (raw) => raw.replace(/^marmot-ts:/i, "").trim(),
    }),
  },

  messaging: {
    normalizeTarget: (target) => {
      const cleaned = target.replace(/^marmot-ts:/i, "").trim();
      try {
        return normalizePubkey(cleaned);
      } catch {
        return cleaned.toLowerCase();
      }
    },
    targetResolver: {
      looksLikeId: (input) => /^[0-9a-fA-F]{64}$/.test(input.trim()),
      hint: "<hex pubkey>",
    },
  },

  outbound: {
    deliveryMode: "direct",
    textChunkLimit: 4000,
    sendText: async ({ to, text, accountId }) => {
      const aid = accountId ?? DEFAULT_ACCOUNT_ID;
      const bus = activeBuses.get(aid);
      if (!bus) {
        throw new Error(`marmot-ts bus not running for account ${aid}`);
      }
      const peer = normalizePubkey(to);
      await bus.sendDmToPeer(peer, text ?? "");
      return { channel: "marmot-ts", to: peer };
    },
  },

  status: {
    defaultRuntime: {
      accountId: DEFAULT_ACCOUNT_ID,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },
    buildChannelSummary: ({ snapshot }) => ({
      configured: snapshot.configured ?? false,
      running: snapshot.running ?? false,
      publicKey: snapshot.publicKey ?? null,
      npub: snapshot.npub ?? null,
      lastStartAt: snapshot.lastStartAt ?? null,
      lastStopAt: snapshot.lastStopAt ?? null,
      lastError: snapshot.lastError ?? null,
    }),
    buildAccountSnapshot: ({ account, runtime }) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: account.configured,
      relays: account.relays,
      publicKey: runtime?.publicKey ?? account.publicKey ?? null,
      npub: runtime?.npub ?? account.npub ?? null,
      running: runtime?.running ?? false,
      lastStartAt: runtime?.lastStartAt ?? null,
      lastStopAt: runtime?.lastStopAt ?? null,
      lastError: runtime?.lastError ?? null,
      lastInboundAt: runtime?.lastInboundAt ?? null,
      lastOutboundAt: runtime?.lastOutboundAt ?? null,
    }),
  },

  gateway: {
    startAccount: async (ctx) => {
      const account = ctx.account;
      const runtime = getMarmotTsRuntime();
      const cfg = runtime.config.loadConfig();
      const coreStateDir = runtime.state.resolveStateDir(cfg);
      const stateDir = resolveAccountStateDir(coreStateDir, account.accountId);
      fs.mkdirSync(stateDir, { recursive: true });

      ctx.log?.info(`[${account.accountId}] starting marmot-ts bus (state: ${stateDir})`);

      const bus = await startMarmotTsBus({
        stateDir,
        relays: account.relays,
        privateKey: account.config.privateKey,
        onLog: (line) => ctx.log?.debug(line),
        onEvent: async (ev) => {
          if (ev.type === "ready") {
            selfKeys.set(account.accountId, { pubkeyHex: ev.pubkey_hex, npub: ev.npub });
            ctx.setStatus({
              accountId: account.accountId,
              running: true,
              publicKey: ev.pubkey_hex,
              npub: ev.npub,
              lastStartAt: Date.now(),
              lastError: null,
            });
            // Parseable line for deterministic E2E harnesses.
            // eslint-disable-next-line no-console
            console.log(`[openclaw_gateway] ready pubkey=${ev.pubkey_hex} npub=${ev.npub}`);
            return;
          }

          if (ev.type === "welcome_accepted") {
            const peer = (ev.peer_pubkey_hex ?? "").trim().toLowerCase();
            if (peer) {
              peerToGroup.set(peer, ev.mls_group_id);
              groupToPeer.set(ev.mls_group_id, peer);
            }
            ctx.log?.info(
              `[${account.accountId}] accepted welcome from ${peer.slice(0, 12)}… (group=${ev.mls_group_id.slice(0, 12)}…)`,
            );
            return;
          }

          if (ev.type === "dm_message") {
            ctx.setStatus({ accountId: account.accountId, lastInboundAt: Date.now() });

            const peer = (ev.peer_pubkey_hex ?? "").trim().toLowerCase();
            if (peer) {
              peerToGroup.set(peer, ev.mls_group_id);
              groupToPeer.set(ev.mls_group_id, peer);
            }

            const dmPolicy = account.config.dmPolicy ?? "pairing";
            const configAllowFrom = (account.config.allowFrom ?? [])
              .map((v) => normalizeAllowFromEntry(String(v)))
              .filter(Boolean);
            const rawBody = (ev.content ?? "").trim();
            const shouldComputeAuth = runtime.channel.commands.shouldComputeCommandAuthorized(
              rawBody,
              cfg,
            );
            const shouldReadAllowFromStore =
              dmPolicy !== "open" ||
              shouldComputeAuth ||
              (dmPolicy === "open" && configAllowFrom.length > 0 && !configAllowFrom.includes("*"));
            const storeAllowFromRaw = shouldReadAllowFromStore
              ? await runtime.channel.pairing.readAllowFromStore("marmot-ts").catch(() => [])
              : [];
            const storeAllowFrom = storeAllowFromRaw
              .map((v) => normalizeAllowFromEntry(String(v)))
              .filter(Boolean);

            const effectiveAllowFrom = [...configAllowFrom, ...storeAllowFrom];
            const allowed = isSenderAllowed(peer, effectiveAllowFrom);

            if (dmPolicy === "disabled") {
              return;
            }

            if (dmPolicy !== "open" && !allowed) {
              if (dmPolicy === "pairing") {
                const { code, created } = await runtime.channel.pairing.upsertPairingRequest({
                  channel: "marmot-ts",
                  id: peer,
                  meta: { name: peer },
                });
                if (created) {
                  try {
                    const h = activeBuses.get(account.accountId);
                    await h?.sendDmToGroup({
                      mlsGroupId: ev.mls_group_id,
                      content: runtime.channel.pairing.buildPairingReply({
                        channel: "marmot-ts",
                        idLine: `Your Marmot pubkey: ${peer}`,
                        code,
                      }),
                    });
                    ctx.setStatus({ accountId: account.accountId, lastOutboundAt: Date.now() });
                  } catch (err) {
                    ctx.log?.debug(
                      `[${account.accountId}] marmot-ts pairing reply failed: ${String(err)}`,
                    );
                  }
                }
              }
              return;
            }

            const exact = extractExactReply(rawBody);
            if (exact !== null) {
              const h = activeBuses.get(account.accountId);
              await h?.sendDmToGroup({ mlsGroupId: ev.mls_group_id, content: exact });
              ctx.setStatus({ accountId: account.accountId, lastOutboundAt: Date.now() });
              return;
            }

            // In dmPolicy="open", we still want the option to restrict which peers can reach
            // the agent/LLM path. This keeps deterministic E2E probes working while narrowing
            // the inference surface area in production deployments.
            const inferenceRestricted =
              dmPolicy === "open" &&
              effectiveAllowFrom.length > 0 &&
              !effectiveAllowFrom.includes("*");
            if (inferenceRestricted && !allowed) {
              return;
            }

            const route = runtime.channel.routing.resolveAgentRoute({
              cfg,
              channel: "marmot-ts",
              accountId: account.accountId,
              peer: { kind: "dm", id: peer },
            });

            const fromLabel = peer.slice(0, 12) + "…";
            const body = runtime.channel.reply.formatAgentEnvelope({
              channel: "Marmot (TS)",
              from: fromLabel,
              timestamp: Date.now(),
              body: rawBody,
            });

            const messageId = `marmot-ts-${Date.now()}-${peer.slice(0, 8)}`;
            const selfHex = selfKeys.get(account.accountId)?.pubkeyHex ?? "unknown";

            const ctxPayload = runtime.channel.reply.finalizeInboundContext({
              Body: body,
              RawBody: rawBody,
              CommandBody: rawBody,
              From: `marmot-ts:${peer}`,
              To: `marmot-ts:${selfHex}`,
              SessionKey: route.sessionKey,
              AccountId: route.accountId,
              ChatType: "direct" as const,
              ConversationLabel: fromLabel,
              SenderName: peer,
              SenderId: peer,
              Provider: "marmot-ts",
              Surface: "marmot-ts",
              MessageSid: messageId,
              OriginatingChannel: "marmot-ts",
              OriginatingTo: `marmot-ts:${selfHex}`,
            });

            const { onModelSelected, ...prefixOptions } = createReplyPrefixOptions({
              cfg,
              agentId: route.agentId,
              channel: "marmot-ts",
              accountId: route.accountId,
            });
            const humanDelay = runtime.channel.reply.resolveHumanDelayConfig(cfg, route.agentId);

            await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
              ctx: ctxPayload,
              cfg,
              dispatcherOptions: {
                ...prefixOptions,
                humanDelay,
                deliver: async (payload: ReplyPayload) => {
                  const replyText = payload.text;
                  if (!replyText) {
                    return;
                  }
                  const h = activeBuses.get(account.accountId);
                  await h?.sendDmToGroup({ mlsGroupId: ev.mls_group_id, content: replyText });
                  ctx.setStatus({ accountId: account.accountId, lastOutboundAt: Date.now() });
                },
                onError: (err, info) => {
                  ctx.log?.error(
                    `[${account.accountId}] ${info.kind} reply failed: ${String(err)}`,
                  );
                },
              },
              replyOptions: { onModelSelected },
            });
            return;
          }

          if (ev.type === "error") {
            ctx.setStatus({
              accountId: account.accountId,
              lastError: `${ev.context}: ${ev.message}`,
            });
            ctx.log?.warn(`[${account.accountId}] marmot-ts error: ${ev.context}: ${ev.message}`);
          }
        },
      });

      activeBuses.set(account.accountId, bus);

      return {
        stop: () => {
          const h = activeBuses.get(account.accountId);
          if (h) {
            h.shutdown();
            activeBuses.delete(account.accountId);
          }
          selfKeys.delete(account.accountId);
          ctx.setStatus({ accountId: account.accountId, running: false, lastStopAt: Date.now() });
          ctx.log?.info(`[${account.accountId}] marmot-ts provider stopped`);
        },
      };
    },
  },
};
