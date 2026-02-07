import type { OpenClawConfig } from "openclaw/plugin-sdk";

export interface MarmotTsAccountConfig {
  enabled?: boolean;
  name?: string;
  privateKey?: string;
  relays?: string[];
  dmPolicy?: "pairing" | "allowlist" | "open" | "disabled";
  allowFrom?: Array<string | number>;
}

export interface ResolvedMarmotTsAccount {
  accountId: string;
  name?: string;
  enabled: boolean;
  configured: boolean;
  relays: string[];
  publicKey?: string;
  npub?: string;
  config: MarmotTsAccountConfig;
}

const DEFAULT_ACCOUNT_ID = "default";

export const DEFAULT_RELAYS = ["wss://relay.damus.io", "wss://nos.lol"];

export function listMarmotTsAccountIds(cfg: OpenClawConfig): string[] {
  const marmotCfg = (cfg.channels as Record<string, unknown> | undefined)?.["marmot-ts"] as
    | MarmotTsAccountConfig
    | undefined;
  if (!marmotCfg) {
    return [];
  }
  if (marmotCfg.enabled === false) {
    return [];
  }
  return [DEFAULT_ACCOUNT_ID];
}

export function resolveDefaultMarmotTsAccountId(cfg: OpenClawConfig): string {
  const ids = listMarmotTsAccountIds(cfg);
  if (ids.includes(DEFAULT_ACCOUNT_ID)) {
    return DEFAULT_ACCOUNT_ID;
  }
  return ids[0] ?? DEFAULT_ACCOUNT_ID;
}

export function resolveMarmotTsAccount(opts: {
  cfg: OpenClawConfig;
  accountId?: string | null;
  runtime?: { publicKey?: string | null; npub?: string | null };
}): ResolvedMarmotTsAccount {
  const accountId = opts.accountId ?? DEFAULT_ACCOUNT_ID;
  const marmotCfg = (opts.cfg.channels as Record<string, unknown> | undefined)?.["marmot-ts"] as
    | MarmotTsAccountConfig
    | undefined;

  const enabled = marmotCfg?.enabled !== false;
  const configured = Boolean(marmotCfg);

  return {
    accountId,
    name: marmotCfg?.name?.trim() || undefined,
    enabled,
    configured,
    relays: marmotCfg?.relays ?? DEFAULT_RELAYS,
    publicKey: opts.runtime?.publicKey ?? undefined,
    npub: opts.runtime?.npub ?? undefined,
    config: {
      enabled: marmotCfg?.enabled,
      name: marmotCfg?.name,
      privateKey: marmotCfg?.privateKey,
      relays: marmotCfg?.relays,
      dmPolicy: marmotCfg?.dmPolicy,
      allowFrom: marmotCfg?.allowFrom,
    },
  };
}
