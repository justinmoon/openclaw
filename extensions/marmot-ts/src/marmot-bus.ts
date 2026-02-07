import type { Rumor } from "applesauce-common/helpers/gift-wrap";
import type { EventSigner } from "applesauce-core";
import type { NostrEvent } from "applesauce-core/helpers/event";
import { PrivateKeyAccount } from "applesauce-accounts/accounts";
import { unlockGiftWrap } from "applesauce-common/helpers/gift-wrap";
import { mapEventsToTimeline } from "applesauce-core";
import { bytesToHex } from "applesauce-core/helpers";
import { RelayPool } from "applesauce-relay";
import { onlyEvents } from "applesauce-relay/operators";
import {
  createCredential,
  createKeyPackageEvent,
  deserializeApplicationRumor,
  extractMarmotGroupData,
  generateKeyPackage,
  getCredentialPubkey,
  GROUP_EVENT_KIND,
  hasAck,
  KeyPackageStore,
  MarmotClient,
  type NostrNetworkInterface,
  type PublishResponse,
  unixNow,
  WELCOME_EVENT_KIND,
} from "marmot-ts";
import fs from "node:fs";
import path from "node:path";
import { getEventHash, nip19, type Event as NostrToolsEvent } from "nostr-tools";
import { lastValueFrom } from "rxjs";
import { defaultCryptoProvider, getCiphersuiteFromName, getCiphersuiteImpl } from "ts-mls";
import { getCredentialFromLeafIndex } from "ts-mls/ratchetTree.js";
import { toLeafIndex } from "ts-mls/treemath.js";
import {
  SqliteGroupStateBackend,
  SqliteKeyValueBackend,
  createNamespacedKeyValueBackend,
  openSqliteDatabase,
  resolveSqlitePath,
} from "./storage.js";

const IDENTITY_FILE = "identity.json";

const DEFAULT_CIPHERSUITE = "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519";

// NIP-59 gift wraps randomize created_at for metadata protection, so the
// timestamps may be days in the past. Use a very large lookback window.
const GIFTWRAP_LOOKBACK_SEC = 30 * 24 * 60 * 60; // 30 days
const FLUSH_DEBOUNCE_MS = 250;

const REQUEST_TIMEOUT_MS = 20_000;

type IdentityFile = { secret_key: string; public_key: string; relays: string[] };

export type MarmotTsBusEvent =
  | { type: "ready"; pubkey_hex: string; npub: string; relays: string[]; state_dir: string }
  | {
      type: "welcome_accepted";
      peer_pubkey_hex: string;
      mls_group_id: string;
      nostr_group_id: string;
      group_name: string;
    }
  | {
      type: "dm_message";
      peer_pubkey_hex: string;
      mls_group_id: string;
      created_at: number;
      content: string;
    }
  | { type: "error"; context: string; message: string };

export type MarmotTsBusReady = {
  pubkeyHex: string;
  npub: string;
  relays: string[];
  stateDir: string;
};

export type MarmotTsBusHandle = {
  ready: MarmotTsBusReady | null;
  publicKey: string;
  sendDmToPeer: (toPubkeyHex: string, text: string) => Promise<void>;
  sendDmToGroup: (params: { mlsGroupId: string; content: string }) => Promise<void>;
  shutdown: () => void;
};

function readIdentityFile(stateDir: string): IdentityFile | null {
  const p = path.join(stateDir, IDENTITY_FILE);
  if (!fs.existsSync(p)) {
    return null;
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(p, "utf8")) as IdentityFile;
    if (!parsed?.secret_key || !parsed?.public_key) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function writeIdentityFile(stateDir: string, identity: IdentityFile): void {
  const p = path.join(stateDir, IDENTITY_FILE);
  fs.writeFileSync(p, JSON.stringify(identity, null, 2));
}

function buildRumor(args: { pubkey: string; content: string }): Rumor {
  const rumor: Rumor = {
    id: "",
    kind: 9,
    pubkey: args.pubkey,
    created_at: unixNow(),
    content: args.content,
    tags: [],
  };
  rumor.id = getEventHash(rumor as unknown as NostrToolsEvent);
  return rumor;
}

function kvKeySeenGiftwrapIds() {
  return "seen.giftwrap.ids";
}

function kvKeySeenGroupEventIds(nostrGroupIdHex: string) {
  return `seen.group.${nostrGroupIdHex}.ids`;
}

function kvKeySinceGroup(nostrGroupIdHex: string) {
  return `since.group.${nostrGroupIdHex}`;
}

function clampSeenSet(ids: string[], max = 5000): string[] {
  if (ids.length <= max) {
    return ids;
  }
  return ids.slice(ids.length - max);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return (await Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timeout`)), timeoutMs),
    ),
  ])) as T;
}

function createNetwork(pool: RelayPool, inboxRelays: string[]): NostrNetworkInterface {
  return {
    request: async (relays, filters) => {
      const perRelay = await Promise.allSettled(
        relays.map(async (relay) => {
          const obs = pool.request([relay], filters).pipe(mapEventsToTimeline());
          return await withTimeout(lastValueFrom(obs), REQUEST_TIMEOUT_MS, `request(${relay})`);
        }),
      );
      const events = perRelay
        .flatMap((res) => (res.status === "fulfilled" ? res.value : []))
        // Dedup by id
        .reduce(
          (acc, ev) => {
            if (!acc.seen.has(ev.id)) {
              acc.seen.add(ev.id);
              acc.events.push(ev);
            }
            return acc;
          },
          { seen: new Set<string>(), events: [] as NostrEvent[] },
        );

      return events.events;
    },

    subscription: (relays, filters) => {
      return pool.subscription(relays, filters).pipe(onlyEvents());
    },

    publish: (relays, event) =>
      pool.publish(relays, event).then((res) =>
        res.reduce(
          (acc, curr) => {
            acc[curr.from] = curr as unknown as PublishResponse;
            return acc;
          },
          {} as Record<string, PublishResponse>,
        ),
      ),

    getUserInboxRelays: async (_pubkey) => inboxRelays,
  };
}

function closeRelayPool(pool: RelayPool) {
  for (const relay of pool.relays.values()) {
    relay.close();
  }
}

function resolveDmPeerPubkeyHex(args: {
  ratchetTree: unknown[];
  selfPubkeyHex: string;
}): string | null {
  const { ratchetTree, selfPubkeyHex } = args;
  const pubkeys: string[] = [];

  for (let i = 0; i < ratchetTree.length; i++) {
    const node = ratchetTree[i];
    if (!node || (node as { nodeType?: string }).nodeType !== "leaf") {
      continue;
    }
    try {
      const cred = getCredentialFromLeafIndex(
        ratchetTree as unknown as Parameters<typeof getCredentialFromLeafIndex>[0],
        toLeafIndex(i),
      );
      pubkeys.push(getCredentialPubkey(cred));
    } catch {
      // ignore
    }
  }

  const others = pubkeys.filter((p) => p.toLowerCase() !== selfPubkeyHex.toLowerCase());
  if (others.length === 1) {
    return others[0] ?? null;
  }
  return null;
}

export async function startMarmotTsBus(params: {
  stateDir: string;
  relays: string[];
  privateKey?: string;
  onEvent: (ev: MarmotTsBusEvent) => void | Promise<void>;
  onLog?: (line: string) => void;
}): Promise<MarmotTsBusHandle> {
  fs.mkdirSync(params.stateDir, { recursive: true });

  let account: PrivateKeyAccount;
  const configuredRelays = params.relays;

  if (params.privateKey?.trim()) {
    account = PrivateKeyAccount.fromKey(params.privateKey.trim());
  } else {
    const existing = readIdentityFile(params.stateDir);
    if (existing?.secret_key) {
      account = PrivateKeyAccount.fromKey(existing.secret_key);
    } else {
      account = PrivateKeyAccount.generateNew();
      const secretKey = (account.signer as unknown as { key: Uint8Array }).key;
      const secret = bytesToHex(secretKey);
      const pubkey = await account.signer.getPublicKey();
      writeIdentityFile(params.stateDir, {
        secret_key: secret,
        public_key: pubkey,
        relays: configuredRelays,
      });
    }
  }

  const pubkeyHex = await account.signer.getPublicKey();
  const npub = nip19.npubEncode(pubkeyHex);
  const signer = account.signer as unknown as EventSigner;

  const dbPath = resolveSqlitePath(params.stateDir);
  const storage = await openSqliteDatabase(dbPath);
  const groupStateBackend = new SqliteGroupStateBackend(storage.db);
  const kvBackend = new SqliteKeyValueBackend(storage.db);
  const keyPackageBackend = createNamespacedKeyValueBackend(kvBackend, "keypkg:");
  const keyPackageStore = new KeyPackageStore(keyPackageBackend);

  const pool = new RelayPool();
  // applesauce-relay defaults to filtering out relays until they are `ready`. For a long-lived
  // bus this can delay subscriptions at startup and cause missed early events. Let the relay
  // layer handle retries and surface errors instead.
  pool.ignoreOffline = false;
  const network = createNetwork(pool, configuredRelays);
  const client = new MarmotClient({
    signer: account.signer,
    groupStateBackend,
    keyPackageStore,
    network,
  });

  const busReady: MarmotTsBusReady = {
    pubkeyHex,
    npub,
    relays: configuredRelays,
    stateDir: params.stateDir,
  };

  await params.onEvent({
    type: "ready",
    pubkey_hex: pubkeyHex,
    npub,
    relays: configuredRelays,
    state_dir: params.stateDir,
  });
  params.onLog?.(`marmot-ts: ready as ${npub} (${pubkeyHex.slice(0, 12)}…)`);

  if (configuredRelays.length === 0) {
    await params.onEvent({
      type: "error",
      context: "config.relays",
      message: "No relays configured; marmot-ts bus will run idle until relays are set",
    });
  }

  // Ensure at least one KeyPackage is published.
  const existingKeyPackages = await client.keyPackageStore.list();
  if (existingKeyPackages.length === 0 && configuredRelays.length > 0) {
    const ciphersuite = await getCiphersuiteImpl(
      getCiphersuiteFromName(DEFAULT_CIPHERSUITE),
      defaultCryptoProvider,
    );
    const credential = createCredential(pubkeyHex);
    const keyPackage = await generateKeyPackage({ credential, ciphersuiteImpl: ciphersuite });
    await client.keyPackageStore.add(keyPackage);

    const unsigned = createKeyPackageEvent({
      keyPackage: keyPackage.publicPackage,
      pubkey: pubkeyHex,
      relays: configuredRelays,
    });

    // Patch KeyPackage event for compatibility with White Noise / MDK (Rust).
    // The released White Noise APK (v0.2.1, MDK rev f46875ec) expects hex-encoded
    // content and no "encoding" tag, but marmot-ts emits base64 by default.
    if (unsigned.content && unsigned.tags) {
      // Convert base64 content to hex
      const raw = Uint8Array.from(atob(unsigned.content), (c) => c.charCodeAt(0));
      unsigned.content = Array.from(raw)
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
      // Remove the "encoding" tag
      unsigned.tags = unsigned.tags.filter((t: string[]) => t[0] !== "encoding");
      // Normalize ciphersuite tag from "0x0001" to "1" for older MDK compat
      for (const t of unsigned.tags as string[][]) {
        if (t[0] === "mls_ciphersuite" && t[1]?.startsWith("0x")) {
          t[1] = String(parseInt(t[1], 16));
        }
        // Normalize protocol version from "1.0" to "1"
        if (t[0] === "mls_protocol_version" && t[1] === "1.0") {
          t[1] = "1";
        }
      }
    }

    const signed = await signer.signEvent(
      unsigned as unknown as Parameters<EventSigner["signEvent"]>[0],
    );
    const publishRes = await network.publish(configuredRelays, signed);
    if (!hasAck(publishRes)) {
      await params.onEvent({
        type: "error",
        context: "keypackage.publish",
        message: "Failed to publish KeyPackage event to any relay",
      });
    }

    // Publish NIP-65 relay list (kind:10002) so other clients (e.g. White Noise)
    // know which relays to query for our KeyPackage and messages.
    const relayListEvent = {
      kind: 10002,
      created_at: unixNow(),
      tags: configuredRelays.map((r: string) => ["r", r]),
      content: "",
      pubkey: pubkeyHex,
    };
    const signedRelayList = await signer.signEvent(
      relayListEvent as unknown as Parameters<EventSigner["signEvent"]>[0],
    );
    const relayListRes = await network.publish(configuredRelays, signedRelayList);
    if (!hasAck(relayListRes)) {
      params.onLog?.("marmot-ts: warning: failed to publish NIP-65 relay list");
    }

    // Publish basic profile metadata (kind:0) so other users see a name/avatar.
    const profileEvent = {
      kind: 0,
      created_at: unixNow(),
      tags: [],
      content: JSON.stringify({
        name: "OpenClaw Bot",
        display_name: "OpenClaw Bot",
        about: "AI assistant powered by OpenClaw. Send me a message!",
        picture: "https://cdn.nostr.build/i/openclaw-bot.png",
      }),
      pubkey: pubkeyHex,
    };
    const signedProfile = await signer.signEvent(
      profileEvent as unknown as Parameters<EventSigner["signEvent"]>[0],
    );
    await network.publish(configuredRelays, signedProfile);
  }

  const peerToGroup = new Map<string, string>();
  const groupToPeer = new Map<string, string>();

  // Hydrate mappings from stored groups (2-person assumption).
  const groups = await client.loadAllGroups();
  for (const group of groups) {
    const mlsGroupId = bytesToHex(group.id);
    const peer = resolveDmPeerPubkeyHex({
      ratchetTree: group.state.ratchetTree,
      selfPubkeyHex: pubkeyHex,
    });

    if (peer) {
      peerToGroup.set(peer.toLowerCase(), mlsGroupId);
      groupToPeer.set(mlsGroupId, peer.toLowerCase());
    }
  }

  let stopped = false;
  const timeouts = new Set<ReturnType<typeof setTimeout>>();
  const subscriptions = new Set<{ unsubscribe: () => void }>();
  const groupListeners = new Map<string, { unsubscribe: () => void }>();

  const sendDmToGroup = async (mlsGroupId: string, content: string) => {
    const group = await client.getGroup(mlsGroupId);
    const rumor = buildRumor({ pubkey: pubkeyHex, content });
    await group.sendApplicationRumor(rumor);
    await group.save();
  };

  const sendDmToPeer = async (toPubkeyHex: string, text: string) => {
    const peer = toPubkeyHex.trim().toLowerCase();
    const groupId = peerToGroup.get(peer);
    if (!groupId) {
      throw new Error(`No DM group for peer ${peer} (invite the bot first)`);
    }
    await sendDmToGroup(groupId, text);
  };

  const startGroupListener = async (mlsGroupId: string) => {
    if (stopped) {
      return;
    }
    if (groupListeners.has(mlsGroupId)) {
      return;
    }

    const group = await client.getGroup(mlsGroupId);
    let data = extractMarmotGroupData(group.state);
    let nostrGroupIdHex: string;

    if (data) {
      nostrGroupIdHex = bytesToHex(data.nostrGroupId);
    } else {
      // Fallback: extract nostrGroupId directly from the raw extension bytes.
      // MDK/OpenMLS uses TLS codec (fixed-length) encoding for NostrGroupDataExtension:
      //   version: u16 (2 bytes) + nostr_group_id: [u8; 32] (32 bytes) + ...
      // marmot-ts's decodeMarmotGroupData expects MLS variable-length encoding,
      // which is incompatible. Extract the 32-byte group ID directly.
      const MARMOT_EXT_TYPE = 0xf2ee;
      const ext = group.state.groupContext.extensions.find(
        (e: { extensionType: number }) =>
          typeof e.extensionType === "number" && e.extensionType === MARMOT_EXT_TYPE,
      );
      if (!ext || !ext.extensionData || ext.extensionData.length < 34) {
        params.onLog?.(
          `marmot-ts: warning: could not extract nostrGroupId for group ${mlsGroupId}`,
        );
        return;
      }
      // Skip 2-byte version field, read next 32 bytes as nostrGroupId
      nostrGroupIdHex = bytesToHex(ext.extensionData.slice(2, 34));
      params.onLog?.(`marmot-ts: extracted nostrGroupId via TLS fallback: ${nostrGroupIdHex}`);
    }

    const sinceStored = await kvBackend.getItem(kvKeySinceGroup(nostrGroupIdHex));
    const since = typeof sinceStored === "number" ? sinceStored : unixNow() - GIFTWRAP_LOOKBACK_SEC;

    const seenIds =
      (await kvBackend.getItem(kvKeySeenGroupEventIds(nostrGroupIdHex))) ?? ([] as string[]);
    const seen = new Set<string>(seenIds);

    let maxCreatedAt = since;
    let dirty = false;
    let flushTimer: ReturnType<typeof setTimeout> | null = null;
    let queue = Promise.resolve();

    const flush = async () => {
      if (stopped || !dirty) {
        return;
      }
      dirty = false;
      await group.save();
      await kvBackend.setItem(
        kvKeySeenGroupEventIds(nostrGroupIdHex),
        clampSeenSet(Array.from(seen)),
      );
      await kvBackend.setItem(kvKeySinceGroup(nostrGroupIdHex), maxCreatedAt);
    };

    const scheduleFlush = () => {
      if (stopped || flushTimer) {
        return;
      }
      flushTimer = setTimeout(() => {
        flushTimer = null;
        flush().catch(async (err) => {
          await params.onEvent({
            type: "error",
            context: "group.flush",
            message: String(err),
          });
        });
      }, FLUSH_DEBOUNCE_MS);
      timeouts.add(flushTimer);
    };

    const onEvent = async (ev: NostrEvent) => {
      if (stopped) {
        params.onLog?.(`marmot-ts: onEvent: skipped (stopped) ${ev.id?.slice(0, 16)}`);
        return;
      }
      if (seen.has(ev.id)) {
        params.onLog?.(`marmot-ts: onEvent: skipped (seen) ${ev.id?.slice(0, 16)}`);
        return;
      }
      seen.add(ev.id);
      if (typeof ev.created_at === "number") {
        maxCreatedAt = Math.max(maxCreatedAt, ev.created_at);
      }

      params.onLog?.(`marmot-ts: onEvent: ingesting ${ev.id?.slice(0, 16)} kind=${ev.kind}`);
      let resultCount = 0;
      for await (const result of group.ingest([ev])) {
        resultCount++;
        params.onLog?.(`marmot-ts: onEvent: ingest result #${resultCount}: kind=${result.kind}`);
        if (stopped) {
          return;
        }
        if (result.kind !== "applicationMessage") {
          continue;
        }
        const rumor = deserializeApplicationRumor(result.message);
        if (rumor.pubkey?.toLowerCase() === pubkeyHex.toLowerCase()) {
          continue;
        }

        const peer =
          groupToPeer.get(mlsGroupId) ??
          resolveDmPeerPubkeyHex({
            ratchetTree: group.state.ratchetTree,
            selfPubkeyHex: pubkeyHex,
          }) ??
          "";

        await params.onEvent({
          type: "dm_message",
          peer_pubkey_hex: peer,
          mls_group_id: mlsGroupId,
          created_at: rumor.created_at ?? unixNow(),
          content: (rumor.content ?? "").toString(),
        });
      }

      dirty = true;
      scheduleFlush();
    };

    params.onLog?.(
      `marmot-ts: subscribing to group events: kinds=[${GROUP_EVENT_KIND}] #h=[${nostrGroupIdHex}] since=${Math.max(0, since - 2)} relays=${configuredRelays.join(",")}`,
    );
    const sub = network
      .subscription(configuredRelays, {
        kinds: [GROUP_EVENT_KIND],
        "#h": [nostrGroupIdHex],
        since: Math.max(0, since - 2),
        limit: 200,
      })
      .subscribe({
        next: (ev) => {
          params.onLog?.(`marmot-ts: received group event: ${ev.id?.slice(0, 16)} kind=${ev.kind}`);
          queue = queue
            .then(() => onEvent(ev))
            .catch(async (err) => {
              await params.onEvent({
                type: "error",
                context: "group.ingest",
                message: String(err),
              });
            });
        },
        error: async (err) => {
          await params.onEvent({ type: "error", context: "group.sub", message: String(err) });
        },
      });

    const unsubscribe = () => {
      sub.unsubscribe();
      if (flushTimer) {
        clearTimeout(flushTimer);
        timeouts.delete(flushTimer);
        flushTimer = null;
      }
    };

    subscriptions.add({ unsubscribe });
    groupListeners.set(mlsGroupId, { unsubscribe });
  };

  const startGiftwrapListener = async () => {
    if (stopped) {
      return;
    }
    const since = unixNow() - GIFTWRAP_LOOKBACK_SEC;

    const seenIds = (await kvBackend.getItem(kvKeySeenGiftwrapIds())) ?? ([] as string[]);
    const seen = new Set<string>(seenIds);
    let dirty = false;
    let flushTimer: ReturnType<typeof setTimeout> | null = null;
    let queue = Promise.resolve();

    const flush = async () => {
      if (stopped || !dirty) {
        return;
      }
      dirty = false;
      await kvBackend.setItem(kvKeySeenGiftwrapIds(), clampSeenSet(Array.from(seen)));
    };

    const scheduleFlush = () => {
      if (stopped || flushTimer) {
        return;
      }
      flushTimer = setTimeout(() => {
        flushTimer = null;
        flush().catch(async (err) => {
          await params.onEvent({
            type: "error",
            context: "giftwrap.flush",
            message: String(err),
          });
        });
      }, FLUSH_DEBOUNCE_MS);
      timeouts.add(flushTimer);
    };

    const onGiftwrap = async (ev: NostrEvent) => {
      if (stopped) {
        return;
      }
      if (seen.has(ev.id)) {
        return;
      }

      let rumor: Rumor | null = null;
      try {
        rumor = await unlockGiftWrap(ev, signer);
      } catch (err) {
        await params.onEvent({
          type: "error",
          context: "giftwrap.unwrap",
          message: String(err),
        });
        return;
      }

      seen.add(ev.id);
      dirty = true;
      scheduleFlush();

      if (rumor.kind !== WELCOME_EVENT_KIND) {
        return;
      }

      try {
        const group = await client.joinGroupFromWelcome({ welcomeRumor: rumor });

        const mlsGroupId = bytesToHex(group.id);
        const data = extractMarmotGroupData(group.state);
        let nostrGroupIdHex = data ? bytesToHex(data.nostrGroupId) : "";

        // TLS fallback for nostrGroupId when marmot-ts variable-length decoding fails
        if (!nostrGroupIdHex) {
          const MARMOT_EXT_TYPE = 0xf2ee;
          const ext = group.state.groupContext.extensions.find(
            (e: { extensionType: number }) =>
              typeof e.extensionType === "number" && e.extensionType === MARMOT_EXT_TYPE,
          );
          if (ext?.extensionData?.length >= 34) {
            nostrGroupIdHex = bytesToHex(ext.extensionData.slice(2, 34));
            params.onLog?.(
              `marmot-ts: welcome: extracted nostrGroupId via TLS fallback: ${nostrGroupIdHex}`,
            );
          }
        }
        const peer = resolveDmPeerPubkeyHex({
          ratchetTree: group.state.ratchetTree,
          selfPubkeyHex: pubkeyHex,
        });

        if (peer) {
          peerToGroup.set(peer.toLowerCase(), mlsGroupId);
          groupToPeer.set(mlsGroupId, peer.toLowerCase());
        }
        await startGroupListener(mlsGroupId);

        await params.onEvent({
          type: "welcome_accepted",
          peer_pubkey_hex: peer ?? "",
          mls_group_id: mlsGroupId,
          nostr_group_id: nostrGroupIdHex,
          group_name: data?.name ?? "DM",
        });
      } catch (err) {
        await params.onEvent({
          type: "error",
          context: "welcome.join",
          message: String(err),
        });
      }
    };

    const sub = network
      .subscription(configuredRelays, {
        kinds: [1059],
        "#p": [pubkeyHex],
        since,
        limit: 200,
      })
      .subscribe({
        next: (ev) => {
          queue = queue
            .then(() => onGiftwrap(ev))
            .catch(async (err) => {
              await params.onEvent({
                type: "error",
                context: "giftwrap.ingest",
                message: String(err),
              });
            });
        },
        error: async (err) => {
          await params.onEvent({ type: "error", context: "giftwrap.sub", message: String(err) });
        },
      });

    const unsubscribe = () => {
      sub.unsubscribe();
      if (flushTimer) {
        clearTimeout(flushTimer);
        timeouts.delete(flushTimer);
        flushTimer = null;
      }
    };
    subscriptions.add({ unsubscribe });
  };

  // Start listeners for all known groups (fast path for existing DMs).
  for (const group of groups) {
    await startGroupListener(bytesToHex(group.id));
  }

  // Start listening for new welcome giftwraps.
  if (configuredRelays.length > 0) {
    await startGiftwrapListener();
  }

  return {
    ready: busReady,
    publicKey: pubkeyHex,
    sendDmToPeer,
    sendDmToGroup: async ({ mlsGroupId, content }) => {
      await sendDmToGroup(mlsGroupId, content);
    },
    shutdown: () => {
      stopped = true;
      for (const t of timeouts) {
        clearTimeout(t);
      }
      timeouts.clear();
      for (const sub of subscriptions) {
        sub.unsubscribe();
      }
      subscriptions.clear();
      groupListeners.clear();
      closeRelayPool(pool);
      storage.close();
    },
  };
}
