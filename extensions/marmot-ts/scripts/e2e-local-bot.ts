import os from "node:os";
import path from "node:path";
import { startMarmotTsBus, type MarmotTsBusEvent } from "../src/marmot-bus.js";

function parseRelays(raw: string | undefined): string[] {
  const v = (raw ?? "").trim();
  if (!v) {
    const single = (process.env.RELAY_URL ?? "").trim();
    if (single) return [single];
    return ["ws://127.0.0.1:8080"];
  }
  return v
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function resolveDefaultStateDir(): string {
  // Prefer the interop-lab layout when running locally.
  return path.join(os.homedir(), "code", "marmot-interop-lab", ".state", "openclaw-e2e");
}

function extractExactReply(content: string): string | null {
  // The harness sends: openclaw: reply exactly "E2E_OK_<token>"
  // We must reply with EXACTLY the string inside the quotes.
  const m = content.match(/openclaw:\s*reply exactly\s+"([^"]*)"/i);
  return m?.[1] ?? null;
}

async function main() {
  const relays = parseRelays(process.env.RELAYS);
  const stateDir = (process.env.OPENCLAW_STATE_DIR?.trim() || resolveDefaultStateDir()).trim();

  let printedReady = false;

  const bus = await startMarmotTsBus({
    stateDir,
    relays,
    onEvent: async (ev: MarmotTsBusEvent) => {
      if (ev.type === "ready") {
        if (!printedReady) {
          printedReady = true;
          // Single parseable line for the harness orchestrator.
          // eslint-disable-next-line no-console
          console.log(`[openclaw_bot] ready pubkey=${ev.pubkey_hex} npub=${ev.npub}`);
        }
        return;
      }

      if (ev.type === "dm_message") {
        const exact = extractExactReply(String(ev.content ?? ""));
        if (!exact) return;
        try {
          await bus.sendDmToPeer(String(ev.peer_pubkey_hex ?? ""), exact);
          // eslint-disable-next-line no-console
          console.log(`[openclaw_bot] replied peer=${ev.peer_pubkey_hex} bytes=${exact.length}`);
        } catch (err) {
          // eslint-disable-next-line no-console
          console.log(`[openclaw_bot] reply_failed peer=${ev.peer_pubkey_hex} err=${String(err)}`);
        }
        return;
      }

      if (ev.type === "error") {
        // eslint-disable-next-line no-console
        console.log(`[openclaw_bot] error context=${ev.context} message=${ev.message}`);
      }
    },
  });

  const shutdown = () => {
    try {
      bus.shutdown();
    } finally {
      process.exit(0);
    }
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);

  // Keep the process alive indefinitely; the orchestrator terminates it.
  // eslint-disable-next-line no-console
  console.log(`[openclaw_bot] running state_dir=${stateDir} relays=${relays.join(",")}`);
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  await new Promise<void>(() => {});
}

await main();
