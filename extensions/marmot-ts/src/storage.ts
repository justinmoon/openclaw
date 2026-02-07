import type {
  GroupStateStoreBackend,
  KeyValueStoreBackend,
  SerializedClientState,
} from "marmot-ts";
import { Buffer } from "node:buffer";
import path from "node:path";

type SqliteDatabase = {
  run: (sql: string, params?: unknown[]) => unknown;
  query: (sql: string) => {
    get: (...params: unknown[]) => unknown;
    all: (...params: unknown[]) => unknown[];
  };
  close: () => void;
};

type SqliteEngine = "auto" | "bun" | "node";

function isBunRuntime(): boolean {
  return Boolean((globalThis as unknown as Record<string, unknown>).Bun);
}

function jsonReplacer(_key: string, value: unknown) {
  if (typeof value === "bigint") {
    return { __type: "BigInt", data: value.toString(10) };
  }
  if (value instanceof Uint8Array) {
    return { __type: "Uint8Array", data: Array.from(value) };
  }
  if (value instanceof ArrayBuffer) {
    return { __type: "Uint8Array", data: Array.from(new Uint8Array(value)) };
  }
  return value;
}

function jsonReviver(_key: string, value: unknown) {
  if (
    value &&
    typeof value === "object" &&
    "__type" in value &&
    // @ts-expect-error runtime check
    value.__type === "BigInt" &&
    // @ts-expect-error runtime check
    typeof value.data === "string"
  ) {
    // @ts-expect-error runtime check
    return BigInt(value.data);
  }

  if (
    value &&
    typeof value === "object" &&
    "__type" in value &&
    // @ts-expect-error runtime check
    value.__type === "Uint8Array" &&
    // @ts-expect-error runtime check
    Array.isArray(value.data)
  ) {
    // @ts-expect-error runtime check
    return new Uint8Array(value.data);
  }

  return value;
}

function encodeJson(value: unknown): string {
  return JSON.stringify(value, jsonReplacer);
}

function decodeJson<T>(text: string): T {
  return JSON.parse(text, jsonReviver) as T;
}

function toUint8Array(value: unknown): Uint8Array | null {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return new Uint8Array(value);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return null;
}

function normalizeBindParam(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    // Node sqlite bindings accept Buffer reliably for BLOBs.
    return Buffer.from(value);
  }
  if (value instanceof ArrayBuffer) {
    return Buffer.from(new Uint8Array(value));
  }
  return value;
}

export class SqliteGroupStateBackend implements GroupStateStoreBackend {
  constructor(private db: SqliteDatabase) {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS group_state (
        group_id BLOB PRIMARY KEY,
        state    BLOB NOT NULL
      );
    `);
  }

  async get(groupId: Uint8Array): Promise<SerializedClientState | null> {
    const row = this.db.query("SELECT state FROM group_state WHERE group_id = ?").get(groupId) as {
      state: unknown;
    } | null;
    if (!row) {
      return null;
    }
    return toUint8Array(row.state) as SerializedClientState | null;
  }

  async set(groupId: Uint8Array, stateBytes: SerializedClientState): Promise<void> {
    this.db.run("INSERT OR REPLACE INTO group_state (group_id, state) VALUES (?, ?)", [
      normalizeBindParam(groupId),
      normalizeBindParam(stateBytes),
    ]);
  }

  async remove(groupId: Uint8Array): Promise<void> {
    this.db.run("DELETE FROM group_state WHERE group_id = ?", [normalizeBindParam(groupId)]);
  }

  async list(): Promise<Uint8Array[]> {
    const rows = this.db.query("SELECT group_id FROM group_state").all() as {
      group_id: unknown;
    }[];
    return rows.map((r) => toUint8Array(r.group_id)).filter((v): v is Uint8Array => Boolean(v));
  }
}

export class SqliteKeyValueBackend<T> implements KeyValueStoreBackend<T> {
  constructor(private db: SqliteDatabase) {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS kv (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
  }

  async getItem(key: string): Promise<T | null> {
    const row = this.db.query("SELECT value FROM kv WHERE key = ?").get(key) as {
      value: string;
    } | null;
    if (!row) {
      return null;
    }
    return decodeJson<T>(row.value);
  }

  async setItem(key: string, value: T): Promise<T> {
    const encoded = encodeJson(value);
    this.db.run("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", [key, encoded]);
    return value;
  }

  async removeItem(key: string): Promise<void> {
    this.db.run("DELETE FROM kv WHERE key = ?", [key]);
  }

  async clear(): Promise<void> {
    this.db.run("DELETE FROM kv");
  }

  async keys(): Promise<string[]> {
    const rows = this.db.query("SELECT key FROM kv").all() as { key: string }[];
    return rows.map((r) => r.key);
  }
}

export function createNamespacedKeyValueBackend<T>(
  backend: KeyValueStoreBackend<T>,
  prefix: string,
): KeyValueStoreBackend<T> {
  const cleanPrefix = prefix.trim();
  if (!cleanPrefix) {
    throw new Error("namespace prefix required");
  }

  const k = (key: string) => `${cleanPrefix}${key}`;

  return {
    getItem: async (key) => backend.getItem(k(key)),
    setItem: async (key, value) => backend.setItem(k(key), value),
    removeItem: async (key) => backend.removeItem(k(key)),
    clear: async () => {
      const keys = await backend.keys();
      const targets = keys.filter((key) => key.startsWith(cleanPrefix));
      for (const key of targets) {
        await backend.removeItem(key);
      }
    },
    keys: async () => {
      const keys = await backend.keys();
      return keys
        .filter((key) => key.startsWith(cleanPrefix))
        .map((key) => key.slice(cleanPrefix.length));
    },
  };
}

export type SqliteStorageHandle = {
  db: SqliteDatabase;
  close: () => void;
};

export function resolveSqlitePath(stateDir: string) {
  return path.join(stateDir, "marmot.sqlite");
}

type SqliteStatement = {
  get: (...params: unknown[]) => unknown;
  all: (...params: unknown[]) => unknown[];
  run: (...params: unknown[]) => unknown;
};

type NodeSqliteNativeDb = {
  prepare: (sql: string) => SqliteStatement;
  close: () => void;
};

function wrapPreparedSqliteDb(native: NodeSqliteNativeDb): SqliteDatabase {
  const statementCache = new Map<string, SqliteStatement>();

  const prepareCached = (sql: string) => {
    const existing = statementCache.get(sql);
    if (existing) {
      return existing;
    }
    const stmt = native.prepare(sql);
    statementCache.set(sql, stmt);
    return stmt;
  };

  return {
    run: (sql: string, params?: unknown[]) => {
      const stmt = prepareCached(sql);
      const normalized = (params ?? []).map(normalizeBindParam);
      return stmt.run(...normalized);
    },
    query: (sql: string) => {
      const stmt = prepareCached(sql);
      return {
        get: (...params: unknown[]) => stmt.get(...params.map(normalizeBindParam)),
        all: (...params: unknown[]) => stmt.all(...params.map(normalizeBindParam)),
      };
    },
    close: () => native.close(),
  };
}

async function openNodeSqliteDatabase(dbPath: string): Promise<SqliteStorageHandle> {
  // Prefer better-sqlite3 when installed (stable, no Node experimental warnings).
  // Fall back to node:sqlite (experimental).
  try {
    const mod = (await import("better-sqlite3")) as unknown as {
      default: new (path: string) => NodeSqliteNativeDb;
    };
    const nativeDb = new mod.default(dbPath);
    const db = wrapPreparedSqliteDb(nativeDb);
    return { db, close: () => db.close() };
  } catch {
    const mod = (await import("node:sqlite")) as unknown as {
      DatabaseSync: new (path: string) => NodeSqliteNativeDb;
    };
    const nativeDb = new mod.DatabaseSync(dbPath);
    const db = wrapPreparedSqliteDb(nativeDb);
    return { db, close: () => db.close() };
  }
}

export async function openSqliteDatabase(
  dbPath: string,
  opts?: { engine?: SqliteEngine },
): Promise<SqliteStorageHandle> {
  const engine: SqliteEngine = opts?.engine ?? "auto";
  const resolved = engine === "auto" ? (isBunRuntime() ? "bun" : "node") : engine;

  if (resolved === "bun") {
    const mod = (await import("bun:sqlite")) as unknown as {
      Database: new (path: string) => SqliteDatabase;
    };
    const db = new mod.Database(dbPath);
    return { db, close: () => db.close() };
  }

  return await openNodeSqliteDatabase(dbPath);
}
