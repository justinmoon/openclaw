declare module "bun:sqlite" {
  export class Database {
    constructor(path: string);
    run(sql: string, params?: unknown[]): unknown;
    query(sql: string): {
      get: (...params: unknown[]) => unknown;
      all: (...params: unknown[]) => unknown[];
    };
    close(): void;
  }
}
