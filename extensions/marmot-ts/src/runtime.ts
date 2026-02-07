import type { PluginRuntime } from "openclaw/plugin-sdk";

let runtime: PluginRuntime | null = null;

export function setMarmotTsRuntime(next: PluginRuntime): void {
  runtime = next;
}

export function getMarmotTsRuntime(): PluginRuntime {
  if (!runtime) {
    throw new Error("marmot-ts runtime not initialized");
  }
  return runtime;
}
