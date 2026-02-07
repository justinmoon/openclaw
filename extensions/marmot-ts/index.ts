import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";
import { marmotTsPlugin } from "./src/channel.js";
import { setMarmotTsRuntime } from "./src/runtime.js";

const plugin = {
  id: "marmot-ts",
  name: "Marmot (TS)",
  description: "Marmot DM channel plugin via marmot-ts (in-process TypeScript)",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    setMarmotTsRuntime(api.runtime);
    api.registerChannel({ plugin: marmotTsPlugin });
  },
};

export default plugin;
