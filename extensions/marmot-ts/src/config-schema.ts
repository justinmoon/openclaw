import { MarkdownConfigSchema, buildChannelConfigSchema } from "openclaw/plugin-sdk";
import { z } from "zod";

const allowFromEntry = z.union([z.string(), z.number()]);

export const MarmotTsConfigSchema = z.object({
  name: z.string().optional(),
  enabled: z.boolean().optional(),
  markdown: MarkdownConfigSchema,
  privateKey: z.string().optional(),
  relays: z.array(z.string()).optional(),
  dmPolicy: z.enum(["pairing", "allowlist", "open", "disabled"]).optional(),
  allowFrom: z.array(allowFromEntry).optional(),
});

export type MarmotTsConfig = z.infer<typeof MarmotTsConfigSchema>;

export const marmotTsChannelConfigSchema = buildChannelConfigSchema(MarmotTsConfigSchema);
