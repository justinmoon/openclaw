import type { LinkUnderstandingOutput } from "./types.js";

function formatSection(output: LinkUnderstandingOutput, index: number, total: number): string {
  const label = total > 1 ? `Link ${index + 1}/${total}` : "Link";
  const source = output.source?.trim() || "unknown";
  const lines = [`[${label}]`, `URL: ${output.url}`, `Source: ${source}`];
  lines.push(`Summary:\n${output.text.trim()}`);
  return lines.join("\n");
}

export function formatLinkUnderstandingSections(outputs: LinkUnderstandingOutput[]): string[] {
  const trimmed = outputs
    .map((output) => ({ ...output, text: output.text.trim(), url: output.url.trim() }))
    .filter((output) => output.text && output.url);
  if (trimmed.length === 0) return [];
  return trimmed.map((output, index) => formatSection(output, index, trimmed.length));
}

export function formatLinkUnderstandingBody(params: {
  body?: string;
  outputs: LinkUnderstandingOutput[];
}): string {
  const sections = formatLinkUnderstandingSections(params.outputs);
  if (sections.length === 0) {
    return params.body ?? "";
  }

  const base = (params.body ?? "").trim();
  if (!base) return sections.join("\n\n");
  return `${base}\n\n${sections.join("\n\n")}`;
}
