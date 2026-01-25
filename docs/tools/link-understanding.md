---
summary: "Run a CLI on URLs in inbound messages and inject results before the agent replies"
read_when:
  - You want to summarize or enrich URLs in inbound messages
  - You need to enable or scope link understanding
  - You want to wire a custom CLI to preprocess links
---

# Link understanding

Link understanding is the **inbound URL enrichment pipeline**: Clawdbot detects
links in a message, runs your **local CLI** to interpret them, and injects the
results into the message body **before the agent sees it**. The goal is a
predictable, structured prompt envelope without adding a browsing tool.

This is **not** an agent tool. It is a preprocessing step in the auto-reply
pipeline, shared across all channels (including webchat).

## What it enables

- Summarize or extract data from links (docs, tickets, dashboards, runbooks).
- Inject structured context (titles, owners, status, timestamps, key facts).
- Normalize or transform link payloads (HTML → text, JSON → bullets).
- Gate link processing by channel, chat type, or session key.
- Apply SSRF protections before your CLI runs.

## When it runs (pipeline order)

Link understanding runs after inbound normalization and media understanding,
but before the agent executes the reply run:

1) Inbound message is normalized and routed.
2) Media understanding may rewrite the body (images/audio/video).
3) Link understanding detects URLs in the current command/body and appends results.
4) The agent receives the final body and command parsing uses the updated text.

## How it works

1) Clawdbot scans the inbound message for **bare** `http/https` URLs.
2) It dedupes links in order and caps them at `tools.links.maxLinks` (default 3).
3) For each link, it runs your configured CLI entries **in order** until one returns non-empty stdout.
4) It appends the resulting outputs to the message body (structured envelope).
5) The agent sees the original text plus the appended link outputs.

No requests are made by Clawdbot itself; **your CLI does the fetching/parsing**.

## URL detection rules

- Only **bare URLs** are extracted (e.g. `https://example.com`).
- Markdown links are ignored (`[label](https://example.com)` is stripped).
- Links are deduped (first occurrence wins).
- Only `http` and `https` are allowed.
- Local and private hosts are blocked (see **Security** below).

## Configuration

Link understanding is configured under `tools.links`:

```json5
{
  tools: {
    links: {
      enabled: true,
      maxLinks: 3,
      timeoutSeconds: 30,
      scope: {
        default: "allow",
        rules: [{ action: "deny", match: { chatType: "group" } }]
      },
      models: [
        {
          command: "link-understand",
          args: ["--url", "{{LinkUrl}}", "--format", "markdown"],
          timeoutSeconds: 20
        }
      ]
    }
  }
}
```

### `tools.links` fields

- `enabled`: enable/disable link understanding (default: enabled when models are configured).
- `maxLinks`: max URLs processed per message (default 3).
- `timeoutSeconds`: default timeout for CLI runs (default 30).
- `scope`: optional gating rules (same structure as media understanding scope).
- `models`: ordered list of CLI entries (fallbacks in order).

### Model entries (`tools.links.models[]`)

Each entry is a CLI command:

- `type`: optional, only `"cli"` is supported (default).
- `command`: executable to run (required).
- `args`: CLI args (templated; see **Template variables**).
- `timeoutSeconds`: optional override for this entry.

The first entry that returns **non-empty stdout** wins. If a command fails or
returns empty output, Clawdbot tries the next entry.

## Performance + ordering

- Links are processed **sequentially** (per message, in order).
- Each CLI entry has a timeout (per entry or default).
- Failure to run a CLI does **not** abort the agent run; it just tries the next entry.
- `maxLinks` is the primary cap for latency. Keep it low for chatty channels.

## Template variables

`tools.links.models[].args` supports the standard template variables plus `{{LinkUrl}}`.
See [Template variables](/gateway/configuration#template-variables) for the full list.

Link-specific variable:

| Variable | Description |
|----------|-------------|
| `{{LinkUrl}}` | URL currently being processed |

## Output format

Each link is wrapped in a predictable envelope and appended to the message body:

```
<original message>

[Link]
URL: https://example.com
Source: link-understand
Summary:
<cli output>
```

When multiple links are present, the header is numbered (`[Link 1/2]`, `[Link 2/2]`).
`Source` is the CLI `command` that produced the output.

### Output best practices

- Keep summaries short and stable (avoid prompt bloat).
- Prefer a compact, consistent structure (title → bullets → status).
- Avoid surrounding fences unless you want the agent to treat it as literal text.


## Scope gating

`tools.links.scope` uses the same rules as media understanding:

- `default`: `"allow"` or `"deny"`.
- `rules[]`: first match wins.
  - `match.channel` (surface/channel)
  - `match.chatType` (`direct`, `group`, `channel`)
  - `match.keyPrefix` (session key prefix)

Example (only allow direct messages):

```json5
{
  tools: {
    links: {
      scope: {
        default: "deny",
        rules: [{ action: "allow", match: { chatType: "direct" } }]
      },
      models: [{ command: "link-understand", args: ["--url", "{{LinkUrl}}"] }]
    }
  }
}
```

## Security (SSRF guard)

Clawdbot blocks local/private hosts before invoking your CLI. The guard rejects:

- `localhost`, `*.localhost`, `*.local`, `*.internal`
- Loopback addresses (IPv4 + IPv6)
- Private and link-local IP ranges (IPv4 + IPv6)
- `metadata.google.internal`
- Addresses that resolve to private/internal IPs

If a URL is blocked, it is **skipped** and your CLI is not invoked for it.
URLs that fail DNS resolution are also skipped.

## Decision tracking

Each run records a decision payload (useful in logs/debugging) in `ctx.LinkUnderstandingDecisions`:

```json5
{
  outcome: "success" | "skipped" | "disabled" | "scope-deny" | "no-links",
  urls: [
    {
      url: "https://example.com",
      attempts: [
        { type: "cli", command: "link-understand", outcome: "success" }
      ],
      chosen: { type: "cli", command: "link-understand", outcome: "success" }
    }
  ]
}
```

## Custom usage patterns

### Ticket summarizer (single link)

```json5
{
  tools: {
    links: {
      maxLinks: 1,
      models: [
        {
          command: "ticket-summary",
          args: ["--url", "{{LinkUrl}}", "--format", "brief"]
        }
      ]
    }
  }
}
```

### Per-channel allowlist (only Discord + Slack)

```json5
{
  tools: {
    links: {
      scope: {
        default: "deny",
        rules: [
          { action: "allow", match: { channel: "discord" } },
          { action: "allow", match: { channel: "slack" } }
        ]
      },
      models: [{ command: "link-understand", args: ["--url", "{{LinkUrl}}"] }]
    }
  }
}
```

### Per-agent override (support agent only)

```json5
{
  agents: {
    list: [
      {
        id: "support",
        tools: {
          links: {
            enabled: true,
            maxLinks: 2,
            models: [{ command: "link-understand", args: ["--url", "{{LinkUrl}}"] }]
          }
        }
      }
    ]
  }
}
```

### Markdown link behavior (ignored)

Message:
```
Please check [our docs](https://docs.example.com) and https://status.example.com
```

Only `https://status.example.com` is processed (markdown links are stripped).

## Troubleshooting

- **Nothing happens:** ensure `tools.links.models` is set and `enabled` is not `false`.
- **No links detected:** only bare URLs are extracted; markdown links are ignored.
- **Output missing:** your CLI returned empty stdout; try logging or return a default line.
- **Wrong channel scope:** check `tools.links.scope` rules and `match.chatType`.
- **SSRF blocked:** local/private URLs are skipped by design.

Enable verbose logs to see CLI execution and scope decisions:

```bash
clawdbot gateway run --verbose
```

## Examples

### Minimal CLI runner

```json5
{
  tools: {
    links: {
      models: [{ command: "link-understand", args: ["--url", "{{LinkUrl}}"] }]
    }
  }
}
```

### Limit to 1 link, custom timeout

```json5
{
  tools: {
    links: {
      maxLinks: 1,
      timeoutSeconds: 10,
      models: [{ command: "link-understand", args: ["--url", "{{LinkUrl}}"] }]
    }
  }
}
```

### Fallback chain (first non-empty output wins)

```json5
{
  tools: {
    links: {
      models: [
        { command: "link-understand", args: ["--url", "{{LinkUrl}}"] },
        { command: "link-backup", args: ["{{LinkUrl}}"] }
      ]
    }
  }
}
```
