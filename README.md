# @pipeworx/social-signal

How much a topic is being discussed across social networks, and how active an account is. Keyless.

Part of [Pipeworx](https://pipeworx.io) — an MCP gateway connecting AI agents to 1476+ live data sources.

## Tools

- `social_topic_volume(...)` — post counts for a topic over time, bucketed, across Bluesky, Mastodon, Reddit and Hacker News.
- `social_account_activity(...)` — posting cadence and engagement for one account on Bluesky or Mastodon.

## Auth

None.

## Coverage — a directional proxy, not total social volume

Covers **Bluesky, Mastodon, Reddit and Hacker News**. **X/Twitter is excluded** — its API is paid — so results measure a partial slice of social media. Every tool description says this, because "social volume" that silently omits the largest network invites a wrong conclusion about a trend.

A network that cannot be reached returns `null` for its buckets rather than `0`. That distinction matters: a measured zero and an unobserved gap are different facts, and Mastodon in particular is often unreachable from Cloudflare egress.

### Per-network quirks handled in-pack

- **Reddit** — `search.rss` prepends subreddit entries (`t5_` ids dated 2010) which faked a 16-year coverage floor; only `t3_`/`t1_` ids are counted.
- **Hacker News** — Algolia caps pagination at 1,000 and returns `nbHits: 0` past it, so the pack keeps the maximum `nbHits` seen rather than the last.
- **Bluesky** — burst-throttles at roughly 10 requests refilling ~1/s, so buckets are swept newest-first under a 12-request budget and any gap lands at the far edge of the window.

## Data sources

- Bluesky: `https://public.api.bsky.app`
- Mastodon: `https://mastodon.social/api/v2/search`
- Reddit: `https://www.reddit.com/search.rss`
- Hacker News: `https://hn.algolia.com/api/v1/search`

## Quick Start

Add to your MCP client (Claude Desktop, Cursor, Windsurf, etc.):

```json
{
  "mcpServers": {
    "social-signal": {
      "url": "https://gateway.pipeworx.io/social-signal/mcp"
    }
  }
}
```

### What this endpoint actually serves

`tools/list` at `https://gateway.pipeworx.io/social-signal/mcp` returns the tools in the table
above **plus the shared Pipeworx meta-tools** — `ask_pipeworx`,
`discover_tools`, `search_within`, `remember`/`recall` and the rest of the
gateway-wide set. So the tool count you see is larger than this table: a
single-pack endpoint currently lists roughly 30 shared tools alongside the
pack's own. The connection's `initialize` response states its exact scope, and
is the authoritative answer for a given day.

This is deliberate, not multiplexing by accident. The meta-tools are what let a
scoped connection answer a question this pack does not cover — via
`ask_pipeworx`, which routes across the whole catalog — without you adding a
second MCP server. There is currently no way to mount a pack endpoint without
them; if the extra schemas cost you more context than the routing is worth,
connect to the full gateway once rather than to several pack endpoints.

Or connect to the full Pipeworx gateway to get every pack's tools listed
directly, instead of just this one's:

```json
{
  "mcpServers": {
    "pipeworx": {
      "url": "https://gateway.pipeworx.io/mcp"
    }
  }
}
```

Both URLs reach the same gateway and the same 1476+ data sources. The
only difference is which pack's tools are listed **directly**; `ask_pipeworx`
reaches all of them from either one.

## Using with ask_pipeworx

Instead of calling tools directly, you can ask questions in plain English —
this works on the pack endpoint above as well as on the full gateway:

```
ask_pipeworx({ question: "your question about Social Signal data" })
```

The gateway picks the right tool and fills the arguments automatically.

## More

- [Docs and guides](https://pipeworx.io/docs)
- [pipeworx.io](https://pipeworx.io)

## License

MIT
