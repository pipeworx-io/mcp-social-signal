# @pipeworx/social-signal

How much a topic is being discussed across social networks, and how active an account is. Keyless.

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

Or connect to the full Pipeworx gateway for access to all 1394+ data sources:

```json
{
  "mcpServers": {
    "pipeworx": {
      "url": "https://gateway.pipeworx.io/mcp"
    }
  }
}
```

## Using with ask_pipeworx

Instead of calling tools directly, you can ask questions in plain English:

```
ask_pipeworx({ question: "your question about Social Signal data" })
```

The gateway picks the right tool and fills the arguments automatically.

## More

- [All tools and guides](https://github.com/pipeworx-io/examples)
- [pipeworx.io](https://pipeworx.io)

## License

MIT
