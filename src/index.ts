interface McpToolDefinition {
  name: string;
  description: string;
  inputSchema: {
    type: 'object';
    properties: Record<string, unknown>;
    required?: string[];
  };
}

interface McpToolExport {
  tools: McpToolDefinition[];
  callTool: (name: string, args: Record<string, unknown>) => Promise<unknown>;
  meter?: { credits: number };
  cost?: Record<string, unknown>;
  provider?: string;
}

/**
 * Social Signal — post-VOLUME TIME SERIES across the open social networks that
 * can be read without an API key.
 *
 * The bluesky / mastodon / reddit / hackernews packs do LOOKUPS (give me the
 * posts). This pack gives you a SERIES: counts bucketed over time, usable as a
 * signal (attention/buzz trend for a topic, posting cadence for an account).
 *
 * ── COVERAGE HONESTY (the whole point of reading this header) ─────────────
 * Bluesky + Mastodon + Reddit + Hacker News ONLY. X/Twitter is NOT included —
 * its API is paid — and for a great many economically-relevant figures X is
 * where the conversation actually lives. Every number here is therefore a
 * PROXY over a partial, non-representative universe. It is not "social volume".
 * Every response repeats this in `coverage_note`.
 *
 * ── Source probe results (2026-07-27, all keyless) ────────────────────────
 * Bluesky   api.bsky.app/xrpc/app.bsky.feed.searchPosts — supports `since` /
 *           `until` and returns `hitsTotal`, so ONE request per bucket yields an
 *           EXACT server-side count for that bucket (no pagination needed).
 *           public.api.bsky.app 403s some egress IPs while api.bsky.app answers
 *           (and vice-versa on Cloudflare), so both hosts are tried in turn.
 *           NOTE: post search on the *authenticated* endpoint is a different
 *           thing — the `bluesky` pack needs an app password for its search;
 *           the AppView host used here answers keyless.
 * Mastodon  /api/v2/search?type=statuses returns EMPTY without a token, so the
 *           public hashtag timeline (/api/v1/timelines/tag/<tag>) is used
 *           instead, paged backwards with max_id. Instance-scoped.
 * Reddit    the JSON endpoints 403 datacenter egress (see the `reddit` pack);
 *           the Atom feeds (/search.rss) stay open but carry NO score or
 *           comment count, cap at 100 entries, and 429 readily.
 * HN        hn.algolia.com search_by_date + numericFilters on created_at_i.
 *           Reliable, returns up to 1000 hits/page with real points/comments.
 *
 * ── Zero vs unknown ───────────────────────────────────────────────────────
 * A bucket count is a NUMBER only when that network actually observed the
 * bucket. If a network's fetch was truncated before reaching that far back, or
 * the network errored, the bucket is `null` — never 0. Each network reports
 * `covered_from`/`covered_to` and `complete`, and the response carries
 * `window_actually_covered`.
 */


const UA = 'pipeworx/1.0 (+https://pipeworx.io)';
const TIMEOUT_MS = 10_000;

const BSKY_HOSTS = ['https://api.bsky.app/xrpc', 'https://public.api.bsky.app/xrpc'];
const MASTODON_DEFAULT_INSTANCE = 'mastodon.social';
const HN_BASE = 'https://hn.algolia.com/api/v1';
const REDDIT_BASE = 'https://www.reddit.com';
// Reddit blocks the plain library UA on its feeds; a browser-shaped one with our
// contact URL is what the `reddit` pack already uses successfully in production.
const REDDIT_UA = 'Mozilla/5.0 (compatible; pipeworx-mcp/1.0; +https://pipeworx.io)';

const ALL_NETWORKS = ['bluesky', 'mastodon', 'reddit', 'hackernews'] as const;
type Network = (typeof ALL_NETWORKS)[number];

const COVERAGE_NOTE =
  'Covers Bluesky, Mastodon, Reddit and Hacker News only. X/Twitter is excluded because its API is paid, ' +
  'and for many economically-relevant topics X is where most of the discussion actually happens. ' +
  'Read these counts as a proxy over a partial, non-representative slice of social media — they are a directional ' +
  'signal for one corner of the internet, not total social volume.';

const DESC_TAIL =
  ' Coverage is Bluesky, Mastodon, Reddit and Hacker News; X/Twitter is excluded (paid API), so results are a ' +
  'directional proxy over a partial slice of social media rather than total social volume.';

// ── HTTP ──────────────────────────────────────────────────────────────────

async function httpText(url: string, ua = UA, accept = 'application/json'): Promise<string> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': ua, Accept: accept },
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} from ${new URL(url).host}`);
    return await res.text();
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      throw new Error(`timed out after ${TIMEOUT_MS / 1000}s (${new URL(url).host})`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function httpJson<T>(url: string, ua = UA): Promise<T> {
  return JSON.parse(await httpText(url, ua)) as T;
}

/** Bluesky AppView, trying each public host — different egress IPs get 403 from different hosts. */
let bskyHostIdx = 0;
async function bskyGet<T>(method: string, params: Record<string, string | number>): Promise<T> {
  const qs = new URLSearchParams(Object.entries(params).map(([k, v]): [string, string] => [k, String(v)])).toString();
  let lastErr: unknown;
  for (let i = 0; i < BSKY_HOSTS.length; i++) {
    const idx = (bskyHostIdx + i) % BSKY_HOSTS.length;
    try {
      const out = await httpJson<T>(`${BSKY_HOSTS[idx]}/${method}?${qs}`);
      bskyHostIdx = idx; // remember the host that works from this egress
      return out;
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error('Bluesky AppView unreachable');
}

/** Bounded-concurrency map — keeps bucket fan-out from hammering an upstream. */
async function pooled<T, R>(items: T[], limit: number, fn: (item: T, i: number) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const i = cursor++;
      if (i >= items.length) return;
      out[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return out;
}

// ── time buckets ──────────────────────────────────────────────────────────

const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;

interface Grid {
  bucket: 'day' | 'hour';
  bucketMs: number;
  starts: number[]; // ms, UTC-aligned
  fromMs: number;
  toMs: number;
}

function buildGrid(days: number, bucket: 'day' | 'hour'): Grid {
  const bucketMs = bucket === 'hour' ? HOUR_MS : DAY_MS;
  const toMs = Date.now();
  const rawFrom = toMs - days * DAY_MS;
  // Align outward to whole UTC bucket boundaries so every bucket is a full bucket.
  const fromMs = Math.floor(rawFrom / bucketMs) * bucketMs;
  const starts: number[] = [];
  for (let t = fromMs; t < toMs; t += bucketMs) starts.push(t);
  return { bucket, bucketMs, starts, fromMs, toMs };
}

function bucketIndex(grid: Grid, ms: number): number {
  if (!Number.isFinite(ms) || ms < grid.fromMs || ms > grid.toMs + grid.bucketMs) return -1;
  const i = Math.floor((ms - grid.fromMs) / grid.bucketMs);
  return i >= 0 && i < grid.starts.length ? i : -1;
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

function parseTs(v: unknown): number {
  if (typeof v === 'number') return v > 1e11 ? v : v * 1000; // seconds vs ms
  if (typeof v === 'string') {
    const n = Date.parse(v);
    return Number.isFinite(n) ? n : NaN;
  }
  return NaN;
}

// ── per-network result shape ──────────────────────────────────────────────

interface NetResult {
  network: Network;
  status: 'ok' | 'error';
  /** counts per bucket, aligned to grid.starts. `null` = NOT OBSERVED (never means zero). */
  counts: (number | null)[];
  method: string;
  count_basis: 'server_side_count_per_bucket' | 'counted_from_fetched_posts';
  complete: boolean;
  covered_from: string | null;
  covered_to: string | null;
  posts_counted: number | null;
  truncated: boolean;
  engagement: Record<string, unknown> | null;
  note?: string;
  error?: string;
}

function errResult(network: Network, grid: Grid, method: string, err: unknown): NetResult {
  return {
    network,
    status: 'error',
    counts: grid.starts.map(() => null),
    method,
    count_basis: 'counted_from_fetched_posts',
    complete: false,
    covered_from: null,
    covered_to: null,
    posts_counted: null,
    truncated: false,
    engagement: null,
    error: err instanceof Error ? err.message : String(err),
    note: 'This network contributed nothing to the series; its buckets are null (unknown), not zero.',
  };
}

/**
 * Turn fetched post timestamps into bucket counts.
 * `oldestReachedMs` is how far back the fetch actually got: buckets that start
 * before it are `null` (we cannot tell zero from not-looked-at down there).
 */
function countsFromPosts(grid: Grid, tsMs: number[], oldestReachedMs: number): (number | null)[] {
  const counts: (number | null)[] = grid.starts.map((s) => (s >= oldestReachedMs ? 0 : null));
  for (const t of tsMs) {
    const i = bucketIndex(grid, t);
    if (i >= 0 && counts[i] !== null) counts[i] = (counts[i] as number) + 1;
  }
  return counts;
}

// ── Bluesky ───────────────────────────────────────────────────────────────

interface BskyPost {
  indexedAt?: string;
  record?: { text?: string; createdAt?: string };
  author?: { handle?: string };
  likeCount?: number;
  repostCount?: number;
  replyCount?: number;
}
interface BskySearch {
  posts?: BskyPost[];
  cursor?: string;
  hitsTotal?: number;
}

const BSKY_HITS_CAP = 10_000; // the AppView clamps hitsTotal here
/**
 * Measured 2026-07-27: the public AppView allows a burst of ~10 requests and
 * then refills at roughly 1/s — 20 requests at any pace faster than 1/s return
 * 403 after the tenth. Counting costs one request per bucket, so the newest
 * BSKY_MAX_COUNT_REQUESTS buckets get measured and anything older is reported
 * as null (unknown) rather than guessed at.
 */
const BSKY_MAX_COUNT_REQUESTS = 12;
const BSKY_STAGGER_MS = 300;
/** Ceiling on how long the bucket sweep may spend waiting out throttling. */
const BSKY_DEADLINE_MS = 20_000;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Retry with backoff — a 403 from the AppView is burst throttling, and the
 * token bucket refills at roughly 1/s, so waiting is the correct response.
 * `deadline` stops a heavily throttled window from stretching tool latency.
 */
async function bskyGetRetry<T>(method: string, params: Record<string, string | number>, deadline = Infinity): Promise<T> {
  const backoffs = [1000, 2200];
  let lastErr: unknown;
  for (let attempt = 0; ; attempt++) {
    try {
      return await bskyGet<T>(method, params);
    } catch (err) {
      lastErr = err;
      if (attempt >= backoffs.length || Date.now() + backoffs[attempt] > deadline) break;
      await sleep(backoffs[attempt]);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error('Bluesky AppView unreachable');
}

async function blueskyVolume(topic: string, grid: Grid): Promise<NetResult> {
  const method =
    'app.bsky.feed.searchPosts with since/until per bucket — hitsTotal is a server-side match count, so each measured bucket is counted directly rather than sampled';
  const deadline = Date.now() + BSKY_DEADLINE_MS;
  try {
    let capped = false;
    // Newest-first budget: the buckets we can afford are the most recent ones.
    const offset = Math.max(0, grid.starts.length - BSKY_MAX_COUNT_REQUESTS);
    const budgeted = grid.starts.slice(offset);

    // Newest-first: the throttle bites partway through, and a gap in last week's
    // data is far worse than a gap at the far edge of the window.
    const order = budgeted.map((_, i) => i).reverse();
    const measured = new Array<number | null>(budgeted.length).fill(null);
    await pooled(order, 2, async (slot) => {
      const start = budgeted[slot];
      const end = Math.min(start + grid.bucketMs, grid.toMs);
      try {
        const r = await bskyGetRetry<BskySearch>('app.bsky.feed.searchPosts', {
          q: topic,
          limit: 1,
          sort: 'latest',
          since: iso(start),
          until: iso(end),
        }, deadline);
        const n = typeof r.hitsTotal === 'number' ? r.hitsTotal : (r.posts?.length ?? 0);
        if (n >= BSKY_HITS_CAP) capped = true;
        measured[slot] = n;
      } catch {
        measured[slot] = null; // this bucket is unknown, not zero
      } finally {
        await sleep(BSKY_STAGGER_MS); // hold the pace under the AppView's burst limit
      }
    });
    const counts: (number | null)[] = [...grid.starts.slice(0, offset).map(() => null), ...measured];

    // One extra call for engagement — the counting calls fetch limit=1 to stay cheap.
    let engagement: Record<string, unknown> | null = null;
    try {
      const sample = await bskyGetRetry<BskySearch>('app.bsky.feed.searchPosts', {
        q: topic,
        limit: 100,
        sort: 'latest',
        since: iso(grid.fromMs),
        until: iso(grid.toMs),
      }, deadline + 4000);
      const posts = sample.posts ?? [];
      if (posts.length) {
        const likes = posts.reduce((a, p) => a + (p.likeCount ?? 0), 0);
        const reposts = posts.reduce((a, p) => a + (p.repostCount ?? 0), 0);
        const replies = posts.reduce((a, p) => a + (p.replyCount ?? 0), 0);
        engagement = {
          basis: `sample of the ${posts.length} most recent matching posts in the window (not the whole window)`,
          sample_size: posts.length,
          likes,
          reposts,
          replies,
          avg_likes_per_post: +(likes / posts.length).toFixed(2),
        };
      }
    } catch {
      /* engagement is optional; counts already succeeded */
    }

    const okBuckets = counts.filter((c) => c !== null) as number[];
    const overBudget = offset > 0;
    const failedInBudget = budgeted.length - measured.filter((c) => c !== null).length;
    // Coverage starts at the oldest bucket we actually measured.
    const firstMeasured = counts.findIndex((c) => c !== null);
    const coveredFromMs = firstMeasured >= 0 ? grid.starts[firstMeasured] : grid.toMs;

    return {
      network: 'bluesky',
      status: 'ok',
      counts,
      method,
      count_basis: 'server_side_count_per_bucket',
      complete: !overBudget && failedInBudget === 0,
      covered_from: iso(coveredFromMs),
      covered_to: iso(grid.toMs),
      posts_counted: okBuckets.reduce((a, b) => a + b, 0),
      truncated: capped || overBudget,
      engagement,
      note:
        (overBudget
          ? `Bluesky's public AppView throttles bursts to roughly 10 requests, and counting costs one request per bucket, ` +
            `so only the most recent ${BSKY_MAX_COUNT_REQUESTS} of ${grid.starts.length} buckets were measured; older ones are null (unknown), not zero. ` +
            `Ask for a shorter window (or bucket="day" instead of "hour") to get Bluesky across the whole span. `
          : '') +
        (failedInBudget ? `${failedInBudget} of ${budgeted.length} bucket queries failed and are null (unknown). ` : '') +
        (capped ? `At least one bucket hit the AppView's ${BSKY_HITS_CAP} hitsTotal ceiling, so that count is a floor. ` : '') +
        "Measured buckets carry Bluesky's own match count for that time window, so a 0 there is a measured zero.",
    };
  } catch (err) {
    return errResult('bluesky', grid, method, err);
  }
}

// ── Hacker News (Algolia) ─────────────────────────────────────────────────

interface HnHit {
  created_at_i?: number;
  created_at?: string;
  title?: string | null;
  points?: number | null;
  num_comments?: number | null;
}
interface HnSearch {
  hits?: HnHit[];
  nbHits?: number;
  nbPages?: number;
}

const HN_PAGE_SIZE = 1000;
const HN_MAX_PAGES = 2; // 2,000 items is plenty for any realistic topic/window

async function hackernewsVolume(topic: string, grid: Grid): Promise<NetResult> {
  const method =
    'hn.algolia.com/search_by_date with numericFilters on created_at_i — stories AND comments are fetched and bucketed locally';
  try {
    const fromSec = Math.floor(grid.fromMs / 1000);
    const toSec = Math.ceil(grid.toMs / 1000);
    const hits: HnHit[] = [];
    let nbHits = 0;
    for (let page = 0; page < HN_MAX_PAGES; page++) {
      const qs = new URLSearchParams({
        query: topic,
        numericFilters: `created_at_i>${fromSec},created_at_i<${toSec}`,
        hitsPerPage: String(HN_PAGE_SIZE),
        page: String(page),
      });
      const r = await httpJson<HnSearch>(`${HN_BASE}/search_by_date?${qs}`);
      // Take the largest nbHits seen: Algolia caps pagination at 1,000 results
      // and a page past that cap comes back with hits:[] and nbHits:0, which
      // would otherwise erase the very evidence that we were truncated.
      nbHits = Math.max(nbHits, r.nbHits ?? 0);
      hits.push(...(r.hits ?? []));
      if (hits.length >= nbHits || (r.hits?.length ?? 0) < HN_PAGE_SIZE) break;
    }

    const stamps = hits.map((h) => parseTs(h.created_at_i ?? h.created_at)).filter((n) => Number.isFinite(n));
    const truncated = hits.length < nbHits;
    // search_by_date is newest-first, so a truncated fetch only reaches back to its oldest hit.
    const oldestReached = truncated && stamps.length ? Math.min(...stamps) : grid.fromMs;
    const counts = countsFromPosts(grid, stamps, oldestReached);

    const points = hits.reduce((a, h) => a + (h.points ?? 0), 0);
    const comments = hits.reduce((a, h) => a + (h.num_comments ?? 0), 0);

    return {
      network: 'hackernews',
      status: 'ok',
      counts,
      method,
      count_basis: 'counted_from_fetched_posts',
      complete: !truncated,
      covered_from: iso(oldestReached),
      covered_to: iso(grid.toMs),
      posts_counted: stamps.filter((t) => t >= oldestReached).length,
      truncated,
      engagement: {
        basis: truncated ? `the ${hits.length} most recent items fetched` : 'every matching item in the window',
        items: hits.length,
        points,
        comments,
      },
      note:
        'Counts include HN comments as well as story submissions — on HN a comment is a post. ' +
        (truncated
          ? `Algolia reported ${nbHits} matches but only ${hits.length} were fetched, so buckets older than ${iso(oldestReached)} are null (unknown).`
          : 'The full window was fetched, so a 0 here is a measured zero.'),
    };
  } catch (err) {
    return errResult('hackernews', grid, method, err);
  }
}

// ── Reddit (Atom feeds) ───────────────────────────────────────────────────

const REDDIT_FEED_CAP = 100;

function redditTimeWindow(days: number): 'day' | 'week' | 'month' | 'year' {
  if (days <= 1) return 'day';
  if (days <= 7) return 'week';
  if (days <= 31) return 'month';
  return 'year';
}

/**
 * Post timestamps from a Reddit Atom feed.
 *
 * search.rss prepends SUBREDDIT matches (`t5_` ids) before the actual posts.
 * Those entries have no <published>, only an <updated> carrying the subreddit's
 * creation date — reading them as posts drags the coverage floor back to 2010
 * and makes a 4-hour feed look like a 16-year one. Only `t3_` (posts) and `t1_`
 * (comments) entries count; `entries` is the raw count, which is what the
 * 100-item truncation test needs.
 */
function atomPublished(xml: string): { stamps: number[]; entries: number } {
  const entries = xml.match(/<entry>[\s\S]*?<\/entry>/g) ?? [];
  const stamps: number[] = [];
  for (const e of entries) {
    const id = e.match(/<id>(t\d)_/);
    if (id && id[1] !== 't3' && id[1] !== 't1') continue; // subreddit / account, not a post
    const m = e.match(/<published>([^<]+)<\/published>/);
    if (m) {
      const t = Date.parse(m[1]);
      if (Number.isFinite(t)) stamps.push(t);
    }
  }
  return { stamps, entries: entries.length };
}

async function redditVolume(topic: string, grid: Grid, days: number): Promise<NetResult> {
  const method = `www.reddit.com/search.rss (Atom) — newest ${REDDIT_FEED_CAP} matching posts in Reddit's t=${redditTimeWindow(days)} window`;
  try {
    const qs = new URLSearchParams({
      q: topic,
      sort: 'new',
      limit: String(REDDIT_FEED_CAP),
      t: redditTimeWindow(days),
    });
    const xml = await httpText(`${REDDIT_BASE}/search.rss?${qs}`, REDDIT_UA, 'application/atom+xml, application/xml, text/xml');
    const { stamps, entries } = atomPublished(xml);
    // Reddit filters server-side to its t-window, which always covers ours. So a
    // short feed means we saw everything; a full feed means we were truncated.
    const truncated = entries >= REDDIT_FEED_CAP;
    const oldestReached = truncated && stamps.length ? Math.min(...stamps) : grid.fromMs;
    const counts = countsFromPosts(grid, stamps, oldestReached);

    return {
      network: 'reddit',
      status: 'ok',
      counts,
      method,
      count_basis: 'counted_from_fetched_posts',
      complete: !truncated,
      covered_from: iso(oldestReached),
      covered_to: iso(grid.toMs),
      posts_counted: stamps.filter((t) => t >= oldestReached).length,
      truncated,
      engagement: null,
      note:
        "Reddit's public Atom feeds carry no vote score and no comment count, so engagement is unavailable here " +
        '(the JSON endpoints that would carry it reject unauthenticated datacenter traffic). ' +
        (truncated
          ? `The feed capped at ${REDDIT_FEED_CAP} entries, so buckets older than ${iso(oldestReached)} are null (unknown).`
          : `The feed returned fewer than ${REDDIT_FEED_CAP} entries, meaning Reddit had nothing more to give for the window — a 0 here is a measured zero.`),
    };
  } catch (err) {
    const r = errResult('reddit', grid, method, err);
    if (r.error?.includes('429')) {
      r.note = 'Reddit rate-limited the request (429). Its buckets are null (unknown), not zero. Retry in a minute.';
    }
    return r;
  }
}

// ── Mastodon (public hashtag timeline) ────────────────────────────────────

interface MastoStatus {
  id?: string;
  created_at?: string;
  favourites_count?: number;
  reblogs_count?: number;
  replies_count?: number;
}

const MASTO_PAGE_SIZE = 40;
const MASTO_MAX_PAGES = 8; // 320 statuses

/** Mastodon hashtag timelines key on a single tag token, so a phrase collapses to one. */
function toHashtag(topic: string): string {
  const cleaned = topic
    .normalize('NFKD')
    .replace(/[^\p{L}\p{N}]+/gu, '')
    .toLowerCase();
  return cleaned;
}

async function mastodonVolume(topic: string, grid: Grid, instance: string): Promise<NetResult> {
  const tag = toHashtag(topic);
  const method = `https://${instance}/api/v1/timelines/tag/${tag || '<empty>'} (public hashtag timeline, paged back with max_id)`;
  try {
    if (!tag) throw new Error('topic contains no letters or digits to form a hashtag');
    const stamps: number[] = [];
    const statuses: MastoStatus[] = [];
    let maxId: string | undefined;
    let reachedWindowStart = false;

    for (let page = 0; page < MASTO_MAX_PAGES; page++) {
      const qs = new URLSearchParams({ limit: String(MASTO_PAGE_SIZE) });
      if (maxId) qs.set('max_id', maxId);
      const batch = await httpJson<MastoStatus[]>(`https://${instance}/api/v1/timelines/tag/${encodeURIComponent(tag)}?${qs}`);
      if (!Array.isArray(batch) || batch.length === 0) {
        reachedWindowStart = true; // ran out of statuses entirely
        break;
      }
      statuses.push(...batch);
      for (const s of batch) {
        const t = parseTs(s.created_at);
        if (Number.isFinite(t)) stamps.push(t);
      }
      const oldest = Math.min(...batch.map((s) => parseTs(s.created_at)).filter((n) => Number.isFinite(n)));
      maxId = batch[batch.length - 1]?.id;
      if (Number.isFinite(oldest) && oldest <= grid.fromMs) {
        reachedWindowStart = true;
        break;
      }
      if (!maxId || batch.length < MASTO_PAGE_SIZE) {
        reachedWindowStart = true;
        break;
      }
    }

    const truncated = !reachedWindowStart;
    const oldestReached = truncated && stamps.length ? Math.min(...stamps) : grid.fromMs;
    const counts = countsFromPosts(grid, stamps, oldestReached);

    const inWindow = statuses.filter((s) => parseTs(s.created_at) >= oldestReached);
    const engagement = inWindow.length
      ? {
          basis: `the ${inWindow.length} statuses fetched from ${instance}`,
          favourites: inWindow.reduce((a, s) => a + (s.favourites_count ?? 0), 0),
          reblogs: inWindow.reduce((a, s) => a + (s.reblogs_count ?? 0), 0),
          replies: inWindow.reduce((a, s) => a + (s.replies_count ?? 0), 0),
        }
      : null;

    return {
      network: 'mastodon',
      status: 'ok',
      counts,
      method,
      count_basis: 'counted_from_fetched_posts',
      complete: !truncated,
      covered_from: iso(oldestReached),
      covered_to: iso(grid.toMs),
      posts_counted: stamps.filter((t) => t >= oldestReached).length,
      truncated,
      engagement,
      note:
        `Matched on the hashtag #${tag}, so Mastodon posts that mention "${topic}" as plain text are outside this count. ` +
        `The timeline is scoped to ${instance} and shows what that instance has federated in, which is a subset of the fediverse. ` +
        (truncated
          ? `Paging stopped after ${MASTO_MAX_PAGES} pages, so buckets older than ${iso(oldestReached)} are null (unknown).`
          : 'Paging reached the start of the window, so a 0 here is a measured zero.'),
    };
  } catch (err) {
    return errResult('mastodon', grid, method, err);
  }
}

// ── social_topic_volume ───────────────────────────────────────────────────

async function socialTopicVolume(args: Record<string, unknown>): Promise<unknown> {
  const topic = String(args.topic ?? '').trim();
  if (!topic) throw new Error('topic is required');

  const bucket: 'day' | 'hour' = args.bucket === 'hour' ? 'hour' : 'day';
  let days = Math.min(30, Math.max(1, Math.floor(Number(args.days ?? 7)) || 7));

  const caveats: string[] = [];
  const warnings: string[] = [];

  const MAX_HOUR_DAYS = 3;
  if (bucket === 'hour' && days > MAX_HOUR_DAYS) {
    caveats.push(
      `Hourly buckets are capped at ${MAX_HOUR_DAYS} days (${MAX_HOUR_DAYS * 24} buckets) to keep the per-bucket queries bounded; days was reduced from ${days} to ${MAX_HOUR_DAYS}. Use bucket="day" for longer windows.`,
    );
    days = MAX_HOUR_DAYS;
  }

  const requested = Array.isArray(args.networks) ? (args.networks as unknown[]).map((n) => String(n).toLowerCase()) : null;
  const unknownNets = requested?.filter((n) => !ALL_NETWORKS.includes(n as Network)) ?? [];
  if (unknownNets.length) warnings.push(`Ignored unrecognized network(s): ${unknownNets.join(', ')}. Valid: ${ALL_NETWORKS.join(', ')}.`);
  const nets: Network[] = requested ? ALL_NETWORKS.filter((n) => requested.includes(n)) : [...ALL_NETWORKS];
  if (!nets.length) throw new Error(`networks must include at least one of: ${ALL_NETWORKS.join(', ')}`);

  const instance = String(args.mastodon_instance ?? MASTODON_DEFAULT_INSTANCE).replace(/^https?:\/\//, '').replace(/\/+$/, '');
  const grid = buildGrid(days, bucket);

  const jobs = nets.map((n) => {
    if (n === 'bluesky') return blueskyVolume(topic, grid);
    if (n === 'hackernews') return hackernewsVolume(topic, grid);
    if (n === 'reddit') return redditVolume(topic, grid, days);
    return mastodonVolume(topic, grid, instance);
  });

  const settled = await Promise.allSettled(jobs);
  const results: NetResult[] = settled.map((s, i) =>
    s.status === 'fulfilled' ? s.value : errResult(nets[i], grid, 'unknown', s.reason),
  );

  for (const r of results) {
    if (r.status === 'error') warnings.push(`${r.network}: ${r.error} — its buckets are null (unknown), not zero.`);
    else if (r.truncated) warnings.push(`${r.network}: reach was truncated; buckets before ${r.covered_from} are null (unknown), not zero.`);
  }

  // Series
  const series = grid.starts.map((start, i) => {
    const byNetwork: Record<string, number | null> = {};
    for (const r of results) byNetwork[r.network] = r.counts[i];
    const known = Object.values(byNetwork).filter((v) => v !== null) as number[];
    const unknown = Object.entries(byNetwork).filter(([, v]) => v === null).map(([k]) => k);
    return {
      bucket_start: iso(start),
      bucket_end: iso(Math.min(start + grid.bucketMs, grid.toMs)),
      total: known.reduce((a, b) => a + b, 0),
      total_is_partial: unknown.length > 0,
      unobserved_networks: unknown,
      by_network: byNetwork,
    };
  });

  // Totals are summed from the SERIES so they never disagree with it. A network
  // whose reach only covers part of the newest bucket contributes 0 here while
  // still reporting what it saw as posts_seen_in_covered_range.
  const totalsByNetwork: Record<string, number | null> = {};
  for (const r of results) {
    const known = r.counts.filter((c) => c !== null) as number[];
    totalsByNetwork[r.network] = r.status === 'ok' && known.length ? known.reduce((a, b) => a + b, 0) : null;
  }
  const overall = Object.values(totalsByNetwork).filter((v) => v !== null).reduce((a: number, b) => a + (b as number), 0);

  const engagement: Record<string, unknown> = {};
  for (const r of results) engagement[r.network] = r.engagement;

  const okResults = results.filter((r) => r.status === 'ok');
  const fullyCovered = okResults.filter((r) => r.complete);
  const coveredFromMs = okResults.length ? Math.max(...okResults.map((r) => Date.parse(r.covered_from as string))) : grid.toMs;

  caveats.push(
    'Buckets are aligned to whole UTC ' + (bucket === 'hour' ? 'hours' : 'days') + `, so the window starts at ${iso(grid.fromMs)}.`,
    'A bucket value of null means that network was not observed that far back (or errored). Only a numeric 0 means "measured, nothing found".',
    'Cross-network totals mix very different populations and posting norms; the trend within one network is more meaningful than the sum across them.',
  );
  if (nets.includes('reddit')) caveats.push('Reddit engagement (score, comment count) is unavailable from its public Atom feeds.');
  if (nets.includes('mastodon')) caveats.push(`Mastodon is matched by hashtag #${toHashtag(topic)} on ${instance} only.`);
  if (nets.includes('hackernews')) caveats.push('Hacker News counts include comments, not only story submissions.');

  return {
    topic,
    window: { from: iso(grid.fromMs), to: iso(grid.toMs), days, bucket },
    window_actually_covered: {
      from: iso(coveredFromMs),
      to: iso(grid.toMs),
      complete: okResults.length > 0 && okResults.every((r) => r.complete),
      fully_covered_networks: fullyCovered.map((r) => r.network),
      partially_covered_networks: okResults.filter((r) => !r.complete).map((r) => r.network),
      unavailable_networks: results.filter((r) => r.status === 'error').map((r) => r.network),
      explanation:
        'Every network in fully_covered_networks was measured across the whole requested window, so their zeros are real zeros. ' +
        'For the others, buckets before their own covered_from are null.',
    },
    bucket,
    series,
    totals: {
      by_network: totalsByNetwork,
      overall,
      note:
        'Summed over fully observed buckets only, so these agree with the series. A network can report posts under ' +
        'posts_seen_in_covered_range while totalling 0 here, when its reach covers only part of the newest bucket. ' +
        'A null total means the network contributed no observed bucket at all.',
    },
    engagement,
    networks: results.map((r) => ({
      network: r.network,
      status: r.status,
      method: r.method,
      count_basis: r.count_basis,
      complete: r.complete,
      covered_from: r.covered_from,
      covered_to: r.covered_to,
      posts_seen_in_covered_range: r.posts_counted,
      truncated: r.truncated,
      note: r.note,
      error: r.error,
    })),
    coverage_note: COVERAGE_NOTE,
    caveats,
    warnings,
  };
}

// ── social_account_activity ───────────────────────────────────────────────

interface BskyFeedItem {
  post: BskyPost;
  reason?: unknown; // present when the item is a repost of someone else
}
interface BskyAuthorFeed {
  feed?: BskyFeedItem[];
  cursor?: string;
}

const BSKY_FEED_PAGES = 4; // 400 posts

async function blueskyAccount(handle: string, grid: Grid) {
  const stamps: number[] = [];
  const posts: BskyPost[] = [];
  let cursor: string | undefined;
  let reachedStart = false;
  let repostsSkipped = 0;

  for (let page = 0; page < BSKY_FEED_PAGES; page++) {
    const params: Record<string, string | number> = { actor: handle, limit: 100 };
    if (cursor) params.cursor = cursor;
    const r = await bskyGet<BskyAuthorFeed>('app.bsky.feed.getAuthorFeed', params);
    const feed = r.feed ?? [];
    if (!feed.length) {
      reachedStart = true;
      break;
    }
    let oldest = Infinity;
    for (const item of feed) {
      const t = parseTs(item.post?.record?.createdAt ?? item.post?.indexedAt);
      if (Number.isFinite(t)) oldest = Math.min(oldest, t);
      if (item.reason) {
        repostsSkipped++;
        continue;
      }
      if (Number.isFinite(t)) {
        stamps.push(t);
        posts.push(item.post);
      }
    }
    cursor = r.cursor;
    if (!cursor) {
      reachedStart = true;
      break;
    }
    if (Number.isFinite(oldest) && oldest <= grid.fromMs) {
      reachedStart = true;
      break;
    }
  }

  const inWindow = posts.filter((p) => parseTs(p.record?.createdAt ?? p.indexedAt) >= grid.fromMs);
  const engagement = inWindow.length
    ? {
        posts_measured: inWindow.length,
        avg_likes: +(inWindow.reduce((a, p) => a + (p.likeCount ?? 0), 0) / inWindow.length).toFixed(2),
        avg_reposts: +(inWindow.reduce((a, p) => a + (p.repostCount ?? 0), 0) / inWindow.length).toFixed(2),
        avg_replies: +(inWindow.reduce((a, p) => a + (p.replyCount ?? 0), 0) / inWindow.length).toFixed(2),
      }
    : null;

  return {
    stamps,
    reachedStart,
    lastPostAt: stamps.length ? iso(Math.max(...stamps)) : null,
    engagement,
    note: `Reposts are excluded from the cadence count (${repostsSkipped} skipped); only the account's own posts and replies are counted.`,
    method: 'app.bsky.feed.getAuthorFeed, paged with cursor',
  };
}

interface MastoAccount {
  id?: string;
  acct?: string;
  statuses_count?: number;
  error?: string;
}

async function mastodonAccount(user: string, instance: string, grid: Grid) {
  const acct = await httpJson<MastoAccount>(`https://${instance}/api/v1/accounts/lookup?acct=${encodeURIComponent(user)}`);
  if (!acct?.id) throw new Error(`account "${user}" not found on ${instance}`);

  const stamps: number[] = [];
  const statuses: MastoStatus[] = [];
  let maxId: string | undefined;
  let reachedStart = false;

  for (let page = 0; page < 6; page++) {
    const qs = new URLSearchParams({ limit: String(MASTO_PAGE_SIZE), exclude_reblogs: 'true' });
    if (maxId) qs.set('max_id', maxId);
    const batch = await httpJson<MastoStatus[]>(`https://${instance}/api/v1/accounts/${acct.id}/statuses?${qs}`);
    if (!Array.isArray(batch) || batch.length === 0) {
      reachedStart = true;
      break;
    }
    statuses.push(...batch);
    for (const s of batch) {
      const t = parseTs(s.created_at);
      if (Number.isFinite(t)) stamps.push(t);
    }
    const oldest = Math.min(...batch.map((s) => parseTs(s.created_at)).filter((n) => Number.isFinite(n)));
    maxId = batch[batch.length - 1]?.id;
    if (!maxId || batch.length < MASTO_PAGE_SIZE || (Number.isFinite(oldest) && oldest <= grid.fromMs)) {
      reachedStart = true;
      break;
    }
  }

  const inWindow = statuses.filter((s) => parseTs(s.created_at) >= grid.fromMs);
  const engagement = inWindow.length
    ? {
        posts_measured: inWindow.length,
        avg_favourites: +(inWindow.reduce((a, s) => a + (s.favourites_count ?? 0), 0) / inWindow.length).toFixed(2),
        avg_reblogs: +(inWindow.reduce((a, s) => a + (s.reblogs_count ?? 0), 0) / inWindow.length).toFixed(2),
        avg_replies: +(inWindow.reduce((a, s) => a + (s.replies_count ?? 0), 0) / inWindow.length).toFixed(2),
      }
    : null;

  return {
    stamps,
    reachedStart,
    lastPostAt: stamps.length ? iso(Math.max(...stamps)) : null,
    engagement,
    note: `Boosts are excluded. Lifetime status count reported by ${instance}: ${acct.statuses_count ?? 'unknown'}.`,
    method: `https://${instance}/api/v1/accounts/{id}/statuses, paged with max_id`,
  };
}

async function redditAccount(user: string, grid: Grid) {
  const xml = await httpText(
    `${REDDIT_BASE}/user/${encodeURIComponent(user)}.rss?limit=100`,
    REDDIT_UA,
    'application/atom+xml, application/xml, text/xml',
  );
  const { stamps } = atomPublished(xml);
  if (!stamps.length) throw new Error(`no Atom entries for u/${user}`);
  return {
    stamps,
    // The user feed is short (~25 entries) and has no paging, so reach is whatever it gave us.
    reachedStart: Math.min(...stamps) <= grid.fromMs,
    lastPostAt: iso(Math.max(...stamps)),
    engagement: null,
    note: "Reddit's user Atom feed returns roughly the last 25 items and carries no score or comment count.",
    method: 'www.reddit.com/user/<name>.rss (Atom)',
  };
}

interface HandleGuess {
  network: Network;
  user: string;
  instance?: string;
}

function guessNetworks(raw: string): HandleGuess[] {
  const handle = raw.trim().replace(/^https?:\/\/[^/]+\//, '');
  // @user@instance  or  user@instance
  const fedi = handle.match(/^@?([^@\s/]+)@([a-z0-9][a-z0-9.-]*\.[a-z]{2,})$/i);
  if (fedi) return [{ network: 'mastodon', user: `${fedi[1]}@${fedi[2]}`, instance: fedi[2] }];

  const redditM = handle.match(/^\/?u\/([A-Za-z0-9_-]{2,})$/);
  if (redditM) return [{ network: 'reddit', user: redditM[1] }];

  const bare = handle.replace(/^@/, '');
  if (bare.startsWith('did:')) return [{ network: 'bluesky', user: bare }];
  // A dotted handle is a Bluesky handle (they are always domain-shaped).
  if (/^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}$/i.test(bare)) return [{ network: 'bluesky', user: bare }];

  // Ambiguous bare name — try the plausible readings in order.
  return [
    { network: 'bluesky', user: `${bare}.bsky.social` },
    { network: 'mastodon', user: bare, instance: MASTODON_DEFAULT_INSTANCE },
    { network: 'reddit', user: bare },
  ];
}

async function socialAccountActivity(args: Record<string, unknown>): Promise<unknown> {
  const raw = String(args.handle ?? '').trim();
  if (!raw) throw new Error('handle is required');
  const days = Math.min(90, Math.max(1, Math.floor(Number(args.days ?? 30)) || 30));
  const grid = buildGrid(days, 'day');

  const guesses = guessNetworks(raw);
  const tried: { network: Network; identifier: string; result: string }[] = [];
  const warnings: string[] = [];

  // Each network's account reader returns the same shape except for its engagement
  // key names (Bluesky likes/reposts, Mastodon favourites/reblogs), so the slot is
  // typed as the union of all three rather than Bluesky's shape alone.
  type AccountRead =
    | Awaited<ReturnType<typeof blueskyAccount>>
    | Awaited<ReturnType<typeof mastodonAccount>>
    | Awaited<ReturnType<typeof redditAccount>>;
  let hit: AccountRead | null = null;
  let hitGuess: HandleGuess | null = null;

  for (const g of guesses) {
    try {
      const r =
        g.network === 'bluesky'
          ? await blueskyAccount(g.user, grid)
          : g.network === 'mastodon'
            ? await mastodonAccount(g.user, g.instance ?? MASTODON_DEFAULT_INSTANCE, grid)
            : await redditAccount(g.user, grid);
      tried.push({ network: g.network, identifier: g.user, result: `resolved, ${r.stamps.length} posts read` });
      hit = r;
      hitGuess = g;
      break;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      tried.push({ network: g.network, identifier: g.user, result: msg });
    }
  }

  if (!hit || !hitGuess) {
    return {
      handle: raw,
      network: null,
      resolved: false,
      networks_tried: tried,
      coverage_note: COVERAGE_NOTE,
      caveats: [
        `The handle "${raw}" did not resolve on any readable network. Bluesky handles look like "user.bsky.social", ` +
          'Mastodon handles look like "@user@instance.tld", Reddit handles look like "u/name".',
      ],
      warnings,
    };
  }

  const oldestReached = hit.reachedStart ? grid.fromMs : hit.stamps.length ? Math.min(...hit.stamps) : grid.toMs;
  const counts = countsFromPosts(grid, hit.stamps, oldestReached);
  const series = grid.starts.map((s, i) => ({
    date: iso(s).slice(0, 10),
    bucket_start: iso(s),
    posts: counts[i],
    observed: counts[i] !== null,
  }));
  const observed = counts.filter((c) => c !== null) as number[];
  const totalPosts = observed.reduce((a, b) => a + b, 0);

  if (!hit.reachedStart) {
    warnings.push(
      `Paging stopped before reaching the start of the ${days}-day window; days before ${iso(oldestReached)} are null (unknown), not zero.`,
    );
  }

  return {
    handle: raw,
    network: hitGuess.network,
    resolved: true,
    resolved_identifier: hitGuess.user + (hitGuess.instance && hitGuess.network === 'mastodon' ? ` (via ${hitGuess.instance})` : ''),
    networks_tried: tried,
    window: { from: iso(grid.fromMs), to: iso(grid.toMs), days, bucket: 'day' },
    window_actually_covered: {
      from: iso(oldestReached),
      to: iso(grid.toMs),
      complete: hit.reachedStart,
      explanation: hit.reachedStart
        ? 'The account feed was read back past the start of the window, so a 0 on any day is a measured zero.'
        : 'The feed was truncated before the start of the window; days before covered_from are null (unknown).',
    },
    method: hit.method,
    series,
    totals: {
      posts_in_covered_range: totalPosts,
      days_observed: observed.length,
      days_unobserved: counts.length - observed.length,
      avg_posts_per_observed_day: observed.length ? +(totalPosts / observed.length).toFixed(2) : null,
      active_days: observed.filter((c) => c > 0).length,
    },
    engagement_averages: hit.engagement,
    last_post_at: hit.lastPostAt,
    coverage_note: COVERAGE_NOTE,
    caveats: [
      hit.note,
      'A day with null posts was not observed; only a numeric 0 means the account was measured and posted nothing.',
      'Cadence reflects public posts visible without authentication; followers-only or otherwise restricted posts are outside this view.',
    ],
    warnings,
  };
}

// ── tool definitions ──────────────────────────────────────────────────────

const tools: McpToolExport['tools'] = [
  {
    name: 'social_topic_volume',
    description:
      'SOCIAL MEDIA VOLUME as a TIME SERIES: how many posts mention a topic, bucketed by day or hour, so you can see the POST VOLUME TREND and whether SOCIAL CHATTER ABOUT A TOPIC is rising or fading. Answers "how much is X being discussed", "BUZZ OVER TIME for X", "is attention on X growing this week". Returns a per-bucket count broken out by network, running totals, engagement sums where the source exposes them, and an explicit window_actually_covered block. A bucket is a number only where that network was truly measured; where reach ran out it is null, so a measured zero is always distinguishable from an unobserved gap.' +
      DESC_TAIL +
      ' Examples: topic "bitcoin", days 7, bucket "day" for a week-long attention curve; topic "openai", days 2, bucket "hour", networks ["bluesky","hackernews"] for an intraday spike check.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        topic: {
          type: 'string' as const,
          description: 'The search term to count posts for. Examples: "bitcoin", "openai", "federal reserve". Mastodon matches this as a single hashtag.',
        },
        days: {
          type: 'number' as const,
          description: 'How many days back to measure. Default 7, maximum 30. With bucket="hour" this is capped at 3 days.',
        },
        bucket: {
          type: 'string' as const,
          enum: ['day', 'hour'],
          description: 'Bucket width for the series. Default "day". Use "hour" for intraday spikes (window capped at 3 days).',
        },
        networks: {
          type: 'array' as const,
          items: { type: 'string' as const, enum: [...ALL_NETWORKS] },
          description: 'Subset of networks to measure. Default is all four: bluesky, mastodon, reddit, hackernews.',
        },
        mastodon_instance: {
          type: 'string' as const,
          description: 'Mastodon instance whose public hashtag timeline is read. Default mastodon.social. Examples: "mastodon.world", "fosstodon.org".',
        },
      },
      required: ['topic'],
    },
  },
  {
    name: 'social_account_activity',
    description:
      'POSTING CADENCE for one social media account as a TIME SERIES: posts per day over a window, plus average engagement per post and the timestamp of the most recent post. Answers "how often does this account post", "has this account gone quiet", "what is their POST VOLUME TREND", "how active is @someone on social media". The network is inferred from the handle shape (a dotted handle is Bluesky, "@user@instance.tld" is Mastodon, "u/name" is Reddit); when the shape is ambiguous the response lists every network tried in networks_tried. Days with a null count were outside the reach of the feed and stay distinguishable from days with a measured zero.' +
      DESC_TAIL +
      ' Examples: handle "bsky.app", days 30 for a month of posting cadence; handle "@Mastodon@mastodon.social", days 90 for a long-run activity check.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        handle: {
          type: 'string' as const,
          description:
            'The account handle. Bluesky: "user.bsky.social" or a did:. Mastodon: "@user@mastodon.social". Reddit: "u/name". A bare name is tried against each in turn.',
        },
        days: {
          type: 'number' as const,
          description: 'How many days of cadence to report. Default 30, maximum 90.',
        },
      },
      required: ['handle'],
    },
  },
];

async function callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
  switch (name) {
    case 'social_topic_volume':
      return socialTopicVolume(args);
    case 'social_account_activity':
      return socialAccountActivity(args);
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

export default { tools, callTool, meter: { credits: 1 } } satisfies McpToolExport;
