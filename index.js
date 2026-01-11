const { Pool } = require("pg");
const pLimit = require("p-limit");
const { TikTokLiveConnection } = require("@adamjessop/tiktok-live-connector");

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("Fatal: DATABASE_URL is not set in Railway Variables");
  process.exit(1);
}

/*
Defaults chosen for:
  around 1300 creators
  minimal logs
  reduced chance of TikTok throttling

You can override via Railway Variables if needed:
  POLL_INTERVAL_SECONDS (default 60)
  CONCURRENCY (default 4)
  OFFLINE_MISS_THRESHOLD (default 2)
  CREATOR_LOOKBACK_DAYS (default 3)
*/

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 60);
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
const OFFLINE_MISS_THRESHOLD = Number(process.env.OFFLINE_MISS_THRESHOLD || 2);
const CREATOR_LOOKBACK_DAYS = Number(process.env.CREATOR_LOOKBACK_DAYS || 3);

// Minimal log mode, requested
const LOG_ERRORS_ONLY = true;

function logInfo(...args) {
  if (!LOG_ERRORS_ONLY) console.log(...args);
}

function logError(...args) {
  console.error(...args);
}

// Neon needs SSL. This works for Neon pooler connections.
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

async function getCreatorsToMonitor() {
  // Uses fasttrack_daily only, last N days, demo data excluded
  const sql = `
    select distinct
      creator_id,
      "Creator's username" as tiktok_username
    from fasttrack_daily
    where "Creator's username" is not null
      and "Creator's username" <> ''
      and creator_id is not null
      and creator_id <> ''
      and "Data period" >= (current_date - ($1::int || ' days')::interval)
      and is_demo_data is not true
  `;

  const { rows } = await pool.query(sql, [CREATOR_LOOKBACK_DAYS]);

  // Multiple rows per creator_id over 3 days are expected, distinct collapses them.
  return rows
    .map((r) => ({
      creator_id: String(r.creator_id).trim(),
      tiktok_username: String(r.tiktok_username || "")
        .trim()
        .replace(/^@/, "")
    }))
    .filter((r) => r.creator_id && r.tiktok_username);
}

async function isLive(username) {
  // Unofficial check, can fail on some IPs. Failures are treated as unknown, not offline.
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: false
  });

  const result = await conn.fetchIsLive();
  return Boolean(result);
}

async function markLive(creator) {
  // Upsert into live_now
  await pool.query(
    `
    insert into live_now (
      creator_id,
      tiktok_username,
      went_live_at,
      last_seen_live_at,
      last_check_at,
      miss_count,
      updated_at
    )
    values ($1,$2,now(),now(),now(),0,now())
    on conflict (creator_id) do update set
      tiktok_username = excluded.tiktok_username,
      last_seen_live_at = now(),
      last_check_at = now(),
      miss_count = 0,
      updated_at = now()
    `,
    [creator.creator_id, creator.tiktok_username]
  );

  // Ensure an open session exists
  await pool.query(
    `
    insert into live_sessions (creator_id, tiktok_username, started_at)
    select $1, $2, now()
    where not exists (
      select 1
      from live_sessions
      where creator_id = $1
        and ended_at is null
    )
    `,
    [creator.creator_id, creator.tiktok_username]
  );
}

async function markNotLiveCandidate(creator_id) {
  // Increment miss_count
  const { rows } = await pool.query(
    `
    update live_now
    set
      miss_count = miss_count + 1,
      last_check_at = now(),
      updated_at = now()
    where creator_id = $1
    returning miss_count
    `,
    [creator_id]
  );

  if (!rows.length) return;

  const missCount = Number(rows[0].miss_count || 0);
  if (missCount < OFFLINE_MISS_THRESHOLD) return;

  // Close open session
  await pool.query(
    `
    update live_sessions
    set ended_at = now(),
        updated_at = now()
    where creator_id = $1
      and ended_at is null
    `,
    [creator_id]
  );

  // Remove from live_now
  await pool.query(`delete from live_now where creator_id = $1`, [creator_id]);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pollOnce() {
  let creators;
  try {
    creators = await getCreatorsToMonitor();
  } catch (err) {
    logError("DB error reading creators:", String(err && err.message ? err.message : err));
    return;
  }

  const limit = pLimit(CONCURRENCY);

  // Stagger requests slightly to reduce burst behaviour
  const perTaskDelayMs = Math.max(0, Math.floor(800 / Math.max(1, CONCURRENCY)));

  await Promise.allSettled(
    creators.map((c, idx) =>
      limit(async () => {
        if (perTaskDelayMs) await sleep(perTaskDelayMs * (idx % CONCURRENCY));

        try {
          const live = await isLive(c.tiktok_username);
          if (live) {
            await markLive(c);
          } else {
            await markNotLiveCandidate(c.creator_id);
          }
        } catch (err) {
          // Do not mark offline on errors, avoids false removals when TikTok blocks
          logError("Live check failed:", c.tiktok_username, String(err && err.message ? err.message : err));
        }
      })
    )
  );
}

async function main() {
  // Connectivity check, one line only
  try {
    await pool.query("select 1 as ok");
  } catch (err) {
    logError("Fatal: cannot connect to database:", String(err && err.message ? err.message : err));
    process.exit(1);
  }

  // Run loop
  await pollOnce();

  setInterval(() => {
    pollOnce().catch((err) => {
      logError("Poll loop error:", String(err && err.message ? err.message : err));
    });
  }, POLL_INTERVAL_SECONDS * 1000);
}

process.on("unhandledRejection", (reason) => {
  logError("Unhandled rejection:", String(reason));
});

process.on("uncaughtException", (err) => {
  logError("Uncaught exception:", String(err && err.message ? err.message : err));
  process.exit(1);
});

main().catch((err) => {
  logError("Fatal error:", String(err && err.message ? err.message : err));
  process.exit(1);
});
