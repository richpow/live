const { Pool } = require("pg");
const pLimit = require("p-limit");
const { TikTokLiveConnection } = require("@adamjessop/tiktok-live-connector");

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is not set in Railway variables");
}

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 45);
const OFFLINE_MISS_THRESHOLD = Number(process.env.OFFLINE_MISS_THRESHOLD || 2);
const CONCURRENCY = Number(process.env.CONCURRENCY || 6);

let dbHost = "unknown";
try {
  dbHost = new URL(DATABASE_URL).hostname;
} catch {
  throw new Error("DATABASE_URL is not a valid connection string");
}

console.log("FastTrack live poller starting");
console.log("DB host:", dbHost);
console.log("Poll interval seconds:", POLL_INTERVAL_SECONDS);
console.log("Concurrency:", CONCURRENCY);
console.log("Offline miss threshold:", OFFLINE_MISS_THRESHOLD);

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function getCreatorsToMonitor() {
  const sql = `
    select distinct
      creator_id,
      "Creator's username" as tiktok_username
    from fasttrack_daily
    where "Creator's username" is not null
      and "Creator's username" <> ''
      and creator_id is not null
      and creator_id <> ''
      and "Data period" >= (current_date - interval '3 days')
      and is_demo_data is not true
  `;

  const { rows } = await pool.query(sql);

  return rows.map(r => ({
    creator_id: String(r.creator_id).trim(),
    tiktok_username: String(r.tiktok_username || "").trim().replace(/^@/, "")
  })).filter(r => r.creator_id && r.tiktok_username);
}

async function isLive(username) {
  const conn = new TikTokLiveConnection(username);
  const result = await conn.fetchIsLive();
  return Boolean(result);
}

async function markLive(creator) {
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

  await pool.query(
    `
    insert into live_sessions (creator_id, tiktok_username, started_at)
    select $1, $2, now()
    where not exists (
      select 1 from live_sessions
      where creator_id = $1
        and ended_at is null
    )
    `,
    [creator.creator_id, creator.tiktok_username]
  );
}

async function markOffline(creator_id) {
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

  await pool.query(
    `delete from live_now where creator_id = $1`,
    [creator_id]
  );
}

async function pollOnce() {
  const creators = await getCreatorsToMonitor();
  console.log("Creators to monitor:", creators.length);

  const limit = pLimit(CONCURRENCY);

  const results = await Promise.allSettled(
    creators.map(c =>
      limit(async () => {
        try {
          const live = await isLive(c.tiktok_username);
          if (live) {
            await markLive(c);
          } else {
            await markOffline(c.creator_id);
          }
        } catch (err) {
          console.error("Live check failed", c.tiktok_username, String(err && err.message ? err.message : err));
        }
      })
    )
  );

  const rejected = results.filter(r => r.status === "rejected").length;
  if (rejected) console.log("Rejected tasks:", rejected);
}

async function main() {
  try {
    await pool.query("select 1 as ok");
    console.log("Database connectivity ok");
  } catch (err) {
    console.error("Database connectivity failed", String(err && err.message ? err.message : err));
    throw err;
  }

  await pollOnce();

  setInterval(() => {
    pollOnce().catch(e => {
      console.error("Poll loop error", String(e && e.message ? e.message : e));
    });
  }, POLL_INTERVAL_SECONDS * 1000);
}

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection", String(reason));
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception", String(err && err.message ? err.message : err));
  process.exit(1);
});

main().catch(err => {
  console.error("Fatal error", String(err && err.message ? err.message : err));
  process.exit(1);
});
