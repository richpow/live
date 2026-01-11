const { Pool } = require("pg");
const { TikTokLiveConnection } = require("@adamjessop/tiktok-live-connector");

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("Fatal: DATABASE_URL is not set");
  process.exit(1);
}

const POLL_INTERVAL_SECONDS = 60;
const CONCURRENCY = 4;
const OFFLINE_MISS_THRESHOLD = 2;
const CREATOR_LOOKBACK_DAYS = 3;

// minimal logs only
function logError(...args) {
  console.error(...args);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
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
      and "Data period" >= (current_date - ($1::int || ' days')::interval)
      and is_demo_data is not true
  `;

  const { rows } = await pool.query(sql, [CREATOR_LOOKBACK_DAYS]);

  return rows
    .map(r => ({
      creator_id: String(r.creator_id).trim(),
      tiktok_username: String(r.tiktok_username).trim().replace(/^@/, "")
    }))
    .filter(r => r.creator_id && r.tiktok_username);
}

async function isLive(username) {
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: false
  });
  return Boolean(await conn.fetchIsLive());
}

async function markLive(c) {
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
    [c.creator_id, c.tiktok_username]
  );

  await pool.query(
    `
    insert into live_sessions (creator_id, tiktok_username, started_at)
    select $1, $2, now()
    where not exists (
      select 1 from live_sessions
      where creator_id = $1 and ended_at is null
    )
    `,
    [c.creator_id, c.tiktok_username]
  );
}

async function markNotLiveCandidate(creator_id) {
  const { rows } = await pool.query(
    `
    update live_now
    set miss_count = miss_count + 1,
        last_check_at = now(),
        updated_at = now()
    where creator_id = $1
    returning miss_count
    `,
    [creator_id]
  );

  if (!rows.length) return;
  if (rows[0].miss_count < OFFLINE_MISS_THRESHOLD) return;

  await pool.query(
    `
    update live_sessions
    set ended_at = now(),
        updated_at = now()
    where creator_id = $1 and ended_at is null
    `,
    [creator_id]
  );

  await pool.query(`delete from live_now where creator_id = $1`, [creator_id]);
}

async function runWithConcurrency(items, limit, worker) {
  let index = 0;

  async function next() {
    if (index >= items.length) return;
    const current = items[index++];
    await worker(current);
    await next();
  }

  const workers = [];
  for (let i = 0; i < limit; i++) {
    workers.push(next());
  }

  await Promise.all(workers);
}

async function pollOnce() {
  let creators;
  try {
    creators = await getCreatorsToMonitor();
  } catch (err) {
    logError("DB read error:", err.message);
    return;
  }

  await runWithConcurrency(creators, CONCURRENCY, async (c) => {
    try {
      const live = await isLive(c.tiktok_username);
      if (live) {
        await markLive(c);
      } else {
        await markNotLiveCandidate(c.creator_id);
      }
    } catch (err) {
      logError("Live check failed:", c.tiktok_username, err.message);
    }
  });
}

async function main() {
  try {
    await pool.query("select 1");
  } catch (err) {
    logError("Fatal DB connection error:", err.message);
    process.exit(1);
  }

  await pollOnce();

  setInterval(() => {
    pollOnce().catch(err => logError("Poll loop error:", err.message));
  }, POLL_INTERVAL_SECONDS * 1000);
}

process.on("unhandledRejection", err => logError("Unhandled rejection:", err));
process.on("uncaughtException", err => {
  logError("Uncaught exception:", err.message);
  process.exit(1);
});

main();
