import { Pool } from "pg";
import { TikTokLiveConnection } from "@adamjessop/tiktok-live-connector";

/* ================= ENV ================= */

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("DATABASE_URL missing");
  process.exit(1);
}

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 60);
const CONCURRENCY = Number(process.env.CONCURRENCY || 3);
const OFFLINE_MISS_THRESHOLD = Number(process.env.OFFLINE_MISS_THRESHOLD || 2);
const CREATOR_LOOKBACK_DAYS = Number(process.env.CREATOR_LOOKBACK_DAYS || 3);
const RETRY_DELAY_MS = Number(process.env.RETRY_DELAY_MS || 15000);

/* ================= DB ================= */

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

/* ================= HELPERS ================= */

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function runWithConcurrency(items, limit, fn) {
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const item = items[index++];
      await fn(item);
    }
  }

  const workers = [];
  for (let i = 0; i < limit; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);
}

/* ================= DATA ================= */

async function getCreators() {
  const { rows } = await pool.query(
    `
    select distinct
      creator_id,
      "Creator's username" as username
    from fasttrack_daily
    where "Creator's username" is not null
      and "Creator's username" <> ''
      and creator_id is not null
      and creator_id <> ''
      and "Data period" >= (current_date - ($1::int || ' days')::interval)
      and is_demo_data is not true
    `,
    [CREATOR_LOOKBACK_DAYS]
  );

  return rows.map(r => ({
    creator_id: String(r.creator_id).trim(),
    username: String(r.username).replace(/^@/, "").trim()
  }));
}

/* ================= TIKTOK ================= */

async function checkIsLive(username) {
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: false
  });
  return Boolean(await conn.fetchIsLive());
}

/* ================= STATE ================= */

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
    [c.creator_id, c.username]
  );

  await pool.query(
    `
    insert into live_sessions (creator_id, tiktok_username, started_at)
    select $1,$2,now()
    where not exists (
      select 1 from live_sessions
      where creator_id = $1
        and ended_at is null
    )
    `,
    [c.creator_id, c.username]
  );
}

async function markMiss(c) {
  const { rows } = await pool.query(
    `
    update live_now
    set miss_count = miss_count + 1,
        last_check_at = now(),
        updated_at = now()
    where creator_id = $1
    returning miss_count
    `,
    [c.creator_id]
  );

  if (!rows.length) return;
  if (rows[0].miss_count < OFFLINE_MISS_THRESHOLD) return;

  await pool.query(
    `
    update live_sessions
    set ended_at = now(),
        updated_at = now()
    where creator_id = $1
      and ended_at is null
    `,
    [c.creator_id]
  );

  await pool.query(
    `delete from live_now where creator_id = $1`,
    [c.creator_id]
  );
}

/* ================= CORE LOGIC ================= */

async function checkCreator(c) {
  try {
    if (await checkIsLive(c.username)) {
      await markLive(c);
      return;
    }
  } catch {}

  await sleep(RETRY_DELAY_MS);

  try {
    if (await checkIsLive(c.username)) {
      await markLive(c);
      return;
    }
  } catch {}

  await markMiss(c);
}

async function pollOnce() {
  let creators;
  try {
    creators = await getCreators();
  } catch {
    return;
  }

  shuffle(creators);

  await runWithConcurrency(creators, CONCURRENCY, checkCreator);
}

/* ================= BOOT ================= */

async function main() {
  try {
    await pool.query("select 1");
  } catch {
    console.error("DB unavailable");
    process.exit(1);
  }

  await pollOnce();

  setInterval(() => {
    pollOnce().catch(() => {});
  }, POLL_INTERVAL_SECONDS * 1000);
}

main();
