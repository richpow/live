const { Pool } = require("pg");
const pLimit = require("p-limit");
const { TikTokLiveConnection } = require("@adamjessop/tiktok-live-connector");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 45);
const OFFLINE_MISS_THRESHOLD = Number(process.env.OFFLINE_MISS_THRESHOLD || 2);
const CONCURRENCY = Number(process.env.CONCURRENCY || 6);

async function getCreators() {
  const { rows } = await pool.query(`
    select
      creator_id,
      tiktok_username,
      coalesce(display_name, username) as display_name,
      profile_image_url
    from users
    where tiktok_username is not null
      and tiktok_username <> ''
      and creator_id is not null
  `);
  return rows;
}

async function isLive(username) {
  const conn = new TikTokLiveConnection(username);
  return Boolean(await conn.fetchIsLive());
}

async function markLive(c) {
  await pool.query(
    `
    insert into live_now (
      creator_id,
      tiktok_username,
      display_name,
      profile_image_url,
      went_live_at,
      last_seen_live_at,
      last_check_at,
      miss_count,
      updated_at
    )
    values ($1,$2,$3,$4,now(),now(),now(),0,now())
    on conflict (creator_id) do update set
      last_seen_live_at = now(),
      last_check_at = now(),
      miss_count = 0,
      updated_at = now()
    `,
    [c.creator_id, c.tiktok_username, c.display_name, c.profile_image_url]
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

async function markOffline(creator_id) {
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
    set ended_at = now(), updated_at = now()
    where creator_id = $1 and ended_at is null
    `,
    [creator_id]
  );

  await pool.query(`delete from live_now where creator_id = $1`, [creator_id]);
}

async function poll() {
  const creators = await getCreators();
  const limit = pLimit(CONCURRENCY);

  await Promise.allSettled(
    creators.map(c =>
      limit(async () => {
        try {
          const live = await isLive(c.tiktok_username);
          if (live) await markLive(c);
          else await markOffline(c.creator_id);
        } catch {}
      })
    )
  );
}

setInterval(poll, POLL_INTERVAL_SECONDS * 1000);
poll();
