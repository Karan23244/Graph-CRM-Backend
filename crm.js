const db = require("./config/db");

const START_DATE = "2025-12-12 00:00:00";

async function runNotificationJob({ dryRun = false }) {
  const result = {
    fp: [],
    fa1: [],
    fa2: [],
  };

  /** ==========================
   * FP NOT FILLED (2 DAYS)
   ========================== */
  const [fpRows] = await db.query(
    `
    SELECT 
      a.id,
      a.campaign_name,
      a.pid,
      l.id AS receiver_id
    FROM adv_data a
    JOIN login l ON l.username = a.pub_name
    WHERE a.pub_name IS NOT NULL
      AND (a.fp IS NULL OR a.fp = '')
      AND a.created_at >= ?
      AND a.created_at <= NOW() - INTERVAL 2 DAY
    `,
    [START_DATE]
  );

  for (const row of fpRows) {
    const message = `⚠️ FP not filled for campaign "${row.campaign_name}" and pid "${row.pid}" within 2 days.`;

    result.fp.push({
      adv_id: row.id,
      receiver_id: row.receiver_id,
      message,
    });

    if (!dryRun) {
      await createNotificationIfNotExists({
        receiver_id: row.receiver_id,
        type: "custom",
        message,
        url: "/dashboard/currentpubdata",
      });
    }
  }

  /** ==========================
   * FA1 NOT FILLED (7 DAYS)
   ========================== */
  const [fa1Rows] = await db.query(
    `
    SELECT id, user_id, campaign_name,pid
    FROM adv_data
    WHERE (fa1 IS NULL OR fa1 = '')
      AND created_at >= ?
      AND created_at <= NOW() - INTERVAL 7 DAY
    `,
    [START_DATE]
  );

  for (const row of fa1Rows) {
    const message = `⚠️ FA1 not filled for campaign "${row.campaign_name}" and pid "${row.pid}" within 7 days.`;

    result.fa1.push({
      adv_id: row.id,
      receiver_id: row.user_id,
      message,
    });

    if (!dryRun) {
      await createNotificationIfNotExists({
        receiver_id: row.user_id,
        type: "custom",
        message,
        url: "/dashboard/currentadvdata",
      });
    }
  }

  /** ==========================
   * FA2 NOT FILLED (14 DAYS)
   ========================== */
  const [fa2Rows] = await db.query(
    `
    SELECT id, user_id, campaign_name,pid
    FROM adv_data
    WHERE (fa IS NULL OR fa = '')
      AND created_at >= ?
      AND created_at <= NOW() - INTERVAL 14 DAY
    `,
    [START_DATE]
  );

  for (const row of fa2Rows) {
    const message = `⚠️ FA2 not filled for campaign "${row.campaign_name}" and pid "${row.pid}" within 14 days.`;

    result.fa2.push({
      adv_id: row.id,
      receiver_id: row.user_id,
      message,
    });

    if (!dryRun) {
      await createNotificationIfNotExists({
        receiver_id: row.user_id,
        type: "custom",
        message,
        url: "/dashboard/currentadvdata",
      });
    }
  }

  return {
    dryRun,
    summary: {
      fp: result.fp.length,
      fa1: result.fa1.length,
      fa2: result.fa2.length,
      total: result.fp.length + result.fa1.length + result.fa2.length,
    },
    data: result,
  };
}

async function createNotificationIfNotExists({
  receiver_id,
  type,
  message,
  url,
}) {
  const [existing] = await db.query(
    `
    SELECT id
    FROM notifications
    WHERE receiver_id = ?
      AND type = ?
      AND message = ?
    `,
    [receiver_id, type, message]
  );

  if (existing.length === 0) {
    await db.query(
      `
      INSERT INTO notifications
      (sender_id, receiver_id, type, message, url)
      VALUES (NULL, ?, ?, ?, ?)
      `,
      [receiver_id, type, message, url]
    );
  }
}
module.exports = {
  runNotificationJob,
};
