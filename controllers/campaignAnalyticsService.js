"use strict";

const db = require("../config/db");
const { buildDateWindows } = require("../utils/dateUtils");
const { computeKPIs } = require("../utils/kpiUtils");
const { applyColors } = require("../utils/colorUtils");
const { classifyPID } = require("../utils/pidUtils");

// ─────────────────────────────────────────────
// 1. Fetch campaign config
// ─────────────────────────────────────────────
async function fetchCampaignConfig(campaignName, os) {
  const [rows] = await db.execute(
    `SELECT *
     FROM campaign_configs
     WHERE LOWER(JSON_UNQUOTE(JSON_EXTRACT(campaign_name, '$[0]'))) = LOWER(?)
       AND LOWER(os) = LOWER(?)
     LIMIT 1`,
    [campaignName, os],
  );

  if (!rows.length) return null;

  const cfg = rows[0];

  // Parse JSON fields safely
  const parse = (field) => {
    try {
      return JSON.parse(field);
    } catch {
      return null;
    }
  };

  cfg.events_arr = parse(cfg.events) || [];
  cfg.rule1 = parse(cfg.rule1_params) || {};
  cfg.rule2 = parse(cfg.rule2_params) || {};
  cfg.ignore_arr = parse(cfg.ignore_metrics) || [];

  return cfg;
}

// ─────────────────────────────────────────────
// 2. Build event key map  e.g. ['submit_success',''] → { E1: 'submit_success' }
// ─────────────────────────────────────────────
function buildEventMap(eventsArr) {
  const map = {};

  eventsArr.forEach((ev, index) => {
    if (ev && ev.trim()) {
      map[`E${index + 1}`] = ev.trim();
    }
  });

  return map;
}

// ─────────────────────────────────────────────
// 3. Fetch aggregated campaign_metrics for all windows in ONE query
//    Uses CASE WHEN to pivot MTD / 7D / 3D in a single pass
// ─────────────────────────────────────────────
// async function fetchMetricsAllWindows(campaignName, os, windows) {
//   const { mtd, primary, secondary } = windows;

//   /*
//    * Date filter uses COALESCE(install_time, event_time, clicks_date)
//    * We cast it to DATE for comparison.
//    */
//   const sql = `
//     SELECT
//       cm.pubam,
//       cm.pubid,
//       cm.pid,
//       MAX(cm.is_paused) AS is_paused,

//       /* MTD aggregates */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.clicks   ELSE 0 END) AS mtd_clicks,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.noi      ELSE 0 END) AS mtd_installs,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.rti      ELSE 0 END) AS mtd_rti,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.pi       ELSE 0 END) AS mtd_pi,

//       /* Primary window (7D) */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.clicks   ELSE 0 END) AS primary_clicks,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.noi      ELSE 0 END) AS primary_installs,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.rti      ELSE 0 END) AS primary_rti,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.pi       ELSE 0 END) AS primary_pi,

//       /* Secondary window (3D) */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.clicks   ELSE 0 END) AS secondary_clicks,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.noi      ELSE 0 END) AS secondary_installs,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.rti      ELSE 0 END) AS secondary_rti,
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cm.pi       ELSE 0 END) AS secondary_pi

//     FROM campaign_metrics_new cm
//     WHERE LOWER(cm.campaign_name) = LOWER(?)
//       AND LOWER(cm.os) = LOWER(?)
//       AND DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date)) BETWEEN ? AND ?

// GROUP BY cm.pubam, cm.pubid, cm.pid, cm.os  `;

//   const params = [
//     // MTD (×4 columns)
//     mtd.start,
//     mtd.end,
//     mtd.start,
//     mtd.end,
//     mtd.start,
//     mtd.end,
//     mtd.start,
//     mtd.end,
//     // Primary (×4)
//     primary.start,
//     primary.end,
//     primary.start,
//     primary.end,
//     primary.start,
//     primary.end,
//     primary.start,
//     primary.end,
//     // Secondary (×4)
//     secondary.start,
//     secondary.end,
//     secondary.start,
//     secondary.end,
//     secondary.start,
//     secondary.end,
//     secondary.start,
//     secondary.end,
//     // WHERE clause
//     campaignName,
//     os,
//     mtd.start,
//     mtd.end, // outer range = MTD (widest window)
//   ];

//   const [rows] = await db.execute(sql, params);
//   return rows;
// }
async function fetchMetricsAllWindows(
  campaignName,
  os,
  geo,
  campaign_ids,
  windows,
) {
  const { mtd, primary, secondary } = windows;

  const campaignPlaceholders =
    campaign_ids.length > 0 ? campaign_ids.map(() => "?").join(",") : "NULL";

  const geoCondition =
    geo.length > 0
      ? geo.map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`).join(" OR ")
      : null;

  const sql = `
    SELECT
      GROUP_CONCAT(DISTINCT cm.campaign_id) AS campaign_ids,
      cm.pubam,
      cm.pubid,
      cm.pid,
      MAX(cm.is_paused) AS is_paused,

      /* MTD */
      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.clicks ELSE 0 END) AS mtd_clicks,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.noi ELSE 0 END) AS mtd_installs,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.rti ELSE 0 END) AS mtd_rti,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.pi ELSE 0 END) AS mtd_pi,

      /* PRIMARY */
      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.clicks ELSE 0 END) AS primary_clicks,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.noi ELSE 0 END) AS primary_installs,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.rti ELSE 0 END) AS primary_rti,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.pi ELSE 0 END) AS primary_pi,

      /* SECONDARY */
      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.clicks ELSE 0 END) AS secondary_clicks,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.noi ELSE 0 END) AS secondary_installs,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.rti ELSE 0 END) AS secondary_rti,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cm.pi ELSE 0 END) AS secondary_pi

    FROM campaign_metrics_new cm

    WHERE LOWER(cm.campaign_name) = LOWER(?)
      AND LOWER(cm.os) = LOWER(?)
      AND (
        cm.campaign_id IN (${campaignPlaceholders})
        OR (
          (
            cm.campaign_id IS NULL
            OR cm.campaign_id = ''
            OR cm.campaign_id = 'N/A'
          )
          AND (${geoCondition})
        )
      )

      AND DATE(COALESCE(
        cm.install_time,
        cm.event_time,
        cm.clicks_date,
        cm.metrics_date
      )) BETWEEN ? AND ?

    GROUP BY cm.pubam, cm.pubid, cm.pid, cm.os
  `;

  const params = [
    // MTD
    mtd.start,
    mtd.end,
    mtd.start,
    mtd.end,
    mtd.start,
    mtd.end,
    mtd.start,
    mtd.end,

    // PRIMARY
    primary.start,
    primary.end,
    primary.start,
    primary.end,
    primary.start,
    primary.end,
    primary.start,
    primary.end,

    // SECONDARY
    secondary.start,
    secondary.end,
    secondary.start,
    secondary.end,
    secondary.start,
    secondary.end,
    secondary.start,
    secondary.end,

    // WHERE
    campaignName,
    os,

    ...campaign_ids,
    ...geo,

    mtd.start,
    mtd.end,
  ];

  const [rows] = await db.execute(sql, params);

  return rows;
}
// ─────────────────────────────────────────────
// 4. Fetch event metrics for all PIDs in ONE query
//    Returns map: { pid → { windowLabel → { eventName → { noe, pe } } } }
// ─────────────────────────────────────────────
// async function fetchEventMetricsAllWindows(
//   campaignName,
//   os,
//   windows,
//   eventNames,
// ) {
//   if (!eventNames.length) return {};

//   const { mtd, primary, secondary } = windows;

//   // Build placeholders for event names
//   const evPlaceholders = eventNames.map(() => "?").join(",");

//   const sql = `
//     SELECT
//       cm.pid,
//       cem.event_name,
//       cem.event_type,

//       /* MTD */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS mtd_count,

//       /* Primary */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS primary_count,

//       /* Secondary */
//       SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date))
//                     BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS secondary_count

//     FROM campaign_event_metrics_new cem
//     INNER JOIN campaign_metrics_new cm ON cm.id = cem.campaign_metrics_id
// WHERE LOWER(cm.campaign_name) = LOWER(?)
//   AND LOWER(cm.os) = LOWER(?)
//   AND cem.event_name IN (${evPlaceholders})
//   AND DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date,cm.metrics_date)) BETWEEN ? AND ?

//    GROUP BY cm.pid, cm.os, cem.event_name, cem.event_type
//   `;

//   const params = [
//     mtd.start,
//     mtd.end,
//     primary.start,
//     primary.end,
//     secondary.start,
//     secondary.end,
//     campaignName,
//     os,
//     ...eventNames,
//     mtd.start,
//     mtd.end,
//   ];

//   const [rows] = await db.execute(sql, params);

//   /*
//    * Build nested map:
//    * {
//    *   "PID123": {
//    *     "submit_success": { mtd_noe: 0, mtd_pe: 400, primary_noe: 0, ... },
//    *     ...
//    *   }
//    * }
//    */
//   const map = {};
//   for (const row of rows) {
//     const pidKey = `${row.pid}_${os}`;
//     const pid = row.pid;
//     const ev = row.event_name;
//     const type = row.event_type; // 'noe' or 'pe'

//     if (!map[pidKey]) map[pidKey] = {};
//     if (!map[pidKey][ev]) map[pidKey][ev] = {};

//     for (const win of ["mtd", "primary", "secondary"]) {
//       if (!map[pidKey][ev][win]) map[pidKey][ev][win] = { noe: 0, pe: 0 };
//       if (type === "noe")
//         map[pidKey][ev][win].noe += parseInt(row[`${win}_count`]) || 0;
//       if (type === "pe")
//         map[pidKey][ev][win].pe += parseInt(row[`${win}_count`]) || 0;
//     }
//   }

//   return map;
// }
async function fetchEventMetricsAllWindows(
  campaignName,
  os,
  geo,
  campaign_ids,
  windows,
  eventNames,
) {
  if (!eventNames.length) return {};

  const { mtd, primary, secondary } = windows;

  const evPlaceholders = eventNames.map(() => "?").join(",");
  const campaignPlaceholders =
    campaign_ids.length > 0 ? campaign_ids.map(() => "?").join(",") : "NULL";

  const geoCondition =
    geo.length > 0
      ? geo.map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`).join(" OR ")
      : null;

  const sql = `
    SELECT
     cm.campaign_id,
      cm.pid,
      cem.event_name,
      cem.event_type,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS mtd_count,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS primary_count,

      SUM(CASE WHEN DATE(COALESCE(cm.install_time, cm.event_time, cm.clicks_date, cm.metrics_date))
          BETWEEN ? AND ? THEN cem.count ELSE 0 END) AS secondary_count

    FROM campaign_event_metrics_new cem
    INNER JOIN campaign_metrics_new cm
      ON cm.id = cem.campaign_metrics_id

    WHERE LOWER(cm.campaign_name) = LOWER(?)
      AND LOWER(cm.os) = LOWER(?)

    AND (
      cm.campaign_id IN (${campaignPlaceholders})
      OR (
        (
          cm.campaign_id IS NULL
          OR cm.campaign_id = ''
          OR cm.campaign_id = 'N/A'
        )
        AND (${geoCondition})
      )
    )

      AND cem.event_name IN (${evPlaceholders})

      AND DATE(COALESCE(
        cm.install_time,
        cm.event_time,
        cm.clicks_date,
        cm.metrics_date
      )) BETWEEN ? AND ?

    GROUP BY cm.campaign_id, cm.pid, cm.os, cem.event_name, cem.event_type
  `;

  const params = [
    mtd.start,
    mtd.end,
    primary.start,
    primary.end,
    secondary.start,
    secondary.end,

    campaignName,
    os,

    ...campaign_ids,
    ...geo,

    ...eventNames,

    mtd.start,
    mtd.end,
  ];

  const [rows] = await db.execute(sql, params);

  const map = {};

  for (const row of rows) {
    const pidKey = `${row.campaign_id}_${row.pid}_${os}`;
    const ev = row.event_name;
    const type = row.event_type;

    if (!map[pidKey]) {
      map[pidKey] = {};
    }

    if (!map[pidKey][ev]) {
      map[pidKey][ev] = {};
    }

    for (const win of ["mtd", "primary", "secondary"]) {
      if (!map[pidKey][ev][win]) {
        map[pidKey][ev][win] = { noe: 0, pe: 0 };
      }

      if (type === "noe") {
        map[pidKey][ev][win].noe += parseInt(row[`${win}_count`]) || 0;
      }

      if (type === "pe") {
        map[pidKey][ev][win].pe += parseInt(row[`${win}_count`]) || 0;
      }
    }
  }

  return map;
}
// ─────────────────────────────────────────────
// 5. Main orchestrator
// ─────────────────────────────────────────────
async function getCampaignAnalytics(payload) {
  const {
    campaign_name,
    campaign_ids = [],
    geo = [],
    os,
    start_date,
    end_date,
    windows: rawWindows = {},
  } = payload;
  console.log("Received payload:", payload);
  const primaryDays = parseInt(rawWindows.primary) || 7;
  const secondaryDays = parseInt(rawWindows.secondary) || 3;
  // ── Step 1: Config ───────────────────────────────────────────────────────
  const config = await fetchCampaignConfig(campaign_name, os);
  if (!config) {
    return { error: "Campaign config not found", data: [] };
  }

  const { rule1, rule2, events_arr, clicks_per_day, installs_per_day } = config;
  const eventMap = buildEventMap(events_arr); // { E1: 'submit_success' }
  const eventKeys = Object.keys(eventMap); // ['E1', 'E2']
  const eventNames = Object.values(eventMap); // ['submit_success', 'kyc_complete']

  // ── Step 2: Date windows ─────────────────────────────────────────────────
  const windows = buildDateWindows(
    start_date,
    end_date,
    primaryDays,
    secondaryDays,
  );

  // ── Step 3 & 4: DB queries (parallel) ───────────────────────────────────
  const [metricsRows, eventMap_db] = await Promise.all([
    fetchMetricsAllWindows(campaign_name, os, geo, campaign_ids, windows),
    fetchEventMetricsAllWindows(
      campaign_name,
      os,
      geo,
      campaign_ids,
      windows,
      eventNames,
    ),
  ]);

  if (!metricsRows.length) {
    return { data: [], windows, eventKeys };
  }

  // ── Step 5 → 8: Per-PID assembly ────────────────────────────────────────
  const result = metricsRows.map((row) => {
    const pid = row.pid;
    const pidKey = `${row.campaign_id}_${row.pid}_${os}`;
    // Build aggregation objects per window
    const buildAgg = (prefix) => ({
      clicks: row[`${prefix}_clicks`],
      installs: row[`${prefix}_installs`],
      rti: row[`${prefix}_rti`],
      pi: row[`${prefix}_pi`],
      events: buildEventAgg(eventMap, eventMap_db[pidKey], prefix),
    });

    const aggMtd = buildAgg("mtd");
    const aggPrimary = buildAgg("primary");
    const aggSecondary = buildAgg("secondary");

    // KPI computation
    const kpiMtd = computeKPIs(aggMtd, eventKeys);
    const kpiPrimary = computeKPIs(aggPrimary, eventKeys);
    const kpiSecondary = computeKPIs(aggSecondary, eventKeys);

    // Color application
    const coloredMtd = applyColors(kpiMtd, rule1, rule2, eventKeys);
    const coloredPrimary = applyColors(kpiPrimary, rule1, rule2, eventKeys);
    const coloredSecondary = applyColors(kpiSecondary, rule1, rule2, eventKeys);
    // PID classification (based on MTD traffic)
    const pidColor = classifyPID(
      {
        pubam: row.pubam,
        pubid: row.pubid,
        is_paused: row.is_paused,
        mtd_clicks: row.mtd_clicks,
        mtd_installs: row.mtd_installs,
      },
      clicks_per_day,
      installs_per_day,
      windows.mtd.days,
    );

    // Flatten into frontend-ready row
    return flattenRow({
      pid,
      pubid: row.pubid,
      pubam: row.pubam,
      pid_color: pidColor,
      mtd: coloredMtd,
      primary: coloredPrimary,
      secondary: coloredSecondary,
      eventKeys,
      primaryLabel: windows.primary.label,
      secondaryLabel: windows.secondary.label,
    });
  });

  return {
    data: result,
    meta: {
      windows: {
        mtd: { start: windows.mtd.start, end: windows.mtd.end },
        primary: {
          start: windows.primary.start,
          end: windows.primary.end,
          days: primaryDays,
        },
        secondary: {
          start: windows.secondary.start,
          end: windows.secondary.end,
          days: secondaryDays,
        },
      },
      events: eventMap,
    },
  };
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

/**
 * Build per-window event aggregation for one PID.
 * eventMap: { E1: 'submit_success' }
 * pidEvents: from DB map — { submit_success: { mtd: {noe,pe}, primary: {..}, secondary: {..} } }
 */
function buildEventAgg(eventMap, pidEvents = {}, windowPrefix) {
  const result = {};
  for (const [eKey, evName] of Object.entries(eventMap)) {
    const winData = pidEvents?.[evName]?.[windowPrefix] || { noe: 0, pe: 0 };
    result[eKey] = { noe: winData.noe, pe: winData.pe };
  }
  return result;
}

/**
 * Flatten colored KPI objects into a single table row with suffixed keys.
 * e.g. { c2i: { value: 12, color: 'green' } }
 *   → c2i_mtd_value: 12, c2i_mtd_color: 'green'
 *
 * Raw counts (clicks, installs) are stored directly as clicks_mtd etc.
 */
// function flattenRow({
//   pid, pubid, pubam, pid_color,
//   mtd, primary, secondary,
//   eventKeys,
//   primaryLabel, secondaryLabel,
// }) {
//   const row = { pid, pubid, pubam, pid_color };

//   const attach = (data, suffix) => {
//     for (const [key, val] of Object.entries(data)) {
//       if (typeof val === 'object' && val !== null && 'value' in val) {
//         row[`${key}_${suffix}`] = val.value;
//         row[`${key}_${suffix}_color`] = val.color;
//       } else {
//         row[`${key}_${suffix}`] = val;
//       }
//     }
//   };

//   attach(mtd,       'mtd');
//   attach(primary,   primaryLabel);
//   attach(secondary, secondaryLabel);

//   return row;
// }

function flattenRow({
  pid,
  pubid,
  pubam,
  pid_color,
  mtd,
  primary,
  secondary,
  eventKeys,
  primaryLabel,
  secondaryLabel,
}) {
  const row = { pid, pubid, pubam, pid_color };

  // safely extract numeric value
  const getValue = (val) => {
    if (typeof val === "object" && val !== null && "value" in val) {
      return Number(val.value) || 0;
    }
    return Number(val) || 0;
  };

  const formatMetric = (count, total) => {
    const percentage = total > 0 ? ((count / total) * 100).toFixed(2) : "0.00";

    return `${count} (${percentage}%)`;
  };

  // const attach = (data, suffix) => {
  //   const installs = getValue(data.installs);
  //   const clicks = getValue(data.clicks);

  //   // IMPORTANT:
  //   // rt_install & pa_install values already store percentages
  //   // so we only need count + existing %

  //   const rtiPercent = getValue(data.rt_install);
  //   const paPercent = getValue(data.pa_install);
  //   const fraudPercent = getValue(data.install_fraud);

  //   const rtiCount = Math.round((rtiPercent * installs) / 100);
  //   const paCount = Math.round((paPercent * installs) / 100);

  //   row[`rt_install_${suffix}`] = `${rtiCount} (${rtiPercent}%)`;

  //   row[`pa_install_${suffix}`] = `${paCount} (${paPercent}%)`;

  //   const installPercent =
  //     clicks > 0 ? ((installs / clicks) * 100).toFixed(2) : "0.00";

  //   row[`total_install_${suffix}`] = `${installs} (${installPercent}%)`;

  //   // existing flatten logic
  //   for (const [key, val] of Object.entries(data)) {
  //     if (typeof val === "object" && val !== null && "value" in val) {
  //       row[`${key}_${suffix}`] = val.value;
  //       row[`${key}_${suffix}_color`] = val.color;
  //     } else {
  //       row[`${key}_${suffix}`] = val;
  //     }
  //   }
  // };
  const attach = (data, suffix) => {
    const installs = getValue(data.installs);
    const clicks = getValue(data.clicks);

    const rtiPercent = getValue(data.rt_install);
    const paPercent = getValue(data.pa_install);
    const fraudPercent = getValue(data.install_fraud);

    const rtiCount = getValue(data.rti);
    const paCount = getValue(data.pi);
    const fraudCount = rtiCount + paCount;

    row[`rt_install_${suffix}`] = `${rtiCount} (${rtiPercent}%)`;

    row[`rt_install_${suffix}_color`] = data.rt_install?.color || "green";

    row[`pa_install_${suffix}`] = `${paCount} (${paPercent}%)`;

    row[`pa_install_${suffix}_color`] = data.pa_install?.color || "green";

    row[`install_fraud_${suffix}`] = `${fraudCount} (${fraudPercent}%)`;

    row[`install_fraud_${suffix}_color`] = data.install_fraud?.color || "green";

    const installPercent =
      clicks > 0 ? ((installs / clicks) * 100).toFixed(2) : "0.00";

    row[`total_install_${suffix}`] = `${installs} (${installPercent}%)`;

    // existing flatten logic
    for (const [key, val] of Object.entries(data)) {
      // skip already formatted fields
      if (
        key === "rt_install" ||
        key === "pa_install" ||
        key === "install_fraud"
      ) {
        continue;
      }
      // format PAE fields (show count + percentage)
      if (key.startsWith("pae_")) {
        const eKey = key.replace("pae_", "");

        // pae value may be object after applyColors
        const peCount = getValue(val);

        // E1/E2 count
        const totalEventCount = getValue(data[`${eKey}_count`]);

        // percentage
        const pePercent =
          totalEventCount > 0
            ? ((peCount / totalEventCount) * 100).toFixed(2)
            : "0.00";

        row[`${key}_${suffix}`] = `${peCount} (${pePercent}%)`;

        // use pae color rule
        row[`${key}_${suffix}_color`] = val?.color || "green";

        continue;
      }
      if (typeof val === "object" && val !== null && "value" in val) {
        row[`${key}_${suffix}`] = val.value;
        row[`${key}_${suffix}_color`] = val.color;
      } else {
        row[`${key}_${suffix}`] = val;
      }
    }
  };

  attach(mtd, "mtd");
  attach(primary, primaryLabel);
  attach(secondary, secondaryLabel);

  return row;
}

module.exports = { getCampaignAnalytics };
