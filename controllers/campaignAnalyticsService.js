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
  cfg.config_type = cfg.config_type || "appsflyer";
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
      cm.campaign_id,
      cm.pubam,
      cm.pubid,
      cm.pid,
     MAX(cm.is_paused) AS is_paused,
SUM(
  CASE
    WHEN DATE(cm.metrics_date) BETWEEN ? AND ?
    THEN cm.impressions
    ELSE 0
  END
) AS total_impressions,
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


    GROUP BY cm.pubam, cm.pubid, cm.pid, cm.os,cm.campaign_id
  `;

  const params = [
    // Impressions date range
    mtd.start,
    mtd.end,
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
  ];

  const [rows] = await db.execute(sql, params);

  return rows;
}
// ─────────────────────────────────────────────
// 4. Fetch event metrics for all PIDs in ONE query
//    Returns map: { pid → { windowLabel → { eventName → { noe, pe } } } }
// ─────────────────────────────────────────────
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

      if (type === "noe" || type === "event") {
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
  const filteredRows = metricsRows.filter((row) => {
    const pidKey = `${row.campaign_id}_${row.pid}_${os}`;

    const hasMetricData =
      Number(row.mtd_clicks || 0) > 0 ||
      Number(row.mtd_installs || 0) > 0 ||
      Number(row.mtd_rti || 0) > 0 ||
      Number(row.mtd_pi || 0) > 0 ||
      Number(row.primary_clicks || 0) > 0 ||
      Number(row.primary_installs || 0) > 0 ||
      Number(row.primary_rti || 0) > 0 ||
      Number(row.primary_pi || 0) > 0 ||
      Number(row.secondary_clicks || 0) > 0 ||
      Number(row.secondary_installs || 0) > 0 ||
      Number(row.secondary_rti || 0) > 0 ||
      Number(row.secondary_pi || 0) > 0;

    const hasEventData =
      eventMap_db[pidKey] &&
      Object.values(eventMap_db[pidKey]).some((event) =>
        ["mtd", "primary", "secondary"].some(
          (win) => (event[win]?.noe || 0) > 0 || (event[win]?.pe || 0) > 0,
        ),
      );

    if (Number(row.is_paused) === 0) {
      return true;
    }

    return hasMetricData || hasEventData;
  });
  const result = filteredRows.map((row) => {
    const pid = row.pid;
    const pidKey = `${row.campaign_id}_${row.pid}_${os}`;

    // Build aggregation objects per window
    const buildAgg = (prefix) => ({
      clicks: row[`${prefix}_clicks`] || 0,
      installs: row[`${prefix}_installs`] || 0,
      rti: row[`${prefix}_rti`] || 0,
      pi: row[`${prefix}_pi`] || 0,
      events: buildEventAgg(eventMap, eventMap_db[pidKey], prefix),
    });

    const aggMtd = buildAgg("mtd");
    const aggPrimary = buildAgg("primary");
    const aggSecondary = buildAgg("secondary");

    // KPI computation
    const kpiMtd = computeKPIs(aggMtd, eventKeys, config.config_type);
    const kpiPrimary = computeKPIs(aggPrimary, eventKeys, config.config_type);
    const kpiSecondary = computeKPIs(
      aggSecondary,
      eventKeys,
      config.config_type,
    );

    // Color application
    const coloredMtd = applyColors(
      kpiMtd,
      rule1,
      rule2,
      eventKeys,
      pid,
      config.config_type,
    );

    const coloredPrimary = applyColors(
      kpiPrimary,
      rule1,
      rule2,
      eventKeys,
      pid,
      config.config_type,
    );

    const coloredSecondary = applyColors(
      kpiSecondary,
      rule1,
      rule2,
      eventKeys,
      pid,
      config.config_type,
    );

    // PID classification
    const pidColor = classifyPID(
      {
        pubam: row.pubam,
        pubid: row.pubid,
        is_paused: row.is_paused,
        mtd_clicks: row.mtd_clicks || 0,
        mtd_installs: row.mtd_installs || 0,
      },
      clicks_per_day,
      installs_per_day,
      windows.mtd.days,
    );

    return flattenRow({
      pid,
      pubid: row.pubid,
      pubam: row.pubam,
      pid_color: pidColor,
      total_impressions: row.total_impressions || 0,
      mtd: coloredMtd,
      primary: coloredPrimary,
      secondary: coloredSecondary,
      eventKeys,
      primaryLabel: windows.primary.label,
      secondaryLabel: windows.secondary.label,
    });
  });
  // ── PID Summary (MTD) ─────────────────────────────
  const pidSummary = {
    total_pids: 0,
    active_pids: 0,
    paused_pids: 0,
    na_pids: 0,
  };

  const uniquePids = new Set();
  const activePids = new Set();
  const pausedPids = new Set();
  const naPids = new Set();

  filteredRows.forEach((row) => {
    const pid = (row.pid || "").toString().trim();

    // unique key so same pid in different campaign/pub doesn't duplicate
    const pidKey = `${row.campaign_id}_${pid}_${row.pubid}`;

    uniquePids.add(pidKey);

    // NA PID
    if (!pid || pid.toUpperCase() === "N/A" || pid.toUpperCase() === "NA") {
      naPids.add(pidKey);
    }

    // paused pid
    if (Number(row.is_paused) === 1) {
      pausedPids.add(pidKey);
    } else {
      activePids.add(pidKey);
    }
  });

  pidSummary.total_pids = uniquePids.size;
  pidSummary.active_pids = activePids.size;
  pidSummary.paused_pids = pausedPids.size;
  pidSummary.na_pids = naPids.size;
  return {
    data: result,
    meta: {
      pid_summary: pidSummary, // <-- add this
      windows: {
        mtd: {
          start: windows.mtd.start,
          end: windows.mtd.end,
        },
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

function flattenRow({
  pid,
  pubid,
  pubam,
  pid_color,
  total_impressions,
  mtd,
  primary,
  secondary,
  eventKeys,
  primaryLabel,
  secondaryLabel,
}) {
  const row = { pid, pubid, pubam, pid_color };
  row.impressions = Number(total_impressions) || 0;
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
