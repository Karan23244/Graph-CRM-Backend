"use strict";

/**
 * decisionEngine.service.js
 *
 * HOW THIS FILE WORKS
 * ───────────────────
 * 1. QUERIES  (decisionEngine.queries.js)
 *    - A plain object of named SQL strings: GET_CONFIG, GET_CLICK_METRICS,
 *      GET_INSTALL_METRICS, GET_EVENT_METRICS
 *    - Imported here and passed directly to db.query(SQL, [params])
 *    - No magic — just a key→SQL lookup so raw SQL stays out of this file
 *
 * 2. LOGIC    (decisionEngine.logic.js)
 *    - Pure functions only (no DB, no HTTP)
 *    - computeMetrics()   → turns raw DB numbers into percentages
 *    - checkEligibility() → checks the 3 eligibility conditions
 *    - gradeMetrics()     → assigns A/B/C/D to each metric via range lookup
 *    - evaluateStatus()   → runs fraud gate (Step 1) + matrix lookup (Step 2)
 *
 * 3. THIS FILE (service)
 *    - Calls the DB 4 times (1 config + 3 parallel metric queries)
 *    - Merges results by (pubam, pubid, pid) group key
 *    - Calls logic functions in order
 *    - Returns a shaped response array
 */

// ─── Imports ────────────────────────────────────────────────────────────────

const db = require("../config/db"); // your existing mysql2 pool
const QUERIES = require("./decisionEngine.queries"); // SQL strings object
const {
  computeMetrics,
  gradeMetrics,
  checkEligibility,
  evaluateStatus,
} = require("./decisionEngine.js"); // pure logic functions

// ─────────────────────────────────────────────────────────────────────────────
// MAIN EXPORT — this is what the controller calls
// ─────────────────────────────────────────────────────────────────────────────

/**
 * runDecisionEngine
 *
 * @param {object} params
 * @param {string} params.campaign_name  e.g. "Moneyview"
 * @param {string} params.os             e.g. "Android"
 * @param {string} params.date           "YYYY-MM-DD"  <- reference date from payload
 * @returns {Promise<object[]>}          Array of per-group decision results
 */
async function runDecisionEngine({ campaign_name, os, date }) {
  // ── STEP A: Fetch campaign config ──────────────────────────────────────────
  // QUERIES.GET_CONFIG is the SQL string from decisionEngine.queries.js
  // db.query returns [rows, fields] — we destructure to get rows only
  //
  // SQL used:
  //   SELECT clicks_per_day, installs_per_day, events,
  //          rule1_params, rule2_params, ignore_metrics
  //   FROM campaign_configs
  //   WHERE JSON_CONTAINS(campaign_name, JSON_QUOTE(?)) AND os = ?
  //
  const [configRows] = await db.query(QUERIES.GET_CONFIG, [campaign_name, os]);

  if (!configRows.length) {
    throw new Error(
      `No config found for campaign="${campaign_name}", os="${os}"`,
    );
  }

  const config = configRows[0];

  // Parse all JSON columns safely
  const rule1Params = safeParseJSON(config.rule1_params, {});
  const rule2Params = safeParseJSON(config.rule2_params, {});
  const ignoreMetrics = safeParseJSON(config.ignore_metrics, []);
  const events = safeParseJSON(config.events, []);

  // E2 = second element in the events array (index 1)
  // e.g. events = ["", "submit_success"]  =>  e2EventName = "submit_success"
  const e2EventName = events[1] && events[1].trim() ? events[1].trim() : null;
  if (!e2EventName) {
    throw new Error(
      `E2 event not set in campaign_configs.events for campaign="${campaign_name}"`,
    );
  }

  // ── STEP B: Three parallel DB queries ─────────────────────────────────────
  //
  // All three run at the same time via Promise.all — no serial waiting.
  //
  // +-----------------------+----------------------+-----------------------+
  // |  GET_CLICK_METRICS    | GET_INSTALL_METRICS  |  GET_EVENT_METRICS   |
  // |  filter: clicks_date  | filter: install_time |  filter: metrics_date|
  // |  7-day outer window   | 7-day outer window   |  7-day window        |
  // |  + 5-day sub-agg      | + 5-day sub-agg      |  (no 5d needed)      |
  // +-----------------------+----------------------+-----------------------+
  //
  // Parameter order matches what's documented in decisionEngine.queries.js:
  //
  //  GET_CLICK_METRICS   => [date, date, campaign_name, os, date, date]
  //                          ^ 5d  ^^5d                   ^ 7d  ^^7d
  //
  //  GET_INSTALL_METRICS => [date, date, campaign_name, os, date, date]
  //
  //  GET_EVENT_METRICS   => [e2EventName, e2EventName, campaign_name, os, date, date]
  //                          ^ e2_total   ^ pe_e2_total
  //
  const [[clickRows], [installRows], [eventRows]] = await Promise.all([
    db.query(QUERIES.GET_CLICK_METRICS, [
      date,
      date,
      campaign_name,
      os,
      date,
      date,
    ]),
    db.query(QUERIES.GET_INSTALL_METRICS, [
      date,
      date,
      campaign_name,
      os,
      date,
      date,
    ]),
    db.query(QUERIES.GET_EVENT_METRICS, [
      e2EventName,
      e2EventName,
      campaign_name,
      os,
      date,
      date,
    ]),
  ]);

  // ── STEP C: Index each result set by group key ────────────────────────────
  //
  // Group key = "pubam||pubid||pid"
  // This lets us do O(1) lookups when merging the three result sets.
  //
  const clickMap = indexByGroup(clickRows);
  const installMap = indexByGroup(installRows);
  const eventMap = indexByGroup(eventRows);

  // Union of all groups found in clicks OR installs result sets
  const allGroupKeys = new Set([
    ...Object.keys(clickMap),
    ...Object.keys(installMap),
  ]);

  // ── STEP D: Per-group evaluation ──────────────────────────────────────────
  const results = [];

  for (const key of allGroupKeys) {
    // Pull row for this group from each map (default to {} if group absent)
    const click = clickMap[key] || {};
    const install = installMap[key] || {};
    const event = eventMap[key] || {};

    const { pubam, pubid, pid } = parseGroupKey(key);

    // 1. Compute metric percentages
    //    computeMetrics() lives in decisionEngine.logic.js
    const metricValues = computeMetrics({
      clicks: click.total_clicks || 0,
      installs: install.total_installs || 0,
      rti: install.total_rti || 0,
      pi: install.total_pi || 0,
      e2Total: event.e2_total || 0,
      peE2Total: event.pe_e2_total || 0,
    });

    // 2. Eligibility check (uses 5-day sub-aggregates from click/install queries)
    //    checkEligibility() lives in decisionEngine.logic.js
    const eligibility = checkEligibility({
      clicks5d: click.clicks_5d || 0,
      installs5d: install.installs_5d || 0,
      sharedDate: click.shared_date || null,
      selectedDate: date,
      clicksPerDay: config.clicks_per_day,
      installsPerDay: config.installs_per_day,
    });

    // 3. Grade all four metrics (A/B/C/D via range lookup in rule1/rule2 params)
    //    gradeMetrics() lives in decisionEngine.logic.js
    const gradedMetrics = gradeMetrics(
      metricValues,
      rule1Params,
      rule2Params,
      ignoreMetrics,
    );

    // 4. Evaluate final status — only when row is eligible
    //    evaluateStatus() runs:
    //      Step 1 → fraud gate   (if fraud grade = D => force Pause)
    //      Step 2 → matrix lookup (sort grades A-D, build key, look up DECISION_MATRIX)
    const status = eligibility.eligible
      ? evaluateStatus(gradedMetrics)
      : "Not Eligible";

    results.push({
      pubam: pubam || null,
      pubid: pubid || null,
      pid: pid || null,
      eligible: eligibility.eligible,
      eligibility_flags: {
        click_cap: eligibility.clickCap,
        install_cap: eligibility.installCap,
        link_active: eligibility.linkActive,
      },
      status,
      metrics: {
        c2i: gradedMetrics.c2i,
        fraud: gradedMetrics.fraud,
        i2e2: gradedMetrics.i2e2,
        pa_e2: gradedMetrics.pa_e2,
      },
    });
  }

  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// PRIVATE HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/** Build a composite group key from a DB row */
function groupKey(row) {
  return `${row.pubam ?? ""}||${row.pubid ?? ""}||${row.pid ?? ""}`;
}

/** Convert an array of DB rows into a { groupKey => row } map */
function indexByGroup(rows) {
  return rows.reduce((map, row) => {
    map[groupKey(row)] = row;
    return map;
  }, {});
}

/** Split a composite group key back into its three fields */
function parseGroupKey(key) {
  const [pubam, pubid, pid] = key.split("||");
  return {
    pubam: pubam || null,
    pubid: pubid || null,
    pid: pid || null,
  };
}

/** JSON.parse with a safe fallback — never throws */
function safeParseJSON(raw, fallback) {
  try {
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPORT
// ─────────────────────────────────────────────────────────────────────────────

module.exports = { runDecisionEngine };
