// /**
//  * decisionEngine.queries.js
//  *
//  * All SQL is aggregated at DB level.
//  * Date windows differ per metric type — see inline comments.
//  *
//  * Parameter binding order is documented above each query.
//  */

// 'use strict';

// const QUERIES = {

//   /**
//    * Fetch campaign config by name + OS.
//    * campaign_name is stored as a JSON array, e.g. ["Moneyview"].
//    *
//    * Params: [campaign_name, os]
//    */
//   GET_CONFIG: `
//     SELECT
//       clicks_per_day,
//       installs_per_day,
//       events,
//       rule1_params,
//       rule2_params,
//       ignore_metrics
//     FROM campaign_configs
//     WHERE JSON_CONTAINS(campaign_name, JSON_QUOTE(?))
//       AND os = ?
//     LIMIT 1
//   `,

//   /**
//    * Click metrics — filtered by clicks_date (7-day window for totals,
//    * 5-day sub-aggregate for eligibility cap check, both in one pass).
//    * Also fetches MIN(shared_date) per group for Link Active eligibility.
//    *
//    * Params: [date, date, campaign_name, os, date, date]
//    *          ↑─5d─ ↑─5d                    ↑─7d─ ↑─7d
//    */
//   GET_CLICK_METRICS: `
//     SELECT
//       pubam,
//       pubid,
//       pid,
//       SUM(clicks)                                                      AS total_clicks,
//       SUM(
//         CASE
//           WHEN clicks_date BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
//           THEN clicks ELSE 0
//         END
//       )                                                                AS clicks_5d,
//       MIN(shared_date)                                                 AS shared_date
//     FROM campaign_metrics_new
//     WHERE campaign_name = ?
//       AND os            = ?
//       AND clicks_date BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?
//     GROUP BY pubam, pubid, pid
//   `,

//   /**
//    * Install metrics — filtered by install_time (7-day + 5-day in one pass).
//    * Fetches noi (installs), rti, pi for fraud and eligibility.
//    *
//    * Params: [date, date, campaign_name, os, date, date]
//    */
//   GET_INSTALL_METRICS: `
//     SELECT
//       pubam,
//       pubid,
//       pid,
//       SUM(noi)                                                         AS total_installs,
//       SUM(rti)                                                         AS total_rti,
//       SUM(pi)                                                          AS total_pi,
//       SUM(
//         CASE
//           WHEN DATE(install_time) BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
//           THEN noi ELSE 0
//         END
//       )                                                                AS installs_5d
//     FROM campaign_metrics_new
//     WHERE campaign_name       = ?
//       AND os                  = ?
//       AND DATE(install_time) BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?
//     GROUP BY pubam, pubid, pid
//   `,

//   /**
//    * Event metrics — join campaign_metrics → campaign_event_metrics.
//    * Filtered by cem.metrics_date (7-day window).
//    *
//    * e2_total   = all rows for the E2 event name (any event_type)
//    * pe_e2_total = rows for E2 where event_type = 'pe'
//    *
//    * Params: [e2EventName, e2EventName, campaign_name, os, date, date]
//    */
//   GET_EVENT_METRICS: `
//     SELECT
//       cm.pubam,
//       cm.pubid,
//       cm.pid,
//       SUM(
//         CASE WHEN cem.event_name = ?                           THEN cem.count ELSE 0 END
//       )                                                                AS e2_total,
//       SUM(
//         CASE WHEN cem.event_name = ? AND cem.event_type = 'pe' THEN cem.count ELSE 0 END
//       )                                                                AS pe_e2_total
//     FROM campaign_metrics_new       cm
//     JOIN campaign_event_metrics_new cem ON cem.campaign_metrics_id = cm.id
//     WHERE cm.campaign_name = ?
//       AND cm.os            = ?
//       AND cem.metrics_date BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?
//     GROUP BY cm.pubam, cm.pubid, cm.pid
//   `,

// };

// module.exports = QUERIES;

/**
 * decisionEngine.queries.js
 *
 * Updated queries:
 * - If campaign_id exists → use campaign_id match
 * - Else → use geo match
 */

"use strict";

const buildGeoCondition = (geo = []) =>
  geo.map(() => `JSON_CONTAINS(geo, JSON_ARRAY(?))`).join(" OR ");

const buildGeoConditionCM = (geo = []) =>
  geo.map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`).join(" OR ");

const buildCampaignCondition = (
  campaignPlaceholders,
  geoCondition,
  alias = "",
) => `
(
  ${alias}campaign_id IN (${campaignPlaceholders})

  OR (

    (${alias}campaign_id IS NULL OR ${alias}campaign_id = '')

    AND (
      ${geoCondition}
    )

  )
)
`;

const QUERIES = {
  /**
   * Params:
   * [
   *   campaign_name,
   *   os,
   *   ...campaign_ids,
   *   ...geo
   * ]
   */
  GET_CONFIG: () => `
  SELECT
    campaign_id,
    clicks_per_day,
    installs_per_day,
    events,
    rule1_params,
    rule2_params,
    ignore_metrics
  FROM campaign_configs
  WHERE JSON_CONTAINS(campaign_name, JSON_QUOTE(?))
    AND os = ?
`,

  /**
   * Params:
   * [
   *   date, date,
   *   campaign_name,
   *   os,
   *   ...campaign_ids,
   *   ...geo,
   *   date, date
   * ]
   */
  GET_CLICK_METRICS: (campaign_ids, geo) => {
    const campaignPlaceholders = campaign_ids.map(() => "?").join(",");
    const geoCondition = buildGeoCondition(geo);

    return `
      SELECT
        pubam,
        pubid,
        pid,

        SUM(clicks) AS total_clicks,

        SUM(
          CASE
            WHEN clicks_date BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
            THEN clicks ELSE 0
          END
        ) AS clicks_5d,

        MIN(shared_date) AS shared_date

      FROM campaign_metrics_new

      WHERE campaign_name = ?
        AND os = ?

        AND ${buildCampaignCondition(campaignPlaceholders, geoCondition)}

        AND clicks_date BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?

      GROUP BY pubam, pubid, pid
    `;
  },

  /**
   * Params:
   * [
   *   date, date,
   *   campaign_name,
   *   os,
   *   ...campaign_ids,
   *   ...geo,
   *   date, date
   * ]
   */
  GET_INSTALL_METRICS: (campaign_ids, geo) => {
    const campaignPlaceholders = campaign_ids.map(() => "?").join(",");
    const geoCondition = buildGeoCondition(geo);

    return `
      SELECT
        pubam,
        pubid,
        pid,

        SUM(noi) AS total_installs,
        SUM(rti) AS total_rti,
        SUM(pi) AS total_pi,

        SUM(
          CASE
            WHEN DATE(install_time)
              BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
            THEN noi ELSE 0
          END
        ) AS installs_5d

      FROM campaign_metrics_new

      WHERE campaign_name = ?
        AND os = ?

        AND ${buildCampaignCondition(campaignPlaceholders, geoCondition)}

        AND DATE(install_time)
          BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?

      GROUP BY pubam, pubid, pid
    `;
  },

  /**
   * Params:
   * [
   *   e2EventName,
   *   e2EventName,
   *   campaign_name,
   *   os,
   *   ...campaign_ids,
   *   ...geo,
   *   date,
   *   date
   * ]
   */
  GET_EVENT_METRICS: (campaign_ids, geo) => {
    const campaignPlaceholders = campaign_ids.map(() => "?").join(",");
    const geoCondition = buildGeoConditionCM(geo);

    return `
      SELECT
        cm.pubam,
        cm.pubid,
        cm.pid,

        SUM(
          CASE
            WHEN cem.event_name = ?
            THEN cem.count ELSE 0
          END
        ) AS e2_total,

        SUM(
          CASE
            WHEN cem.event_name = ?
              AND cem.event_type = 'pe'
            THEN cem.count ELSE 0
          END
        ) AS pe_e2_total

      FROM campaign_metrics_new cm

      JOIN campaign_event_metrics_new cem
        ON cem.campaign_metrics_id = cm.id

      WHERE cm.campaign_name = ?
        AND cm.os = ?

        AND (
          cm.campaign_id IN (${campaignPlaceholders})

          OR (

            (cm.campaign_id IS NULL OR cm.campaign_id = '')

            AND (
              ${geoCondition}
            )

          )
        )

        AND cem.metrics_date
          BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?

      GROUP BY cm.pubam, cm.pubid, cm.pid
    `;
  },
};

module.exports = QUERIES;
