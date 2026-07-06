"use strict";

const buildGeoCondition = (geo = []) =>
  geo.length
    ? geo.map(() => `JSON_CONTAINS(geo, JSON_ARRAY(?))`).join(" OR ")
    : "";

const buildGeoConditionCM = (geo = []) =>
  geo.length
    ? geo.map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`).join(" OR ")
    : "";

const buildCampaignCondition = (
  campaignPlaceholders,
  geoCondition,
  alias = "",
) => {
  const conditions = [];

  if (campaignPlaceholders) {
    conditions.push(`${alias}campaign_id IN (${campaignPlaceholders})`);
  }

  if (geoCondition) {
    conditions.push(`
      (
        (${alias}campaign_id IS NULL OR ${alias}campaign_id = '')
        AND (${geoCondition})
      )
    `);
  }

  return conditions.length ? `(${conditions.join(" OR ")})` : "1=1";
};

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
    ignore_metrics,
     config_type
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
    const campaignPlaceholders = campaign_ids.length
      ? campaign_ids.map(() => "?").join(",")
      : "";
    const geoCondition = buildGeoCondition(geo);

    return `
      SELECT
        pubam,
        pubid,
        pid,

        SUM(clicks) AS total_clicks,

        SUM(
        CASE
          WHEN COALESCE(clicks_date, metrics_date)
              BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
          THEN clicks
          ELSE 0
        END
        ) AS clicks_5d,

        MIN(shared_date) AS shared_date

      FROM campaign_metrics_new

      WHERE campaign_name = ?
        AND os = ?
        AND is_paused = 0
        AND ${buildCampaignCondition(campaignPlaceholders, geoCondition)}

        AND COALESCE(clicks_date, metrics_date)
            BETWEEN DATE_SUB(?, INTERVAL 6 DAY) AND ?

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
    const campaignPlaceholders = campaign_ids.length
      ? campaign_ids.map(() => "?").join(",")
      : "";
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
        WHEN DATE(COALESCE(install_time, metrics_date))
             BETWEEN DATE_SUB(?, INTERVAL 4 DAY) AND ?
        THEN noi
        ELSE 0
      END
    ) AS installs_5d

  FROM campaign_metrics_new

  WHERE campaign_name = ?
    AND os = ?
    AND is_paused = 0
    AND ${buildCampaignCondition(campaignPlaceholders, geoCondition)}

    AND DATE(COALESCE(install_time, metrics_date))
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
    const campaignPlaceholders = campaign_ids.length
      ? campaign_ids.map(() => "?").join(",")
      : "";

    const geoCondition = buildGeoConditionCM(geo);

    return `
    SELECT
      cm.pubam,
      cm.pubid,
      cm.pid,

      SUM(
        CASE
          WHEN cem.event_name = ?
          THEN cem.count
          ELSE 0
        END
      ) AS e1_total,

      SUM(
        CASE
          WHEN cem.event_name = ?
          THEN cem.count
          ELSE 0
        END
      ) AS e2_total,

      SUM(
        CASE
          WHEN cem.event_name = ?
             AND cem.event_type='pe'
          THEN cem.count
          ELSE 0
        END
      ) AS pe_e1_total,

      SUM(
        CASE
          WHEN cem.event_name = ?
             AND cem.event_type='pe'
          THEN cem.count
          ELSE 0
        END
      ) AS pe_e2_total

    FROM campaign_metrics_new cm

    JOIN campaign_event_metrics_new cem
      ON cem.campaign_metrics_id = cm.id

    WHERE cm.campaign_name = ?
      AND cm.os = ?
      AND cm.is_paused = 0

      AND ${buildCampaignCondition(campaignPlaceholders, geoCondition, "cm.")}

      AND cem.metrics_date
          BETWEEN DATE_SUB(?, INTERVAL 6 DAY)
          AND ?

    GROUP BY
      cm.pubam,
      cm.pubid,
      cm.pid
  `;
  },
};

module.exports = QUERIES;
