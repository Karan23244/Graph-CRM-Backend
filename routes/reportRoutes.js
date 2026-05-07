// routes/reportRoutes.js
const express = require("express");
const router = express.Router();
const pool = require("../config/db");

router.post("/campaign-report", async (req, res) => {
  try {
    const {
      campaign_name,
      os,
      start_date,
      end_date,
      days7 = 7,
      days3 = 3,
    } = req.body;

    const [fullMetrics] = await pool.query(
      `
      SELECT
          pubam AS poc,
          pubid,
          pid,

          SUM(clicks) AS clicks,
          SUM(noi) AS installs,
          SUM(rti) AS rt_install,
          SUM(pe) AS pa_install,
          SUM(pi) AS install_fraud

      FROM campaign_metrics
      WHERE campaign_name = ?
      AND os = ?
      AND DATE(metrics_date) BETWEEN ? AND ?

      GROUP BY pubid, pid
    `,
      [campaign_name, os, start_date, end_date]
    );

    const [days7Metrics] = await pool.query(
      `
      SELECT
          pubid,
          pid,

          SUM(clicks) AS clicks_7d,
          SUM(noi) AS installs_7d,
          SUM(rti) AS rt_install_7d,
          SUM(pe) AS pa_install_7d,
          SUM(pi) AS install_fraud_7d

      FROM campaign_metrics
      WHERE campaign_name = ?
      AND os = ?
      AND DATE(metrics_date)
      BETWEEN DATE_SUB(?, INTERVAL ? DAY) AND ?

      GROUP BY pubid, pid
    `,
      [campaign_name, os, end_date, days7 - 1, end_date]
    );

    const [days3Metrics] = await pool.query(
      `
      SELECT
          pubid,
          pid,

          SUM(clicks) AS clicks_3d,
          SUM(noi) AS installs_3d,
          SUM(rti) AS rt_install_3d,
          SUM(pe) AS pa_install_3d,
          SUM(pi) AS install_fraud_3d

      FROM campaign_metrics
      WHERE campaign_name = ?
      AND os = ?
      AND DATE(metrics_date)
      BETWEEN DATE_SUB(?, INTERVAL ? DAY) AND ?

      GROUP BY pubid, pid
    `,
      [campaign_name, os, end_date, days3 - 1, end_date]
    );

    const [fullEvents] = await pool.query(
      `
      SELECT
          cm.pubid,
          cm.pid,
          cem.event_name,
          SUM(cem.count) AS total_count

      FROM campaign_metrics cm
      JOIN campaign_event_metrics cem
          ON cm.id = cem.campaign_metrics_id

      WHERE cm.campaign_name = ?
      AND cm.os = ?
      AND DATE(cm.metrics_date) BETWEEN ? AND ?

      GROUP BY cm.pubid, cm.pid, cem.event_name
    `,
      [campaign_name, os, start_date, end_date]
    );

    const [days7Events] = await pool.query(
      `
      SELECT
          cm.pubid,
          cm.pid,
          cem.event_name,
          SUM(cem.count) AS total_count_7d

      FROM campaign_metrics cm
      JOIN campaign_event_metrics cem
          ON cm.id = cem.campaign_metrics_id

      WHERE cm.campaign_name = ?
      AND cm.os = ?
      AND DATE(cm.metrics_date)
      BETWEEN DATE_SUB(?, INTERVAL ? DAY) AND ?

      GROUP BY cm.pubid, cm.pid, cem.event_name
    `,
      [campaign_name, os, end_date, days7 - 1, end_date]
    );

    const [days3Events] = await pool.query(
      `
      SELECT
          cm.pubid,
          cm.pid,
          cem.event_name,
          SUM(cem.count) AS total_count_3d

      FROM campaign_metrics cm
      JOIN campaign_event_metrics cem
          ON cm.id = cem.campaign_metrics_id

      WHERE cm.campaign_name = ?
      AND cm.os = ?
      AND DATE(cm.metrics_date)
      BETWEEN DATE_SUB(?, INTERVAL ? DAY) AND ?

      GROUP BY cm.pubid, cm.pid, cem.event_name
    `,
      [campaign_name, os, end_date, days3 - 1, end_date]
    );

    const reportMap = {};
    const allEvents = new Set();

    const getKey = (pubid, pid) => `${pubid}_${pid}`;

    fullMetrics.forEach((row) => {
      const key = getKey(row.pubid, row.pid);

      const installs = Number(row.installs || 0);
      const clicks = Number(row.clicks || 0);

      reportMap[key] = {
        key,
        poc: row.poc,
        pubid: row.pubid,
        pid: row.pid,

        clicks: {
          full: clicks,
          d7: 0,
          d3: 0,
        },

        installs: {
          full: installs,
          d7: 0,
          d3: 0,
        },

        c2i: {
          full: clicks ? ((installs / clicks) * 100).toFixed(2) : 0,
          d7: 0,
          d3: 0,
        },

        rt_install: {
          full: Number(row.rt_install || 0),
          d7: 0,
          d3: 0,
        },

        pa_install: {
          full: Number(row.pa_install || 0),
          d7: 0,
          d3: 0,
        },

        install_fraud: {
          full: Number(row.install_fraud || 0),
          d7: 0,
          d3: 0,
        },

        events: {},
      };
    });

    days7Metrics.forEach((row) => {
      const key = getKey(row.pubid, row.pid);
      if (!reportMap[key]) return;

      const installs = Number(row.installs_7d || 0);
      const clicks = Number(row.clicks_7d || 0);

      reportMap[key].clicks.d7 = clicks;
      reportMap[key].installs.d7 = installs;

      reportMap[key].c2i.d7 = clicks
        ? ((installs / clicks) * 100).toFixed(2)
        : 0;

      reportMap[key].rt_install.d7 = Number(row.rt_install_7d || 0);
      reportMap[key].pa_install.d7 = Number(row.pa_install_7d || 0);
      reportMap[key].install_fraud.d7 = Number(
        row.install_fraud_7d || 0
      );
    });

    days3Metrics.forEach((row) => {
      const key = getKey(row.pubid, row.pid);
      if (!reportMap[key]) return;

      const installs = Number(row.installs_3d || 0);
      const clicks = Number(row.clicks_3d || 0);

      reportMap[key].clicks.d3 = clicks;
      reportMap[key].installs.d3 = installs;

      reportMap[key].c2i.d3 = clicks
        ? ((installs / clicks) * 100).toFixed(2)
        : 0;

      reportMap[key].rt_install.d3 = Number(row.rt_install_3d || 0);
      reportMap[key].pa_install.d3 = Number(row.pa_install_3d || 0);
      reportMap[key].install_fraud.d3 = Number(
        row.install_fraud_3d || 0
      );
    });

    fullEvents.forEach((row) => {
      const key = getKey(row.pubid, row.pid);
      if (!reportMap[key]) return;

      allEvents.add(row.event_name);

      if (!reportMap[key].events[row.event_name]) {
        reportMap[key].events[row.event_name] = {
          full: 0,
          d7: 0,
          d3: 0,
          cr_full: 0,
          cr_d7: 0,
          cr_d3: 0,
        };
      }

      reportMap[key].events[row.event_name].full = Number(
        row.total_count || 0
      );

      const installs = reportMap[key].installs.full;

      reportMap[key].events[row.event_name].cr_full = installs
        ? ((row.total_count / installs) * 100).toFixed(2)
        : 0;
    });

    days7Events.forEach((row) => {
      const key = getKey(row.pubid, row.pid);
      if (!reportMap[key]) return;

      if (!reportMap[key].events[row.event_name]) {
        reportMap[key].events[row.event_name] = {
          full: 0,
          d7: 0,
          d3: 0,
          cr_full: 0,
          cr_d7: 0,
          cr_d3: 0,
        };
      }

      reportMap[key].events[row.event_name].d7 = Number(
        row.total_count_7d || 0
      );

      const installs = reportMap[key].installs.d7;

      reportMap[key].events[row.event_name].cr_d7 = installs
        ? ((row.total_count_7d / installs) * 100).toFixed(2)
        : 0;
    });

    days3Events.forEach((row) => {
      const key = getKey(row.pubid, row.pid);
      if (!reportMap[key]) return;

      if (!reportMap[key].events[row.event_name]) {
        reportMap[key].events[row.event_name] = {
          full: 0,
          d7: 0,
          d3: 0,
          cr_full: 0,
          cr_d7: 0,
          cr_d3: 0,
        };
      }

      reportMap[key].events[row.event_name].d3 = Number(
        row.total_count_3d || 0
      );

      const installs = reportMap[key].installs.d3;

      reportMap[key].events[row.event_name].cr_d3 = installs
        ? ((row.total_count_3d / installs) * 100).toFixed(2)
        : 0;
    });

    return res.json({
      success: true,
      events: [...allEvents],
      data: Object.values(reportMap),
    });
  } catch (error) {
    console.log(error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

module.exports = router;