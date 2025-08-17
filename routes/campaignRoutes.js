const express = require("express");
const router = express.Router();
const pool = require("../config/db");
const {
  calculateCTI,
  calculateITE,
  calculateETC,
  calculateFraudScore,
  getZoneDynamic,
} = require("../zoneUtils");
router.get("/campaign-metrics", async (req, res) => {
  try {
    const [rows] = await pool.query(
      "SELECT * FROM campaign_metrics ORDER BY id DESC"
    );
    res.json(rows);
  } catch (error) {
    console.error("Error fetching metrics:", error);
    res.status(500).json({ msg: "Server error" });
  }
});
router.get("/campaign-event-metrics", async (req, res) => {
  try {
    const [rows] = await pool.query("SELECT * FROM campaign_event_metrics");
    res.json(rows);
  } catch (err) {
    console.error("❌ Failed to fetch metrics:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Get PIDs on Alert (>60% in Red/Orange)
router.get("/pids-on-alert", async (req, res) => {
  try {
    // 1️⃣ First get only PIDs with at least 5 campaigns in the current month
    const [eligiblePids] = await pool.query(`
      SELECT pid
      FROM (
        SELECT pid, COUNT(DISTINCT campaign_name) AS campaign_count
        FROM (
          SELECT pid, campaign_name
          FROM campaign_metrics
          WHERE MONTH(created_at) = MONTH(CURRENT_DATE())
            AND YEAR(created_at) = YEAR(CURRENT_DATE())
          GROUP BY pid, campaign_name
        ) t
        GROUP BY pid
      ) t2
      WHERE campaign_count >= 5
    `);

    if (eligiblePids.length === 0) {
      console.log("No PIDs with >=5 campaigns found this month");
      return res.json([]);
    }

    const pidList = eligiblePids.map((row) => row.pid);
    console.log(`Eligible PIDs (>=5 campaigns):`, pidList);

    // 2️⃣ Fetch only latest record per (pid, campaign) for those eligible PIDs
    const [metrics] = await pool.query(
      `
      SELECT cm.*
      FROM campaign_metrics cm
      INNER JOIN (
        SELECT pid, campaign_name, MAX(created_at) AS latest
        FROM campaign_metrics
        WHERE MONTH(created_at) = MONTH(CURRENT_DATE())
          AND YEAR(created_at) = YEAR(CURRENT_DATE())
          AND pid IN (?)
        GROUP BY pid, campaign_name
      ) latest_rec
      ON cm.pid = latest_rec.pid
      AND cm.campaign_name = latest_rec.campaign_name
      AND cm.created_at = latest_rec.latest
    `,
      [pidList]
    );

    console.log(`Fetched ${metrics.length} latest records for eligible PIDs`);

    // 3️⃣ Fetch conditions
    const [conditions] = await pool.query(
      "SELECT * FROM campaign_zone_conditions"
    );

    const condMap = {};
    for (const c of conditions) {
      if (!condMap[c.campaign_name]) condMap[c.campaign_name] = [];
      condMap[c.campaign_name].push(c);
    }

    const pidStats = {};

    // 4️⃣ Zone calculations only for eligible PIDs
    for (const m of metrics) {
      const cond = condMap[m.campaign_name] || condMap["__DEFAULT__"] || [];

      const fraud = calculateFraudScore(m.rti, m.pi, m.noi);
      const cti = calculateCTI(m.clicks, m.noi);
      const ite = calculateITE(m.noe, m.noi);
      const etc = calculateETC(m.nocrm, m.noe);

      const zone = getZoneDynamic(fraud, cti, ite, etc, cond);

      if (!pidStats[m.pid]) {
        pidStats[m.pid] = {
          total: 0,
          redOrOrange: 0,
          campaigns: new Set(),
          campaignDetails: [],
        };
      }

      pidStats[m.pid].total++;
      pidStats[m.pid].campaigns.add(m.campaign_name);
      if (zone === "Red" || zone === "Orange") {
        pidStats[m.pid].redOrOrange++;
      }
      pidStats[m.pid].campaignDetails.push({ campaign: m.campaign_name, zone });
    }

    // 5️⃣ Final filtering for >60% Red/Orange
    const alerts = Object.entries(pidStats)
      .filter(([pid, stats]) => {
        const redOrangePct = (stats.redOrOrange / stats.total) * 100;
        console.log(
          `PID: ${pid} | Campaigns: ${
            stats.campaigns.size
          } | % Red/Orange: ${redOrangePct.toFixed(2)}`
        );
        return redOrangePct > 60;
      })
      .map(([pid, stats]) => ({
        pid,
        campaigns: stats.campaignDetails,
      }));

    console.log(
      `Final Alert PIDs:`,
      alerts.map((a) => a.pid)
    );

    res.json(alerts);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "DB error" });
  }
});

// Get PIDs with stable performance (>60% Yellow/Green)
router.get("/pids-stable", async (req, res) => {
  try {
    // 1️⃣ First get only PIDs with at least 5 campaigns in the current month
    const [eligiblePids] = await pool.query(`
      SELECT pid
      FROM (
        SELECT pid, COUNT(DISTINCT campaign_name) AS campaign_count
        FROM (
          SELECT pid, campaign_name
          FROM campaign_metrics
          WHERE MONTH(created_at) = MONTH(CURRENT_DATE())
            AND YEAR(created_at) = YEAR(CURRENT_DATE())
          GROUP BY pid, campaign_name
        ) t
        GROUP BY pid
      ) t2
      WHERE campaign_count >= 5
    `);

    if (eligiblePids.length === 0) {
      console.log("No PIDs with >=5 campaigns found this month");
      return res.json([]);
    }

    const pidList = eligiblePids.map(row => row.pid);
    console.log(`Eligible PIDs (>=5 campaigns):`, pidList);

    // 2️⃣ Fetch only latest record per (pid, campaign) for those eligible PIDs
    const [metrics] = await pool.query(`
      SELECT cm.*
      FROM campaign_metrics cm
      INNER JOIN (
        SELECT pid, campaign_name, MAX(created_at) AS latest
        FROM campaign_metrics
        WHERE MONTH(created_at) = MONTH(CURRENT_DATE())
          AND YEAR(created_at) = YEAR(CURRENT_DATE())
          AND pid IN (?)
        GROUP BY pid, campaign_name
      ) latest_rec
      ON cm.pid = latest_rec.pid
      AND cm.campaign_name = latest_rec.campaign_name
      AND cm.created_at = latest_rec.latest
    `, [pidList]);

    console.log(`Fetched ${metrics.length} latest records for eligible PIDs`);

    // 3️⃣ Fetch conditions
    const [conditions] = await pool.query(
      "SELECT * FROM campaign_zone_conditions"
    );

    const condMap = {};
    for (const c of conditions) {
      if (!condMap[c.campaign_name]) condMap[c.campaign_name] = [];
      condMap[c.campaign_name].push(c);
    }

    const pidStats = {};

    // 4️⃣ Zone calculations only for eligible PIDs
    for (const m of metrics) {
      const cond = condMap[m.campaign_name] || condMap["__DEFAULT__"] || [];

      const fraud = calculateFraudScore(m.rti, m.pi, m.noi);
      const cti = calculateCTI(m.clicks, m.noi);
      const ite = calculateITE(m.noe, m.noi);
      const etc = calculateETC(m.nocrm, m.noe);

      const zone = getZoneDynamic(fraud, cti, ite, etc, cond);

      if (!pidStats[m.pid]) {
        pidStats[m.pid] = {
          total: 0,
          yellowOrGreen: 0,
          campaigns: new Set(),
          campaignDetails: []
        };
      }

      pidStats[m.pid].total++;
      pidStats[m.pid].campaigns.add(m.campaign_name);

      if (zone === "Yellow" || zone === "Green") {
        pidStats[m.pid].yellowOrGreen++;
      }

      pidStats[m.pid].campaignDetails.push({
        campaign: m.campaign_name,
        zone
      });
    }

    // 5️⃣ Final filtering for >60% Yellow/Green
    const stablePids = Object.entries(pidStats)
      .filter(([pid, stats]) => {
        const yellowGreenPct = (stats.yellowOrGreen / stats.total) * 100;
        console.log(
          `PID: ${pid} | Campaigns: ${stats.campaigns.size} | % Yellow/Green: ${yellowGreenPct.toFixed(
            2
          )}`
        );
        return yellowGreenPct > 60;
      })
      .map(([pid, stats]) => ({
        pid,
        campaigns: stats.campaignDetails
      }));

    console.log(`Final Stable PIDs:`, stablePids.map(a => a.pid));

    res.json(stablePids);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "DB error" });
  }
});

module.exports = router;
