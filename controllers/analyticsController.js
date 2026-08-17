// ==========================
// 📁 controllers/analyticsController.js
// ==========================
const db = require("../config/db");
const service = require("../services/analyticsService");

exports.getRevenueAnalytics = async (req, res) => {
  try {
    const { user_id, role, month, assign_subadmin } = req.body;
    if (!month) {
      return res.status(400).json({
        error: "month required",
      });
    }

    // 🔹 Admin → no filter
    // 🔹 Others → filter by assign_subadmin
    const pubIds = await service.getPublisherIds(
      user_id,
      role,
      assign_subadmin,
    );

    const [geo, vertical, os] = await Promise.all([
      service.getTopGeo(pubIds, month),
      service.getTopVertical(pubIds, month),
      service.getTopOS(pubIds, month),
    ]);

    res.json({ geo, vertical, os });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Internal Server Error",
    });
  }
};

exports.getPidHistory = async (req, res) => {
  try {
    const { pid } = req.params;
    console.log(pid);
    if (!pid) {
      return res.status(400).json({
        success: false,
        message: "PID is required",
      });
    }

    // ===========================
    // Summary
    // ===========================

    const [summary] = await db.query(
      `
      SELECT
          ? AS pid,

          COUNT(DISTINCT campaign_id) AS total_campaigns,

          SUM(clicks) AS clicks,

          SUM(noi) AS installs,

          SUM(rti) AS rti,

          SUM(pi) AS pi,

          (SUM(rti)+SUM(pi)) AS total_fraud,

          ROUND(
              CASE
                  WHEN SUM(noi)=0 THEN 0
                  ELSE ((SUM(rti)+SUM(pi))*100)/SUM(noi)
              END,
          2) AS fraud_percentage,

          MAX(metrics_date) AS latest_activity

      FROM campaign_metrics_new

      WHERE pid=?
      AND STR_TO_DATE(metrics_date,'%Y-%m-%d')
      BETWEEN DATE_SUB(CURDATE(),INTERVAL 60 DAY)
      AND CURDATE()
      `,
      [pid, pid],
    );

    // ===========================
    // Campaign Wise
    // ===========================

    const [campaigns] = await db.query(
      `
  SELECT
      campaign_id,
      campaign_name,
      pubam,
      os,
      geo,

      SUM(clicks) AS clicks,
      SUM(noi) AS installs,
      SUM(rti) AS rti,
      SUM(pi) AS pi,

      (SUM(rti)+SUM(pi)) AS total_fraud,

      ROUND(
          CASE
              WHEN SUM(noi)=0 THEN 0
              ELSE ((SUM(rti)+SUM(pi))*100)/SUM(noi)
          END,
      2) AS fraud_percentage,

      MAX(metrics_date) AS latest_metrics_date

  FROM campaign_metrics_new

  WHERE pid=?
  AND STR_TO_DATE(metrics_date,'%Y-%m-%d')
      BETWEEN DATE_SUB(CURDATE(),INTERVAL 60 DAY)
      AND CURDATE()

  GROUP BY
      campaign_id,
      campaign_name,
      os,
      geo,
      pubam

  HAVING SUM(noi) >= 200

  ORDER BY latest_metrics_date DESC
  `,
      [pid],
    );

    return res.status(200).json({
      success: true,
      summary: summary[0] || {},
      campaigns,
    });
  } catch (err) {
    console.error("PID History Error:", err);

    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: err.message,
    });
  }
};
