"use strict";
const db = require("../config/db");
const analyticsService = require("./campaignAnalyticsService");

exports.getCampaignAnalytics = async (req, res) => {
  try {
    console.log("REQ BODY:", req.body);

    const result = await analyticsService.getCampaignAnalytics(req.body);

    return res.status(200).json({
      success: true,
      ...result,
    });
  } catch (err) {
    console.error(err);

    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
};
// exports.getUniqueCampaigns = async (req, res) => {
//   try {
//     const query = `
//       SELECT DISTINCT campaign_name
//       FROM campaign_metrics_new
//       WHERE campaign_name IS NOT NULL
//         AND campaign_name != ''
//       ORDER BY campaign_name ASC
//     `;

//     const [rows] = await db.query(query);

//     return res.status(200).json({
//       success: true,
//       count: rows.length,
//       data: rows.map(row => row.campaign_name)
//     });

//   } catch (error) {
//     console.error('Error fetching campaigns:', error);

//     return res.status(500).json({
//       success: false,
//       message: 'Failed to fetch campaign names',
//       error: error.message
//     });
//   }
// }

exports.getUniqueCampaigns = async (req, res) => {
  try {
    const { user_id, role, assign_subadmins = [] } = req.body;

    console.log(
      "Fetching unique campaigns for user_id:",
      user_id,
      "role:",
      role,
      "assign_subadmins:",
      assign_subadmins,
    );

    const restrictedRoles = [
      "advertiser",
      "advertiser_manager",
      "adv_executive",
    ];

    let query = "";
    let params = [];

    // ==============================
    // RESTRICTED ROLES
    // ==============================
    if (Array.isArray(role) && role.some((r) => restrictedRoles.includes(r))) {
      console.log("User role requires campaign filtering:", role);

      const allowedUsers = [String(user_id)];

      if (Array.isArray(assign_subadmins)) {
        assign_subadmins.forEach((id) => {
          if (id) allowedUsers.push(String(id));
        });
      }

      const placeholders = allowedUsers.map(() => "?").join(",");

      query = `
      SELECT
      cmn.campaign_name,
      cmn.os,
      GROUP_CONCAT(DISTINCT cmn.geo SEPARATOR '|||') AS geos,
      GROUP_CONCAT(DISTINCT cmn.campaign_id) AS campaign_ids
    FROM campaign_metrics_new cmn
    INNER JOIN campaign_data cd
      ON cmn.campaign_id = cd.id
    WHERE cd.user_id IN (${placeholders})
      AND cmn.campaign_name IS NOT NULL
      AND cmn.campaign_name != ''
    GROUP BY
      cmn.campaign_name,
      cmn.os
    ORDER BY cmn.campaign_name ASC;
      `;

      params = allowedUsers;
    }

    // ==============================
    // ALL OTHER ROLES
    // ==============================
    else {
      query = `
    SELECT
      campaign_name,
      os,
      GROUP_CONCAT(DISTINCT geo SEPARATOR '|||') AS geos,
      GROUP_CONCAT(DISTINCT campaign_id) AS campaign_ids
    FROM campaign_metrics_new
    WHERE campaign_name IS NOT NULL
      AND campaign_name != ''
    GROUP BY
      campaign_name,
      os
    ORDER BY campaign_name ASC;
      `;
    }

    const [rows] = await db.query(query, params);

    const formattedData = rows.map((row) => ({
      campaign_name: row.campaign_name,

      os: row.os,

      geo: row.geos ? row.geos.split("|||").filter(Boolean) : [],

      campaign_ids: row.campaign_ids
        ? row.campaign_ids.split(",").map((id) => Number(id))
        : [],
    }));

    return res.status(200).json({
      success: true,
      count: formattedData.length,
      data: formattedData,
    });
  } catch (error) {
    console.error("Error fetching campaigns:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to fetch campaign data",
      error: error.message,
    });
  }
};
exports.deleteCampaignData = async (req, res) => {
  const {
    campaign_name,
    campaign_ids = [],
    geo = [],
    os,
    start_date,
    end_date,
  } = req.body;
  console.log("Delete Campaign Data Request:", {
    campaign_name,
    campaign_ids,
    geo,
    os,
    start_date,
    end_date,
  });
  const connection = await db.getConnection();

  try {
    await connection.beginTransaction();

    const campaignPlaceholders = campaign_ids.map(() => "?").join(",");

    const geoCondition =
      geo.length > 0
        ? geo.map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`).join(" OR ")
        : "1=0";

    const queryParams = [
      campaign_name,
      os,

      start_date,
      end_date,

      ...campaign_ids,
      ...geo,
    ];

    const [metricRows] = await connection.execute(
      `
      SELECT cm.id
      FROM campaign_metrics_new cm
      WHERE LOWER(cm.campaign_name) = LOWER(?)
        AND LOWER(cm.os) = LOWER(?)

        AND DATE(cm.metrics_date) BETWEEN DATE(?) AND DATE(?)

        AND (
          cm.campaign_id IN (${campaignPlaceholders || "NULL"})
          OR (
            (cm.campaign_id IS NULL OR cm.campaign_id = '')
            AND (${geoCondition})
          )
        )
      `,
      queryParams,
    );

    if (!metricRows.length) {
      await connection.rollback();

      return res.status(404).json({
        success: false,
        message: "No matching data found for selected date range",
      });
    }

    const metricIds = metricRows.map((row) => row.id);

    const idPlaceholders = metricIds.map(() => "?").join(",");

    // delete events first
    const [eventResult] = await connection.execute(
      `
      DELETE FROM campaign_event_metrics_new
      WHERE campaign_metrics_id IN (${idPlaceholders})
      `,
      metricIds,
    );

    // delete metrics
    const [metricResult] = await connection.execute(
      `
      DELETE FROM campaign_metrics_new
      WHERE id IN (${idPlaceholders})
      `,
      metricIds,
    );

    await connection.commit();

    return res.status(200).json({
      success: true,
      message: "Campaign data deleted successfully",
      deleted_events: eventResult.affectedRows,
      deleted_metrics: metricResult.affectedRows,
      start_date,
      end_date,
    });
  } catch (error) {
    await connection.rollback();

    console.error("Delete Campaign Data Error:", error);

    return res.status(500).json({
      success: false,
      message: error.message,
    });
  } finally {
    connection.release();
  }
};
