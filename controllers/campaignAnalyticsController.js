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
          cmn.geo,
          GROUP_CONCAT(DISTINCT cmn.campaign_id) AS campaign_ids
        FROM campaign_metrics_new cmn
        INNER JOIN campaign_data cd
          ON cmn.campaign_id = cd.id
        WHERE cd.user_id IN (${placeholders})
          AND cmn.campaign_name IS NOT NULL
          AND cmn.campaign_name != ''
        GROUP BY 
          cmn.campaign_name,
          cmn.os,
          cmn.geo
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
          geo,
          GROUP_CONCAT(DISTINCT campaign_id) AS campaign_ids
        FROM campaign_metrics_new
        WHERE campaign_name IS NOT NULL
          AND campaign_name != ''
        GROUP BY 
          campaign_name,
          os,
          geo
        ORDER BY campaign_name ASC;
      `;
    }

    const [rows] = await db.query(query, params);

    const formattedData = rows.map((row) => ({
      campaign_name: row.campaign_name,
      os: row.os,
      geo: row.geo,
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
  const { campaign_name, campaign_ids = [], geo = [], os } = req.body;

  const connection = await db.getConnection();

  try {
    await connection.beginTransaction();

    const campaignPlaceholders = campaign_ids.map(() => "?").join(",");

    const geoCondition = geo
      .map(() => `JSON_CONTAINS(cm.geo, JSON_ARRAY(?))`)
      .join(" OR ");

    const [metricRows] = await connection.execute(
      `
      SELECT cm.id
      FROM campaign_metrics_new cm
      WHERE LOWER(cm.campaign_name) = LOWER(?)
        AND LOWER(cm.os) = LOWER(?)
        AND (
          cm.campaign_id IN (${campaignPlaceholders})
          OR (
            (cm.campaign_id IS NULL OR cm.campaign_id = '')
            AND (${geoCondition})
          )
        )
      `,
      [campaign_name, os, ...campaign_ids, ...geo],
    );

    if (!metricRows.length) {
      await connection.rollback();

      return res.status(404).json({
        success: false,
        message: "No matching data found",
      });
    }

    const metricIds = metricRows.map((row) => row.id);

    const idPlaceholders = metricIds.map(() => "?").join(",");

    const [eventResult] = await connection.execute(
      `
      DELETE FROM campaign_event_metrics_new
      WHERE campaign_metrics_id IN (${idPlaceholders})
      `,
      metricIds,
    );

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
