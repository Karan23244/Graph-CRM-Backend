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
    SELECT DISTINCT cmn.campaign_name
    FROM campaign_metrics_new cmn
    INNER JOIN campaign_data cd
      ON cmn.campaign_id = cd.id
    WHERE cd.user_id IN (${placeholders})
      AND cmn.campaign_name IS NOT NULL
      AND cmn.campaign_name != ''
    ORDER BY cmn.campaign_name ASC;
  `;

      params = allowedUsers;
    }

    // ==============================
    // ALL OTHER ROLES
    // ==============================
    else {
      query = `
        SELECT DISTINCT campaign_name
        FROM campaign_metrics_new
        WHERE campaign_name IS NOT NULL
          AND campaign_name != ''
        ORDER BY campaign_name ASC
      `;
    }

    const [rows] = await db.query(query, params);

    return res.status(200).json({
      success: true,
      count: rows.length,
      data: rows.map((row) => row.campaign_name),
    });
  } catch (error) {
    console.error("Error fetching campaigns:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to fetch campaign names",
      error: error.message,
    });
  }
};
