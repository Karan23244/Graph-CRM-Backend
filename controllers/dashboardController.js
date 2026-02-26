const pool = require("../config/db");

exports.getAdvertiserDashboardData = async (req, res) => {
  try {
    const { user_id, username, role, startDate, endDate } = req.body;

    let query = `
      SELECT
        ad.*,

        ai.adv_name,
        CONCAT(ai.adv_name, ' (', ad.adv_id, ')') AS adv_display,

        pi.pub_name AS pub_am,
        CONCAT(pi.pub_name, ' (', ad.pub_id, ')') AS pub_display,

        u.username

      FROM adv_data ad
      LEFT JOIN advids ai ON ai.adv_id = ad.adv_id
      LEFT JOIN publids pi ON pi.pub_id = ad.pub_id
      LEFT JOIN login u ON u.id = ad.user_id
      WHERE DATE(ad.created_at) BETWEEN ? AND ?
    `;

    const params = [startDate, endDate];

    // 🔐 Role-based filters
    if (["advertiser", "advertiser_manager"].includes(role)) {
      query += " AND ad.user_id = ?";
      params.push(user_id);
    }

    if (["publisher", "publisher_manager"].includes(role)) {
      query += " AND ad.pub_name = ?";
      params.push(username);
    }

    query += " ORDER BY ad.created_at DESC";

    const [rows] = await pool.execute(query, params);

    res.json({
      success: true,
      count: rows.length,
      data: rows,
    });
  } catch (error) {
    console.error("DASHBOARD ADV DATA ERROR:", error);
    res.status(500).json({
      success: false,
      message: "Server error",
    });
  }
};
