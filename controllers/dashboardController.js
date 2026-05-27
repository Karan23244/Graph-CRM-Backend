const pool = require("../config/db");

// exports.getAdvertiserDashboardData = async (req, res) => {
//   try {
//     const { user_id, username, role, startDate, endDate } = req.body;
//     console.log("Advertiser Dashboard Request:", { user_id, username, role, startDate, endDate });
//     let query = `
//       SELECT
//         ad.*,

//         ai.adv_name,
//         CONCAT(ai.adv_name, ' (', ad.adv_id, ')') AS adv_display,

//         pi.pub_name AS pub_am,
//         CONCAT(pi.pub_name, ' (', ad.pub_id, ')') AS pub_display,

//         u.username

//       FROM adv_data ad
//       LEFT JOIN advids ai ON ai.adv_id = ad.adv_id
//       LEFT JOIN publids pi ON pi.pub_id = ad.pub_id
//       LEFT JOIN login u ON u.id = ad.user_id
//       WHERE DATE(ad.created_at) BETWEEN ? AND ?
//     `;

//     const params = [startDate, endDate];

//     // 🔐 Role-based filters
//     if (["advertiser", "advertiser_manager"].includes(role)) {
//       query += " AND ad.user_id = ?";
//       params.push(user_id);
//     }

//     if (["publisher", "publisher_manager"].includes(role)) {
//       query += " AND ad.pub_name = ?";
//       params.push(username);
//     }

//     query += " ORDER BY ad.created_at DESC";

//     const [rows] = await pool.execute(query, params);
//     console.log(`Fetched ${rows.length} rows for advertiser dashboard`);
//     res.json({
//       success: true,
//       count: rows.length,
//       data: rows,
//     });
//   } catch (error) {
//     console.error("DASHBOARD ADV DATA ERROR:", error);
//     res.status(500).json({
//       success: false,
//       message: "Server error",
//     });
//   }
// };

// exports.getAdvertiserDashboardData = async (req, res) => {
//   try {
//     const { user_id, username, role, startDate, endDate, assign_subadmin } =
//       req.body;

//     console.log("Advertiser Dashboard Request:", {
//       user_id,
//       username,
//       role,
//       startDate,
//       endDate,
//       assign_subadmin,
//     });

//     let query = `
//       SELECT
//         ad.*,

//         -- advertiser info
//         ai.adv_name,
//         CONCAT(ai.adv_name, ' (', ad.adv_id, ')') AS adv_display,

//         -- publisher info
//         pi.pub_name AS pub_am,
//         CONCAT(pi.pub_name, ' (', ad.pub_id, ')') AS pub_display,

//         -- login username
//         u.username,

//         -- campaign info
//         cd.sub_campaign_id

//       FROM adv_data ad

//       LEFT JOIN advids ai
//         ON ai.adv_id = ad.adv_id

//       LEFT JOIN publids pi
//         ON pi.pub_id = ad.pub_id

//       LEFT JOIN login u
//         ON u.id = ad.user_id

// LEFT JOIN campaign_data cd
//   ON cd.id = ad.campaign_id
//  AND cd.sub_campaign_id = ad.pid

//       WHERE DATE(ad.created_at) BETWEEN ? AND ?
//     `;

//     const params = [startDate, endDate];

//     // =========================================================
//     // 🔐 ROLE FILTERS
//     // =========================================================

//     // ✅ ADMIN -> ALL DATA
//     if (role === "admin") {
//       // no filter
//     }

//     // ✅ advertiser / advertiser_manager / adv_executive
//     else if (
//       ["advertiser", "advertiser_manager", "adv_executive"].includes(role)
//     ) {
//       // adv_executive -> only own data
//       // advertiser / advertiser_manager -> assigned data if passed

//       const targetUserId = assign_subadmin || user_id;

//       query += ` AND ad.user_id = ?`;
//       params.push(targetUserId);
//     }

//     // ✅ publisher / publisher_manager / pub_executive
//     else if (
//       ["publisher", "publisher_manager", "pub_executive"].includes(role)
//     ) {
//       let publisherName = username;

//       // if assign_subadmin sent, fetch username
//       if (assign_subadmin) {
//         const [loginRows] = await pool.execute(
//           `SELECT username FROM login WHERE id = ? LIMIT 1`,
//           [assign_subadmin],
//         );

//         if (loginRows.length > 0) {
//           publisherName = loginRows[0].username;
//         }
//       }

//       query += ` AND ad.pub_name = ?`;
//       params.push(publisherName);
//     }

//     query += ` ORDER BY ad.created_at DESC`;

//     console.log("FINAL QUERY:", query);
//     console.log("PARAMS:", params);

//     const [rows] = await pool.execute(query, params);

//     console.log(`Fetched ${rows.length} rows for advertiser dashboard`);

//     res.json({
//       success: true,
//       count: rows.length,
//       data: rows,
//     });
//   } catch (error) {
//     console.error("DASHBOARD ADV DATA ERROR:", error);

//     res.status(500).json({
//       success: false,
//       message: "Server error",
//       error: error.message,
//     });
//   }
// };

exports.getAdvertiserDashboardData = async (req, res) => {
  try {
    const { user_id, username, role, startDate, endDate, assign_subadmin } =
      req.body;

    let query = `
      SELECT
        ad.*,

        -- advertiser info
        ai.adv_name,
        CONCAT(ai.adv_name, ' (', ad.adv_id, ')') AS adv_display,

        -- publisher info
        pi.pub_name AS pub_am,
        CONCAT(pi.pub_name, ' (', ad.pub_id, ')') AS pub_display,

        -- login username
        u.username,

        -- campaign info
        cd.sub_campaign_id

      FROM adv_data ad

      LEFT JOIN advids ai
        ON ai.adv_id = ad.adv_id

      LEFT JOIN publids pi
        ON pi.pub_id = ad.pub_id

      LEFT JOIN login u
        ON u.id = ad.user_id

      -- campaign mapping
      LEFT JOIN campaign_data cd
        ON cd.id = ad.campaign_id

      WHERE DATE(ad.created_at) BETWEEN ? AND ?
    `;

    const params = [startDate, endDate];

    // =========================================================
    // 🔐 ROLE FILTERS
    // =========================================================

    // ✅ ADMIN -> ALL DATA
    if (role === "admin") {
      // no filter
    }

    // ✅ advertiser / advertiser_manager / adv_executive
    else if (
      ["advertiser", "advertiser_manager", "adv_executive"].includes(role)
    ) {
      // if assigned subadmins exist
      if (assign_subadmin?.length > 0) {
        // include self + assigned subadmins
        const userIds = [user_id, ...assign_subadmin];

        query += ` AND ad.user_id IN (${userIds.map(() => "?").join(",")})`;

        params.push(...userIds);
      } else {
        // fallback → only self data
        query += ` AND ad.user_id = ?`;
        params.push(user_id);
      }
    }

    // ✅ publisher / publisher_manager / pub_executive
    else if (
      ["publisher", "publisher_manager", "pub_executive"].includes(role)
    ) {
      // if assigned subadmins exist
      if (assign_subadmin?.length > 0) {
        // fetch usernames of all assigned subadmins
        const placeholders = assign_subadmin.map(() => "?").join(",");

        const [loginRows] = await pool.execute(
          `SELECT username FROM login WHERE id IN (${placeholders})`,
          assign_subadmin,
        );

        const usernames = loginRows.map((row) => row.username);

        // include manager's own username also
        usernames.push(username);

        if (usernames.length > 0) {
          const userPlaceholders = usernames.map(() => "?").join(",");

          query += ` AND ad.pub_name IN (${userPlaceholders})`;
          params.push(...usernames);
        }
      } else {
        // fallback → only self data
        query += ` AND ad.pub_name = ?`;
        params.push(username);
      }
    }

    query += ` ORDER BY ad.created_at DESC`;

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
      error: error.message,
    });
  }
};
