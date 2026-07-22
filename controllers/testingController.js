const db = require("../config/db");

// // POST /api/campaign-publisher-map
// exports.createCampaignPublisherMap = async (req, res) => {
//   try {
//     let { entries } = req.body;
//     console.log("Received entries:", entries);
//     if (!entries || !Array.isArray(entries) || entries.length === 0) {
//       return res
//         .status(400)
//         .json({ success: false, message: "entries array is required" });
//     }

//     // Validate required fields and check for duplicates within the incoming batch
//     const incomingIds = [];
//     for (const e of entries) {
//       if (!e.campaign_id || !e.userid) {
//         return res.status(400).json({
//           success: false,
//           message: "Each entry requires campaign_id and userid",
//         });
//       }
//       if (incomingIds.includes(e.campaign_id)) {
//         return res.status(400).json({
//           success: false,
//           message: `Duplicate campaign_id ${e.campaign_id} in request`,
//         });
//       }
//       incomingIds.push(e.campaign_id);
//     }

//     // Check which campaign_ids already exist in DB
//     const placeholders = incomingIds.map(() => "?").join(",");
//     const [existing] = await db.query(
//       `SELECT campaign_id FROM campaign_publisher_map WHERE campaign_id IN (${placeholders})`,
//       incomingIds,
//     );

//     if (existing.length > 0) {
//       return res.status(409).json({
//         success: false,
//         message: "Some campaign IDs already exist in the database",
//         duplicates: existing.map((r) => r.campaign_id),
//       });
//     }

//     // One access_id for the whole batch — simple incrementing number
//     const [[{ maxId }]] = await db.query(
//       "SELECT COALESCE(MAX(access_id), 0) AS maxId FROM campaign_publisher_map",
//     );
//     const access_id = maxId + 1;

//     const rowPlaceholders = entries
//       .map(() => "(?, ?, ?, ?, ?, ?, ?)")
//       .join(", ");
//     const values = entries.flatMap((e) => [
//       access_id,
//       e.campaign_id,
//       e.campaign_name || null,
//       e.adv_name || null,
//       e.da || null,
//       e.pub_am || null,
//       e.userid,
//     ]);

//     await db.query(
//       `INSERT INTO campaign_publisher_map
//          (access_id, campaign_id, campaign_name, adv_name, da, pub_am, userid)
//        VALUES ${rowPlaceholders}`,
//       values,
//     );

//     return res.status(201).json({
//       success: true,
//       message: "Campaign publisher map created successfully",
//       access_id,
//       inserted: entries.length,
//     });
//   } catch (error) {
//     console.error("❌ createCampaignPublisherMap:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Internal server error",
//       error: error.message,
//     });
//   }
// };

// // GET /api/campaign-publisher-map
// // Query params: userid, role, access_id
// // admin / publisher_manager → returns all rows (no userid filter)
// // any other role           → filters by userid
// exports.getCampaignPublisherMap = async (req, res) => {
//   try {
//     const { userid, role } = req.query;

//     console.log("Query params:", { userid, role });

//     let query = `SELECT * FROM campaign_publisher_map`;
//     const params = [];
//     const conditions = [];

//     // Roles that can access all mappings
//     const privilegedRoles = ["admin", "publisher_manager"];

//     if (!privilegedRoles.includes(role)) {
//       if (!userid) {
//         return res.status(400).json({
//           success: false,
//           message: "userid is required",
//         });
//       }

//       conditions.push("userid = ?");
//       params.push(userid);
//     }

//     if (conditions.length) {
//       query += ` WHERE ${conditions.join(" AND ")}`;
//     }

//     query += ` ORDER BY created_at DESC`;

//     const [rows] = await db.query(query, params);

//     return res.status(200).json({
//       success: true,
//       count: rows.length,
//       data: rows,
//     });
//   } catch (error) {
//     console.error("❌ getCampaignPublisherMap:", error);

//     return res.status(500).json({
//       success: false,
//       message: "Internal server error",
//       error: error.message,
//     });
//   }
// };

// // PUT /api/campaign-publisher-map/:access_id
// // Replaces all entries under that access_id with the new entries
// exports.updateCampaignPublisherMap = async (req, res) => {
//   try {
//     const { access_id } = req.params;
//     let { entries } = req.body;

//     if (!access_id) {
//       return res
//         .status(400)
//         .json({ success: false, message: "access_id param is required" });
//     }
//     if (!entries || !Array.isArray(entries) || entries.length === 0) {
//       return res
//         .status(400)
//         .json({ success: false, message: "entries array is required" });
//     }

//     // Confirm group exists
//     const [existing] = await db.query(
//       "SELECT campaign_id FROM campaign_publisher_map WHERE access_id = ?",
//       [access_id],
//     );
//     if (existing.length === 0) {
//       return res.status(404).json({
//         success: false,
//         message: "No records found for this access_id",
//       });
//     }

//     const existingIds = existing.map((r) => r.campaign_id);

//     // Validate incoming and check for intra-batch duplicates
//     const incomingIds = [];
//     for (const e of entries) {
//       if (!e.campaign_id || !e.userid) {
//         return res.status(400).json({
//           success: false,
//           message: "Each entry requires campaign_id and userid",
//         });
//       }
//       if (incomingIds.includes(e.campaign_id)) {
//         return res.status(400).json({
//           success: false,
//           message: `Duplicate campaign_id ${e.campaign_id} in request`,
//         });
//       }
//       incomingIds.push(e.campaign_id);
//     }

//     // Check if any NEW campaign_ids (not already in this group) conflict with other groups
//     const newIds = incomingIds.filter((id) => !existingIds.includes(id));
//     if (newIds.length > 0) {
//       const conflictPlaceholders = newIds.map(() => "?").join(",");
//       const [conflicts] = await db.query(
//         `SELECT campaign_id FROM campaign_publisher_map
//          WHERE campaign_id IN (${conflictPlaceholders}) AND access_id != ?`,
//         [...newIds, access_id],
//       );
//       if (conflicts.length > 0) {
//         return res.status(409).json({
//           success: false,
//           message: "Some campaign IDs already exist under a different group",
//           duplicates: conflicts.map((r) => r.campaign_id),
//         });
//       }
//     }

//     // Delete old → insert new (same access_id preserved)
//     await db.query("DELETE FROM campaign_publisher_map WHERE access_id = ?", [
//       access_id,
//     ]);

//     const rowPlaceholders = entries
//       .map(() => "(?, ?, ?, ?, ?, ?, ?)")
//       .join(", ");
//     const values = entries.flatMap((e) => [
//       access_id,
//       e.campaign_id,
//       e.campaign_name || null,
//       e.adv_name || null,
//       e.da || null,
//       e.pub_am || null,
//       e.userid,
//     ]);

//     await db.query(
//       `INSERT INTO campaign_publisher_map
//          (access_id, campaign_id, campaign_name, adv_name, da, pub_am, userid)
//        VALUES ${rowPlaceholders}`,
//       values,
//     );

//     return res.status(200).json({
//       success: true,
//       message: "Campaign publisher map updated successfully",
//       access_id,
//       updated: entries.length,
//     });
//   } catch (error) {
//     console.error("❌ updateCampaignPublisherMap:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Internal server error",
//       error: error.message,
//     });
//   }
// };

// // DELETE /api/campaign-publisher-map/:access_id
// exports.deleteCampaignPublisherMap = async (req, res) => {
//   try {
//     const { access_id } = req.params;

//     if (!access_id) {
//       return res
//         .status(400)
//         .json({ success: false, message: "access_id param is required" });
//     }

//     const [result] = await db.query(
//       "DELETE FROM campaign_publisher_map WHERE access_id = ?",
//       [access_id],
//     );

//     if (result.affectedRows === 0) {
//       return res.status(404).json({
//         success: false,
//         message: "No records found for this access_id",
//       });
//     }

//     return res.status(200).json({
//       success: true,
//       message: "Campaign publisher map deleted successfully",
//       deletedRows: result.affectedRows,
//     });
//   } catch (error) {
//     console.error("❌ deleteCampaignPublisherMap:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Internal server error",
//       error: error.Message,
//     });
//   }
// };

// exports.getAssignCampaign = async (req, res) => {
//   try {
//     const query = `
//       SELECT
//         cd.*,
//         CONCAT(cd.Adv_name, '(', cd.adv_d, ')') AS adv_full,
//         l.username AS adv_am,
//         adv.assign_id,
//         adv.assign_user,
//         assignLogin.username AS assign_username
//       FROM campaign_data cd
//       LEFT JOIN login l
//         ON cd.user_id = l.id
//       LEFT JOIN advids adv
//         ON adv.adv_id = cd.adv_d
//       LEFT JOIN login assignLogin
//         ON assignLogin.id = adv.assign_id
//       ORDER BY cd.created_at DESC;
//     `;

//     const [rows] = await db.query(query);

//     return res.status(200).json({
//       success: true,
//       count: rows.length,
//       data: rows,
//     });
//   } catch (error) {
//     console.error("Get Campaigns Error:", error);

//     return res.status(500).json({
//       success: false,
//       message: "Internal Server Error",
//       error: error.message,
//     });
//   }
// };

// exports.getAllPublishers = async (req, res) => {
//     try {
//         console.log("🟢 Fetching all publishers...");

//         // ✅ Fetch all data from publids table
//         const [publishers] = await db.query("SELECT * FROM publids");

//         // ✅ Check if data exists
//         if (publishers.length === 0) {
//             return res.status(404).json({ success: false, message: "No publishers found." });
//         }

//         console.log("✅ Publishers retrieved successfully.");
//         res.status(200).json({ success: true, data: publishers });

//     } catch (error) {
//         console.error("❌ Error fetching publishers:", error);
//         res.status(500).json({ success: false, message: "Internal server error." });
//     }
// };
// exports.getPublishersByCampaign = async (req, res) => {
//   try {
//     const { adv_id, campaign_name, os } = req.body;

//     if (!adv_id || !campaign_name || !os) {
//       return res.status(400).json({
//         success: false,
//         message: "adv_id, campaign_name and os are required.",
//       });
//     }

//     console.log("Fetching publishers:", {
//       adv_id,
//       campaign_name,
//       os,
//     });

//     const [rows] = await db.query(
//       `
//            SELECT DISTINCT
//                 p.id,
//                 p.pub_name,
//                 p.pub_id,
//                 p.user_id,
//                 p.geo,
//                 p.note,
//                 p.pause,
//                 p.target,
//                 p.level,
//                 p.vector,
//                 p.publisher_handle,
//                 p.postback_url,
//                 p.api_token,
//                 p.api_url,
//                 p.updated_at,
//                 a.pid,
//                 a.os,
//                 a.campaign_name,
//                 a.adv_id
//             FROM adv_data a
//             INNER JOIN publids p
//                 ON p.pub_id = CAST(a.pub_id AS UNSIGNED)
//             WHERE a.adv_id = ?
//               AND a.campaign_name = ?
//               AND a.os = ?

//               -- Only current month's shared_date
//               AND MONTH(STR_TO_DATE(a.shared_date, '%Y-%m-%d')) = MONTH(CURDATE())
//               AND YEAR(STR_TO_DATE(a.shared_date, '%Y-%m-%d')) = YEAR(CURDATE())

//               -- Entry should not be paused
//               AND (a.paused_date IS NULL OR a.paused_date = '')

//             ORDER BY p.pub_name, a.pid;
//             `,
//       [adv_id, campaign_name, os],
//     );
//     console.log(
//       `Fetched ${rows.length} publishers for campaign "${campaign_name}" and OS "${os}".`,
//     );
//     console.log("Publisher data sample:", rows.slice(0, 5)); // Log first 5 rows for verification
//     return res.status(200).json({
//       success: true,
//       count: rows.length,
//       data: rows,
//     });
//   } catch (error) {
//     console.error("Error fetching publishers:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Internal server error.",
//     });
//   }
// };
const updatePublisherEmail = async (connection, pub_id, mail) => {
  try {
    if (!mail) return; // skip if no email provided

    await connection.query(`UPDATE pub_accounts SET mail = ? WHERE pubid = ?`, [
      mail,
      pub_id,
    ]);

    console.log("✅ pub_accounts email updated:", mail, pub_id);
  } catch (err) {
    console.error("❌ Failed to update pub_accounts email:", err);
    throw err; // important for transaction rollback
  }
};

exports.updatePublisher = async (req, res) => {
  const connection = await db.getConnection();
  await connection.beginTransaction();

  try {
    console.log("🟡 Update Publisher Request Received:", req.body);

    const {
      pub_name,
      pub_id,
      user_id,
      geo,
      note,
      target,
      level,
      vector,
      username,
      mail,
      pause,
      role,
    } = req.body;

    if (!pub_id || !user_id) {
      return res
        .status(400)
        .json({ success: false, message: "pub_id and user_id are required" });
    }

    // ✅ Get existing publisher
    const [existingPublisherRows] = await connection.query(
      "SELECT * FROM publids WHERE pub_id = ?",
      [pub_id],
    );

    if (!existingPublisherRows.length) {
      connection.release();
      return res
        .status(404)
        .json({ success: false, message: "Publisher not found" });
    }

    const previous_user_id = existingPublisherRows[0].user_id;

    // ✅ Step 1: Update publisher data
    const [result] = await connection.query(
      "UPDATE publids SET pub_name = ?, geo = ?, note = ?, target = ?, user_id = ?, level = ?, vector = ?,pause=? WHERE pub_id = ?",
      [pub_name, geo, note, target, user_id, level, vector, pause, pub_id],
    );

    console.log("🔄 Rows affected:", result.affectedRows);

    // ✅ Step 2: Update adv_data (assuming pub_id maps to adv_id or adjust logic as needed)
    //   await connection.query(
    //     `UPDATE adv_data SET pub_name = ? WHERE pub_id = ?`,
    //   [username, pub_id] // if pub_id ≠ adv_id, change this to pub_data logic
    // );
    console.log("✅ adv_data Updated Successfully", username, pub_id);

    // ✅ NEW STEP: Update email in pub_accounts
    await updatePublisherEmail(connection, pub_id, mail);

    // ✅ Step 3: Log the transfer
    if (previous_user_id !== user_id) {
      await connection.query(
        `INSERT INTO adv_transfer_logs (adv_id, from_user_id, to_user_id) VALUES (?, ?, ?)`,
        [pub_id, previous_user_id, user_id], // again, adjust if it's pub_transfer_logs
      );

      // ✅ Step 4: Update id_ranges if sub_admin_id matches
      await connection.query(
        // `UPDATE id_ranges SET sub_admin_id = ? WHERE sub_admin_id = ?`,
        `UPDATE id_assignments SET sub_admin_id = ? WHERE single_id = ?`,

        [user_id, pub_id],
      );
    }
    console.log("✅ ID Ranges Updated Successfully");

    await connection.commit();
    connection.release();

    console.log("✅ Publisher Updated Successfully");
    res
      .status(200)
      .json({ success: true, message: "Publisher updated successfully" });
  } catch (error) {
    await connection.rollback();
    connection.release();
    console.error("❌ Server Error:", error);
    res.status(500).json({ success: false, message: "Internal server error" });
  }
};

exports.getPublisherStatus = async (req, res) => {
  try {
    const { publisher } = req.query;

    if (!publisher) {
      return res.status(400).json({
        success: false,
        message: "Publisher name is required",
      });
    }

    const searchWord = publisher.trim().toLowerCase();

    const query = `
      SELECT
          p.pub_id,
          p.pub_name,
          p.user_id,
          l.username,
          p.pause,
          CASE
              WHEN p.pause = '1' THEN 'Paused'
              ELSE 'Active'
          END AS status
      FROM publids p
      LEFT JOIN login l
          ON p.user_id = l.id
      WHERE LOWER(p.pub_name) REGEXP CONCAT('(^|[[:space:]])', ?, '([[:space:]]|$)')
      LIMIT 1
    `;

    const [rows] = await db.query(query, [searchWord]);

    if (rows.length === 0) {
      return res.json({
        success: true,
        found: false,
        message: "Publisher not found",
      });
    }

    return res.json({
      success: true,
      found: true,
      data: rows[0],
    });
  } catch (error) {
    console.error(error);

    res.status(500).json({
      success: false,
      message: "Internal Server Error",
    });
  }
};

const getAccessibleUserIds = async (conn, userId) => {
  try {
    // =========================================================
    // 1️⃣ Get current user
    // =========================================================

    const [[user]] = await conn.query(
      `SELECT id, role FROM login WHERE id = ? AND pause = 0`,
      [userId],
    );

    if (!user) return [];

    const { role } = user;

    // =========================================================
    //  HELPER: GET FULL HIERARCHY (RECURSIVE)
    // =========================================================
    const getAllSubAdmins = async (startIds) => {
      let allIds = [...startIds];
      let queue = [...startIds];

      while (queue.length > 0) {
        const placeholders = queue.map(() => "?").join(",");

        const [rows] = await conn.query(
          `SELECT sub_admin_id
             FROM manager_subadmins
             WHERE manager_id IN (${placeholders})`,
          queue,
        );

        const newIds = rows
          .map((r) => r.sub_admin_id)
          .filter((id) => !allIds.includes(id));

        if (newIds.length === 0) break;

        allIds.push(...newIds);
        queue = newIds;
      }

      return allIds;
    };

    // =========================================================
    // ADMIN → FULL ACCESS OF ALL APIS
    // =========================================================
    if (role === "admin") {
      const [allUsers] = await conn.query(
        `SELECT id FROM login WHERE pause = 0`,
      );
      return allUsers.map((u) => u.id);
    }

    // =========================================================
    //  MANAGER ROLES → FULL TREE ACCESS
    // publisher_manager / advertiser_manager
    // =========================================================
    if (["publisher_manager", "advertiser_manager"].includes(role)) {
      const allIds = await getAllSubAdmins([userId]);
      return [...new Set(allIds)];
    }

    // =========================================================
    //  PUBLISHER / ADVERTISER
    // → self + their executives
    // =========================================================
    if (["publisher", "advertiser"].includes(role)) {
      const allIds = await getAllSubAdmins([userId]);
      return [...new Set(allIds)];
    }

    // =========================================================
    //  EXECUTIVES → ONLY SELF
    // =========================================================
    if (["pub_executive", "adv_executive"].includes(role)) {
      return [userId];
    }

    // =========================================================
    //  OPERATIONS / OPTIMIZATION
    // → self (extend later with assignment table)
    // =========================================================
    if (["operations", "optimization"].includes(role)) {
      return [userId];
    }

    // =========================================================
    //  DEFAULT → SAFE FALLBACK FOR ALL
    // =========================================================
    return [userId];
  } catch (err) {
    console.error("Access Control Error:", err);
    return [];
  }
};

exports.getNamePublishers = async (req, res) => {
  try {
    console.log("🟢 Fetching publishers with role-based access...");
    const { user_id } = req.query;
    console.log(req.query);
    if (!user_id) {
      return res.status(400).json({
        success: false,
        message: "user_id is required",
      });
    }

    // =========================================================
    // 🔥 GET ACCESSIBLE USER IDS
    // =========================================================
    const accessibleIds = await getAccessibleUserIds(db, user_id);

    console.log("✅ Accessible IDs:", accessibleIds);

    if (!accessibleIds.length) {
      return res.status(200).json({
        success: true,
        data: [],
      });
    }

    const placeholders = accessibleIds.map(() => "?").join(",");

    // =========================================================
    // 🔥 MAIN QUERY (FILTERED)
    // =====================================================
    const [publishers] = await db.query(
      `
  SELECT
    p.*,
 
    l.username AS username,
    l.role     AS user_role,
 
    pa.username AS publisher_username,
    pa.role     AS publisher_role,
    pa.act_pass AS password,
    pa.mail     AS mail,
    pa.id       AS publisher_login_id,
 
    -- ✅ Billing as JSON array (clean + no null objects)
    COALESCE(
      JSON_ARRAYAGG(
        CASE
          WHEN pbd.id IS NOT NULL THEN JSON_OBJECT(
            'id', pbd.id,
            'legal_name', pbd.legal_name,
            'billing_address', pbd.billing_address,
            'tax_type', pbd.tax_type,
            'tax_id', pbd.tax_id,
            'user_id', pbd.user_id,
            'created_at', pbd.created_at,
            'updated_at', pbd.updated_at
          )
        END
      ),
      JSON_ARRAY()
    ) AS billing_details
 
  FROM publids p
 
  LEFT JOIN login l
    ON p.user_id = l.id
 
  LEFT JOIN pub_accounts pa
    ON pa.pubid = p.pub_id
 
  LEFT JOIN publisher_billing_details pbd
    ON pbd.pub_id = p.pub_id
 
  WHERE p.user_id IN (${placeholders})
 
  GROUP BY
    p.pub_id,
    p.id,
    l.username,
    l.role,
    pa.username,
    pa.role,
    pa.act_pass,
    pa.mail,
    pa.id
  `,
      accessibleIds,
    );
    console.log(publishers[0]);
    //  console.log("Current User:", user);
    console.log("Accessible IDs Count:", accessibleIds.length);
    console.log("First 20 IDs:", accessibleIds.slice(0, 20));
    if (publishers.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No publishers found.",
      });
    }

    console.log("✅ Publishers retrieved successfully.");
    return res.status(200).json({
      success: true,
      data: publishers,
    });
  } catch (error) {
    console.error("❌ Error fetching publishers:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error.",
    });
  }
};

// app.post("/addPubRequestnew", async (req, res) => {
//   try {
//     const {
//       adv_name,
//       campaign_name,
//       payout,
//       os,
//       pub_name,
//       pub_id,
//       pid,
//       geo,
//       note,
//       adv_am,
//     } = req.body;

//     const io = req.app.locals.io;
//     let insertedIds = [];

//     // Normalize arrays
//     const payoutArr = Array.isArray(payout) ? payout : [payout];
//     const geoArr = Array.isArray(geo) ? geo : [geo];
//     const osArr = Array.isArray(os) ? os : [os];

//     // Look up adv_name in login table once — used for campaign_ids lookup below
//     const [[loginRow]] = await db.query(
//       "SELECT id FROM login WHERE username = ? LIMIT 1",
//       [adv_am],
//     );
//     const advUserId = loginRow?.id || null;

//     // We loop based on payout/geo length (main rows)
//     for (let i = 0; i < payoutArr.length; i++) {
//       // ----- Normalize OS for this row -----
//       let thisOS = osArr[i] || osArr[0];
//       let rowOsList = [];

//       if (thisOS === "both") {
//         rowOsList = ["Android", "iOS"];
//       } else if (Array.isArray(thisOS)) {
//         rowOsList = thisOS;
//       } else {
//         rowOsList = [thisOS];
//       }

//       // ----- Normalize GEO for this row -----
//       let thisGeo = geoArr[i] || geoArr[0];
//       if (!Array.isArray(thisGeo)) thisGeo = [thisGeo];

//       let thisPayout = payoutArr[i] || payoutArr[0];

//       // Now create entries for each OS of this row
//       for (let j = 0; j < rowOsList.length; j++) {
//         const finalOS = rowOsList[j];

//         // Find matching campaign IDs from campaign_data
//         // finalOS is always a single value here ("Android"/"iOS") since "both" is already expanded above
//         let campaignIds = [];

//         const getCampaignIds = async (osName) => {
//           let ids = [];

//           // First try using login.user_id
//           if (advUserId) {
//             const [campaigns] = await db.query(
//               `SELECT id FROM campaign_data
//        WHERE campaign_name = ? AND os = ? AND user_id = ?`,
//               [campaign_name, osName, advUserId],
//             );

//             ids = campaigns.map((c) => c.id);
//           }

//           // Fallback using advids.assign_id
//           if (ids.length === 0) {
//             const [[advRow]] = await db.query(
//               `SELECT assign_id FROM advids WHERE adv_name = ? LIMIT 1`,
//               [adv_name],
//             );

//             if (advRow?.assign_id) {
//               const [campaigns] = await db.query(
//                 `SELECT id FROM campaign_data
//          WHERE campaign_name = ? AND os = ? AND user_id = ?`,
//                 [campaign_name, osName, advRow.assign_id],
//               );

//               ids = campaigns.map((c) => c.id);
//             }
//           }

//           return ids;
//         };

//         if (thisOS === "both") {
//           let androidIds = await getCampaignIds("Android");
//           let iosIds = await getCampaignIds("iOS");

//           // Copy IDs if one side is missing
//           if (androidIds.length === 0 && iosIds.length > 0) {
//             androidIds = [...iosIds];
//           }

//           if (iosIds.length === 0 && androidIds.length > 0) {
//             iosIds = [...androidIds];
//           }

//           campaignIds = finalOS === "Android" ? androidIds : iosIds;
//         } else {
//           campaignIds = await getCampaignIds(finalOS);
//         }

//         const [result] = await db.query(
//           `INSERT INTO pub_req
//           (adv_name, campaign_name, payout, os, pub_name, pub_id, pid, geo, note, campaign_ids)
//           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
//           [
//             adv_name,
//             campaign_name,
//             thisPayout,
//             finalOS,
//             pub_name,
//             pub_id,
//             pid,
//             JSON.stringify(thisGeo),
//             note,
//             JSON.stringify(campaignIds),
//           ],
//         );

//         insertedIds.push(result.insertId);

//         // Real-time Emit
//         io.emit("pub_request_added", {
//           id: result.insertId,
//           adv_name,
//           campaign_name,
//           payout: thisPayout,
//           os: finalOS,
//           pub_name,
//           pub_id,
//           pid,
//           geo: thisGeo,
//           note,
//         });
//       }
//     }

//     return res.status(201).json({
//       success: true,
//       message: "Pub request processed successfully",
//       inserted_ids: insertedIds,
//     });
//   } catch (error) {
//     console.error("❌ Error in addPubRequest:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Internal server error",
//       error: error.message,
//     });
//   }
// });
