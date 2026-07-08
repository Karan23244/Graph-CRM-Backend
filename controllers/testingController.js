// const db = require("../config/db");

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
