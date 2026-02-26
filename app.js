const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const uploadRoutes = require("./routes/uploadRoutes");
const adjustuploadRoutes = require("./routes/adjustUploadRoutes");
const singularuploadRoutes = require("./routes/singularUploadRoutes");
const campaignRoutes = require("./routes/campaignRoutes");
const dashboardRoutes = require("./routes/dashboardRoutes");
const advertiserBillingRoutes = require("./routes/advertiserBillingRoutes");
const publisherBillingRoutes = require("./routes/publisherBillingRoutes");
const billingRoutes = require("./routes/billingRoutes");
const router = express.Router();
const http = require("http");
const pool = require("./config/db");
dotenv.config();
const app = express();
const server = http.createServer(app);
const { Server } = require("socket.io");
const cron = require("node-cron");
const { runNotificationJob } = require("./crm");
// ✅ Create Socket.IO server
const io = new Server(server, {
  cors: {
    origin: [
      "http://localhost:5173",
      "https://clickorbits.in",
      "https://gapi.pidmetric.com",
      "https://pidmetric.com",
    ],
    methods: ["GET", "POST"],
    credentials: true,
  },
});

// ✅ Attach `io` to the app BEFORE routes
app.set("io", io);
app.use(cors());
app.use(express.json());

//  ^|^e CORS FIRST

//  ^|^e Increase body size limit
app.use(express.json({ limit: "500mb" }));
app.use(express.urlencoded({ limit: "500mb", extended: true }));

//  ^|^e Routes
app.use("/api", uploadRoutes);
app.use("/api", campaignRoutes);
app.use("/api", adjustuploadRoutes);
app.use("/api", singularuploadRoutes);
app.use("/api", dashboardRoutes);
app.use("/api/billing", billingRoutes);
app.use("/api/billing", advertiserBillingRoutes);
app.use("/api/billing", publisherBillingRoutes);
app.use("/api/advertiser", require("./routes/advertiserAccountRoutes"));
app.use("/api/publisher", require("./routes/publisherAccountRoutes"));
// Helper: fetch campaign conditions; if not present, fall back to __DEFAULT__
app.get("/api/zone-conditions/:campaign", async (req, res) => {
  const campaign = req.params.campaign;
  console.log("Campaign:", campaign);

  try {
    const [rows] = await pool.query(
      `SELECT * 
       FROM campaign_zone_conditions 
       WHERE campaign_name = ?
       ORDER BY FIELD(zone_color, "Green","Yellow","Orange","Red")`,
      [campaign],
    );

    // ✅ No rows found
    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No zone conditions found for this campaign",
      });
    }

    // ✅ Rows found
    return res.status(200).json(rows);
  } catch (err) {
    console.error("DB Error:", err);
    return res.status(500).json({
      success: false,
      message: "Database error",
    });
  }
});

// Upsert (create or update) one zone condition for a campaign
app.post("/api/zone-conditions/:campaign", async (req, res) => {
  const campaign = req.params.campaign;
  const {
    zone_color,
    fraud_min = 0,
    fraud_max = 9999,
    cti_min = 0,
    cti_max = 9999,
    ite_min = 0,
    ite_max = 9999,
    etc_min = 0,
    etc_max = 9999,
  } = req.body;

  try {
    const sql = `
      INSERT INTO campaign_zone_conditions
      (campaign_name, zone_color, fraud_min, fraud_max, cti_min, cti_max, ite_min, ite_max, etc_min, etc_max)
      VALUES (?,?,?,?,?,?,?,?,?,?)
      ON DUPLICATE KEY UPDATE
        fraud_min = VALUES(fraud_min), fraud_max = VALUES(fraud_max),
        cti_min = VALUES(cti_min), cti_max = VALUES(cti_max),
        ite_min = VALUES(ite_min), ite_max = VALUES(ite_max),
        etc_min = VALUES(etc_min), etc_max = VALUES(etc_max);
    `;

    await pool.query(sql, [
      campaign,
      zone_color,
      fraud_min,
      fraud_max,
      cti_min,
      cti_max,
      ite_min,
      ite_max,
      etc_min,
      etc_max,
    ]);

    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "DB error" });
  }
});

// Restore defaults for a campaign
app.post(
  "/api/zone-conditions/:campaign/restore-defaults",
  async (req, res) => {
    const campaign = req.params.campaign;
    try {
      const [defaults] = await pool.query(
        'SELECT * FROM campaign_zone_conditions WHERE campaign_name = "__DEFAULT__"',
      );

      // insert/update each default row into campaign
      for (const d of defaults) {
        await pool.query(
          `INSERT INTO campaign_zone_conditions 
   (campaign_name, zone_color, fraud_min, fraud_max, cti_min, cti_max, ite_min, ite_max, etc_min, etc_max,
    fraud_ignore, cti_ignore, ite_ignore, etc_ignore)
   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
   ON DUPLICATE KEY UPDATE 
    fraud_min=VALUES(fraud_min), fraud_max=VALUES(fraud_max), 
    cti_min=VALUES(cti_min), cti_max=VALUES(cti_max), 
    ite_min=VALUES(ite_min), ite_max=VALUES(ite_max),
    etc_min=VALUES(etc_min), etc_max=VALUES(etc_max),
    fraud_ignore=VALUES(fraud_ignore), cti_ignore=VALUES(cti_ignore),
    ite_ignore=VALUES(ite_ignore), etc_ignore=VALUES(etc_ignore)`,
          [
            campaign,
            d.zone_color,
            d.fraud_min,
            d.fraud_max,
            d.cti_min,
            d.cti_max,
            d.ite_min,
            d.ite_max,
            d.etc_min,
            d.etc_max,
            d.fraud_ignore,
            d.cti_ignore,
            d.ite_ignore,
            d.etc_ignore,
          ],
        );
      }

      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: "DB error" });
    }
  },
);

// Add a dedicated endpoint for global ignores
app.post("/api/zone-conditions/:campaign/set-ignores", async (req, res) => {
  const campaign = req.params.campaign;
  const {
    fraud_ignore = 0,
    cti_ignore = 0,
    ite_ignore = 0,
    etc_ignore = 0,
  } = req.body;

  try {
    // ✅ update ALL rows of this campaign with same ignore flags
    await pool.query(
      `UPDATE campaign_zone_conditions
       SET fraud_ignore=?, cti_ignore=?, ite_ignore=?, etc_ignore=?
       WHERE campaign_name=?`,
      [fraud_ignore, cti_ignore, ite_ignore, etc_ignore, campaign],
    );

    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "DB error" });
  }
});

// new code
app.get("/api/zone-conditions1/:campaignName", async (req, res) => {
  const [rows] = await pool.query(
    `SELECT * FROM campaign_zone_conditions WHERE campaign_name = ?`,
    [req.params.campaignName],
  );

  if (!rows.length) {
    return res.json({ success: true, data: null });
  }
  console.log(rows);
  const conditions = {};
  const globalIgnores = {
    fraud: rows[0].fraud_ignore,
    cti: rows[0].cti_ignore,
    ite: rows[0].ite_ignore,
    etc: rows[0].etc_ignore,
  };

  for (const row of rows) {
    const zone = row.zone_color;

    for (const metric of ["fraud", "cti", "ite", "etc"]) {
      conditions[metric] ??= {};
      conditions[metric][zone] = {
        range1: {
          min: row[`${metric}_min`],
          max: row[`${metric}_max`],
        },
        range2: {
          min: row[`${metric}_min_2`],
          max: row[`${metric}_max_2`],
        },
      };
    }
  }

  res.json({ success: true, data: { conditions, globalIgnores } });
});

const ZONES = ["Green", "Yellow", "Orange", "Red"];
const METRICS = ["fraud", "cti", "ite", "etc"];

app.post("/api/zone-conditions", async (req, res) => {
  const { campaignName, globalIgnores, conditions } = req.body;

  if (!campaignName || !conditions) {
    return res.status(400).json({ message: "Invalid payload" });
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    for (const zone of ZONES) {
      const values = {
        fraud: conditions.fraud?.[zone],
        cti: conditions.cti?.[zone],
        ite: conditions.ite?.[zone],
        etc: conditions.etc?.[zone],
      };

      await conn.query(
        `
        INSERT INTO campaign_zone_conditions (
          campaign_name, zone_color,

          fraud_min, fraud_max, fraud_min_2, fraud_max_2,
          cti_min, cti_max, cti_min_2, cti_max_2,
          ite_min, ite_max, ite_min_2, ite_max_2,
          etc_min, etc_max, etc_min_2, etc_max_2,

          fraud_ignore, cti_ignore, ite_ignore, etc_ignore
        ) VALUES (?, ?,
          ?, ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?, ?
        )
        ON DUPLICATE KEY UPDATE
          fraud_min = VALUES(fraud_min),
          fraud_max = VALUES(fraud_max),
          fraud_min_2 = VALUES(fraud_min_2),
          fraud_max_2 = VALUES(fraud_max_2),

          cti_min = VALUES(cti_min),
          cti_max = VALUES(cti_max),
          cti_min_2 = VALUES(cti_min_2),
          cti_max_2 = VALUES(cti_max_2),

          ite_min = VALUES(ite_min),
          ite_max = VALUES(ite_max),
          ite_min_2 = VALUES(ite_min_2),
          ite_max_2 = VALUES(ite_max_2),

          etc_min = VALUES(etc_min),
          etc_max = VALUES(etc_max),
          etc_min_2 = VALUES(etc_min_2),
          etc_max_2 = VALUES(etc_max_2),

          fraud_ignore = VALUES(fraud_ignore),
          cti_ignore = VALUES(cti_ignore),
          ite_ignore = VALUES(ite_ignore),
          etc_ignore = VALUES(etc_ignore)
        `,
        [
          campaignName,
          zone,

          values.fraud?.range1?.min ?? null,
          values.fraud?.range1?.max ?? null,
          values.fraud?.range2?.min ?? null,
          values.fraud?.range2?.max ?? null,

          values.cti?.range1?.min ?? null,
          values.cti?.range1?.max ?? null,
          values.cti?.range2?.min ?? null,
          values.cti?.range2?.max ?? null,

          values.ite?.range1?.min ?? null,
          values.ite?.range1?.max ?? null,
          values.ite?.range2?.min ?? null,
          values.ite?.range2?.max ?? null,

          values.etc?.range1?.min ?? null,
          values.etc?.range1?.max ?? null,
          values.etc?.range2?.min ?? null,
          values.etc?.range2?.max ?? null,

          globalIgnores.fraud ?? 0,
          globalIgnores.cti ?? 0,
          globalIgnores.ite ?? 0,
          globalIgnores.etc ?? 0,
        ],
      );
    }

    await conn.commit();
    res.json({ success: true });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ message: "Failed to save conditions" });
  } finally {
    conn.release();
  }
});

// ⬇⬇⬇ ADD THIS BLOCK HERE ⬇⬇⬇

io.on("connection", (socket) => {
  console.log("🔥 New socket connected:", socket.id);

  socket.on("joinRoom", (roomId) => {
    socket.join(roomId);
    console.log(`🏠 User joined room: ${roomId}`);

    // 🔥 TEST EMIT — send message immediately after joining
    io.to(roomId).emit("testEvent", `Hello user ${roomId}, backend connected!`);
  });

  socket.on("disconnect", () => {
    console.log("❌ Socket disconnected:", socket.id);
  });
});
// /**
//  * Helper: normalize role
//  */
// const normalizeRole = (role) => {
//   if (Array.isArray(role)) role = role[0];

//   if (["publisher", "publisher_manager"].includes(role)) return role;
//   if (["advertiser", "advertiser_manager"].includes(role)) return role;

//   return role; // admin or others
// };

// /**
//  * Helper: get sub-admin user IDs
//  * users.manager_id -> manager user id
//  */
// const getSubAdminIds = async (managerId) => {
//   const [rows] = await pool.query(
//     "SELECT sub_admin_id FROM manager_subadmins WHERE manager_id = ?",
//     [managerId],
//   );

//   return rows.map((r) => r.sub_admin_id);
// };
// /**
//  * =========================
//  * DROPDOWNS API
//  * =========================
//  */
// app.post("/api/billing/dropdowns", async (req, res) => {
//   try {
//     let { roles, user_id } = req.body;

//     // Ensure roles is always an array
//     if (!Array.isArray(roles)) {
//       roles = [roles];
//     }

//     roles = roles.map(normalizeRole);

//     let publishers = [];
//     let advertisers = [];

//     const isAdmin = roles.includes("admin");
//     const isPublisherManager = roles.includes("publisher_manager");
//     const isAdvertiserManager = roles.includes("advertiser_manager");
//     const isPublisher = roles.includes("publisher");
//     const isAdvertiser = roles.includes("advertiser");

//     /**
//      * =====================
//      * ADMIN → ALL DATA
//      * =====================
//      */
//     if (isAdmin) {
//       const [pubs] = await pool.query(
//         "SELECT pub_id, pub_name FROM publids ORDER BY pub_name",
//       );

//       const [advs] = await pool.query(
//         "SELECT adv_id, adv_name FROM advids ORDER BY adv_name",
//       );

//       return res.json({
//         publishers: pubs,
//         advertisers: advs,
//       });
//     }

//     /**
//      * =====================
//      * GET SUB ADMINS (ONCE)
//      * =====================
//      */
//     let userIds = [user_id];

//     if (isPublisherManager || isAdvertiserManager) {
//       const subAdmins = await getSubAdminIds(user_id);
//       userIds = [user_id, ...subAdmins];
//     }

//     /**
//      * =====================
//      * PUBLISHERS
//      * =====================
//      */
//     if (isPublisherManager) {
//       const [rows] = await pool.query(
//         `
//         SELECT DISTINCT pub_id, pub_name
//         FROM publids
//         WHERE user_id IN (?)
//         ORDER BY pub_name
//         `,
//         [userIds],
//       );
//       publishers.push(...rows);
//     }

//     if (isPublisher) {
//       const [rows] = await pool.query(
//         `
//         SELECT pub_id, pub_name
//         FROM publids
//         WHERE user_id = ?
//         ORDER BY pub_name
//         `,
//         [user_id],
//       );
//       publishers.push(...rows);
//     }

//     /**
//      * =====================
//      * ADVERTISERS
//      * =====================
//      */
//     if (isAdvertiserManager) {
//       const [rows] = await pool.query(
//         `
//         SELECT DISTINCT adv_id, adv_name
//         FROM advids
//         WHERE user_id IN (?)
//         ORDER BY adv_name
//         `,
//         [userIds],
//       );
//       advertisers.push(...rows);
//     }

//     if (isAdvertiser) {
//       const [rows] = await pool.query(
//         `
//         SELECT adv_id, adv_name
//         FROM advids
//         WHERE user_id = ?
//         ORDER BY adv_name
//         `,
//         [user_id],
//       );
//       advertisers.push(...rows);
//     }

//     /**
//      * =====================
//      * REMOVE DUPLICATES
//      * =====================
//      */
//     publishers = Object.values(
//       publishers.reduce((acc, cur) => {
//         acc[cur.pub_id] = cur;
//         return acc;
//       }, {}),
//     );

//     advertisers = Object.values(
//       advertisers.reduce((acc, cur) => {
//         acc[cur.adv_id] = cur;
//         return acc;
//       }, {}),
//     );

//     return res.json({
//       publishers,
//       advertisers,
//     });
//   } catch (err) {
//     console.error("Billing dropdown error:", err);
//     return res.status(500).json({ message: "Server error" });
//   }
// });

// /**
//  * =========================
//  * BILLING DATA API
//  * =========================
//  */
// app.post("/api/billing/data", async (req, res) => {
//   const { type, id, month } = req.body;
//   console.log("Billing Request:", type, id, month);

//   try {
//     let rows = [];

//     // ======================================================
//     // 🔵 ADVERTISER BILLING
//     // ======================================================
//     if (type === "advertiser") {
//       [rows] = await pool.query(
//         `
//         SELECT
//           a.campaign_name,
//           a.adv_payout AS payout_rate,

//           SUM(CAST(a.adv_approved_no AS UNSIGNED)) AS approved_no,

//           SUM(
//             CAST(a.adv_approved_no AS UNSIGNED) *
//             CAST(a.adv_payout AS DECIMAL(10,2))
//           ) AS payout,

//           COALESCE(bv.status,0) AS verified

//         FROM adv_data a

//         LEFT JOIN billing_verifications bv
//           ON bv.adv_id = a.adv_id
//           AND bv.campaign_name = a.campaign_name
//           AND bv.month = ?
//           AND bv.role = 'advertiser'

//         WHERE a.adv_id = ?
//           AND a.shared_date LIKE CONCAT(?, '%')

//         GROUP BY a.campaign_name, a.adv_payout, bv.status
//         ORDER BY a.campaign_name
//         `,
//         [month, id, month],
//       );
//     }

//     // ======================================================
//     // 🟢 PUBLISHER BILLING
//     // ======================================================
//     if (type === "publisher") {
//       [rows] = await pool.query(
//         `
// SELECT
//   CONCAT(
//     x.campaign_name, ' - ',
//     GROUP_CONCAT(DISTINCT x.os ORDER BY x.os SEPARATOR ','),
//     ' - ',
//     x.geo
//   ) AS campaign_key,

//   x.campaign_name,
//   GROUP_CONCAT(DISTINCT x.os ORDER BY x.os SEPARATOR ',') AS os,
//   x.geo,
//   x.payable_event,
//   x.payout_rate,

//   SUM(x.total_no) AS total_no,
//   SUM(x.approved_no) AS approved_no,

//   SUM(x.approved_no * x.payout_rate) AS payout,

//   COALESCE(pv.status,0) AS publisher_verified,
//   COALESCE(av.status,0) AS advertiser_verified

// FROM (
//     /* ===== Data from adv_data (auto data) ===== */
//     SELECT
//       a.pub_id,
//       a.adv_id,
//       TRIM(a.campaign_name) AS campaign_name,
//       TRIM(a.geo) AS geo,
//       TRIM(a.os) AS os,
//       a.payable_event,
//       CAST(a.pay_out AS DECIMAL(10,2)) AS payout_rate,
//       CAST(NULLIF(a.adv_total_no,'') AS DECIMAL(10,2)) AS total_no,
//       CAST(NULLIF(a.pub_Apno,'') AS DECIMAL(10,2)) AS approved_no,
//       a.shared_date
//     FROM adv_data a
//     WHERE a.pub_id = ?
//       AND a.shared_date LIKE CONCAT(?, '%')

//     UNION ALL

//     /* ===== Data from publisher_entries (manual data) ===== */
//     SELECT
//       pe.pub_id,
//       NULL AS adv_id,
//       TRIM(pe.campaign_name) AS campaign_name,
//       TRIM(pe.geo) AS geo,
//       TRIM(pe.os) AS os,
//       pe.payable_event,
//       CAST(pe.payout_rate AS DECIMAL(10,2)) AS payout_rate,
//       CAST(pe.total_no AS DECIMAL(10,2)) AS total_no,
//       CAST(pe.approved_no AS DECIMAL(10,2)) AS approved_no,
//       CONCAT(pe.month, '-01') AS shared_date
//     FROM publisher_entries pe
//     WHERE pe.pub_id = ?
//       AND pe.month = ?
// ) x

// LEFT JOIN billing_verifications pv
//   ON pv.pub_id = x.pub_id
//   AND pv.campaign_name = x.campaign_name
//   AND pv.month = ?
//   AND pv.role = 'publisher'

// LEFT JOIN billing_verifications av
//   ON av.adv_id = x.adv_id
//   AND av.campaign_name = x.campaign_name
//   AND av.month = ?
//   AND av.role = 'advertiser'

// GROUP BY
//   x.campaign_name,
//   x.geo,
//   x.payable_event,
//   x.payout_rate,
//   pv.status,
//   av.status

// ORDER BY campaign_key;
// `,
//         [
//           id, // adv_data.pub_id
//           month, // adv_data.shared_date LIKE 'YYYY-MM%'
//           id, // publisher_entries.pub_id
//           month, // publisher_entries.month = 'YYYY-MM'
//           month, // verification publisher month
//           month, // verification advertiser month
//         ],
//       );
//     }

//     // ======================================================
//     // 🔁 FORMAT ROWS (LOCK UNVERIFIED)
//     // ======================================================
//     const formattedRows = rows.map((r) => {
//       if (type === "publisher" && r.advertiser_verified !== 1) {
//         return {
//           ...r,
//           approved_no: "Not verified yet",
//           payout: "Not verified yet",
//           locked: true,
//         };
//       }

//       return {
//         ...r,
//         approved_no: r.approved_no,
//         payout: r.payout,
//         locked: false,
//       };
//     });

//     // ======================================================
//     // 🧮 TOTALS (ONLY VERIFIED)
//     // ======================================================
//     const totals = rows.reduce(
//       (acc, r) => {
//         if (type === "publisher" && r.advertiser_verified !== 1) {
//           return acc;
//         }

//         acc.approved_no += Number(r.approved_no || 0);
//         acc.payout += Number(r.payout || 0);
//         return acc;
//       },
//       { approved_no: 0, payout: 0 },
//     );

//     // ======================================================
//     // ✅ RESPONSE
//     // ======================================================
//     res.json({
//       data: formattedRows,
//       totals,
//     });
//   } catch (err) {
//     console.error("Billing data error:", err);
//     res.status(500).json({ message: "Server error" });
//   }
// });
//new code start

/**
 * =========================
 * ADVERTISER BILLING API
 * =========================
 * type: advertiser
 * id  : adv_id
 * month: YYYY-MM
 */
// app.post("/api/billing/advertiser-data", async (req, res) => {
//   let { id: adv_id, month } = req.body;

//   // normalize month (VERY IMPORTANT)
//   month = month.trim(); // "2025-01"

//   try {
//     /* =========================
//        0️⃣ CHECK SNAPSHOT
//     ========================= */
//     const [[exists]] = await pool.query(
//       `
//       SELECT id
//       FROM advertiser_billing
//       WHERE adv_id = ? AND month = ?
//       LIMIT 1
//       `,
//       [adv_id, month],
//     );

//     let data = [];

//     /* =========================
//        1️⃣ FETCH FROM SNAPSHOT
//     ========================= */
//     if (exists) {
//       const [rows] = await pool.query(
//         `
//         SELECT
//           b.id AS billing_id,
//            b.status,
//           b.campaign_name,
//           b.geo,
//           b.os,
//           b.payable_event,
//           b.adv_payout,

//           b.total_no,
//           b.deductions,
//           b.approved_no,

//           (b.approved_no * b.adv_payout) AS payout_amount,

//           p.pid,
//           p.total_no AS pid_total_no,
//           p.deductions AS pid_deductions,
//           p.approved_no AS pid_approved_no

//         FROM advertiser_billing b
//         LEFT JOIN advertiser_billing_pid p
//           ON p.billing_id = b.id

//         WHERE b.adv_id = ? AND b.month = ?
//         ORDER BY b.campaign_name, b.adv_payout, p.pid
//         `,
//         [adv_id, month],
//       );

//       const map = {};
//       for (const r of rows) {
//         if (!map[r.billing_id]) {
//           map[r.billing_id] = {
//             billing_id: r.billing_id,
//             campaign_name: r.campaign_name,
//             status: r.status,
//             geo: r.geo,
//             os: r.os,
//             payable_event: r.payable_event,
//             adv_payout: r.adv_payout,

//             total_no: r.total_no,
//             deductions: r.deductions,
//             approved_no: r.approved_no,
//             payout_amount: r.payout_amount,

//             pid_data: [],
//           };
//         }

//         if (r.pid) {
//           map[r.billing_id].pid_data.push({
//             pid: r.pid,
//             total_no: r.pid_total_no,
//             deductions: r.pid_deductions,
//             approved_no: r.pid_approved_no,
//             payout_amount:
//               Number(r.pid_approved_no || 0) * Number(r.adv_payout || 0),
//           });
//         }
//       }

//       data = Object.values(map);
//     }

//     /* =========================
//        2️⃣ LIVE CALCULATION (FROM adv_data)
//     ========================= */
//     if (!exists) {
//       const [summary] = await pool.query(
//         `
//         SELECT
//           TRIM(campaign_name) AS campaign_name,
//           TRIM(geo) AS geo,
//           GROUP_CONCAT(DISTINCT TRIM(os)) AS os,
//           payable_event,
//           CAST(adv_payout AS DECIMAL(10,2)) AS adv_payout,

//           SUM(CAST(adv_total_no AS UNSIGNED)) AS total_no,
//           SUM(CAST(adv_deductions AS UNSIGNED)) AS deductions,
//           SUM(CAST(adv_approved_no AS UNSIGNED)) AS approved_no

//         FROM adv_data
//         WHERE adv_id = ?
//           AND shared_date LIKE CONCAT(?, '%')

//         GROUP BY campaign_name, geo, payable_event, adv_payout
//         `,
//         [adv_id, month],
//       );

//       const [pidRows] = await pool.query(
//         `
//         SELECT
//           TRIM(campaign_name) AS campaign_name,
//           TRIM(geo) AS geo,
//           TRIM(os) AS os,
//           payable_event,
//           pid,
//           CAST(adv_payout AS DECIMAL(10,2)) AS adv_payout,

//           SUM(CAST(adv_total_no AS UNSIGNED)) AS total_no,
//           SUM(CAST(adv_deductions AS UNSIGNED)) AS deductions,
//           SUM(CAST(adv_approved_no AS UNSIGNED)) AS approved_no

//         FROM adv_data
//         WHERE adv_id = ?
//           AND shared_date LIKE CONCAT(?, '%')

//         GROUP BY campaign_name, geo, os, payable_event, adv_payout, pid
//         `,
//         [adv_id, month],
//       );

//       data = summary.map((s) => ({
//         ...s,
//         payout_amount: Number(s.approved_no || 0) * Number(s.adv_payout || 0),
//         pid_data: pidRows
//           .filter(
//             (p) =>
//               p.campaign_name === s.campaign_name &&
//               p.geo === s.geo &&
//               Number(p.adv_payout) === Number(s.adv_payout),
//           )
//           .map((p) => ({
//             pid: p.pid,
//             total_no: p.total_no,
//             deductions: p.deductions,
//             approved_no: p.approved_no,
//             payout_amount:
//               Number(p.approved_no || 0) * Number(p.adv_payout || 0),
//           })),
//       }));
//     }

//     /* =========================
//        3️⃣ GRAND TOTALS
//     ========================= */
//     const totals = data.reduce(
//       (acc, r) => {
//         acc.total_no += Number(r.total_no || 0);
//         acc.deductions += Number(r.deductions || 0);
//         acc.approved_no += Number(r.approved_no || 0);
//         acc.payout += Number(r.payout_amount || 0);
//         return acc;
//       },
//       { total_no: 0, deductions: 0, approved_no: 0, payout: 0 },
//     );

//     res.json({
//       source: exists ? "snapshot" : "live",
//       data,
//       totals,
//     });
//   } catch (err) {
//     console.error("Advertiser fetch error:", err);
//     res.status(500).json({ message: "Server error" });
//   }
// });

// app.post("/api/billing/advertiser-save", async (req, res) => {
//   const { adv_id, month, data } = req.body;
//   const conn = await pool.getConnection();
//   console.log("Saving advertiser billing:", adv_id, month);
//   console.log("Data:", data);
//   try {
//     await conn.beginTransaction();

//     const billingIdMap = [];

//     for (const row of data) {
//       /* =========================
//      0️⃣ NORMALIZE PAYOUT
//   ========================= */
//       const adv_payout =
//         row.adv_payout !== undefined &&
//         row.adv_payout !== null &&
//         row.adv_payout !== ""
//           ? Number(row.adv_payout)
//           : 0;
//       /* =========================
//          1️⃣ CALCULATE TOTALS
//       ========================= */
//       let total_no = null;
//       let deductions = null;
//       let approved_no = null;

//       for (const p of row.pid_data || []) {
//         if (p.total_no != null) {
//           total_no = (total_no ?? 0) + Number(p.total_no);
//         }
//         if (p.deductions != null) {
//           deductions = (deductions ?? 0) + Number(p.deductions);
//         }
//         if (p.approved_no != null) {
//           approved_no = (approved_no ?? 0) + Number(p.approved_no);
//         }
//       }

//       /* =========================
//          2️⃣ UPSERT CAMPAIGN
//       ========================= */
//       let billing_id = row.billing_id || null;

//       if (billing_id) {
//         await conn.query(
//           `
//           UPDATE advertiser_billing
//           SET
//             campaign_name = ?,
//             geo = ?,
//             os = ?,
//             payable_event = ?,
//             adv_payout = ?,
//             total_no = ?,
//             deductions = ?,
//             approved_no = ?
//           WHERE id = ?
//           `,
//           [
//             row.campaign_name,
//             row.geo,
//             row.os,
//             row.payable_event,
//             adv_payout,
//             total_no,
//             deductions,
//             approved_no,
//             billing_id,
//           ],
//         );
//       } else {
//         const [result] = await conn.query(
//           `
//           INSERT INTO advertiser_billing
//           (
//             adv_id, month,
//             campaign_name, geo, os,
//             payable_event, adv_payout,
//             total_no, deductions, approved_no
//           )
//           VALUES (?,?,?,?,?,?,?,?,?,?)
//           `,
//           [
//             adv_id,
//             month,
//             row.campaign_name,
//             row.geo,
//             row.os,
//             row.payable_event,
//             adv_payout,
//             total_no,
//             deductions,
//             approved_no,
//           ],
//         );

//         billing_id = result.insertId;
//       }

//       billingIdMap.push({
//         tmp_id: row._tmp_id || null,
//         billing_id,
//       });

//       /* =========================
//          3️⃣ UPSERT PID
//       ========================= */
//       for (const p of row.pid_data || []) {
//         if (!p.pid) continue;

//         await conn.query(
//           `
//   INSERT INTO advertiser_billing_pid
//   (billing_id, pid, total_no, deductions, approved_no)
//   VALUES (?,?,?,?,?)
//   ON DUPLICATE KEY UPDATE
//     total_no = VALUES(total_no),
//     deductions = VALUES(deductions),
//     approved_no = VALUES(approved_no)
//   `,
//           [
//             billing_id,
//             p.pid,
//             p.total_no ?? null,
//             p.deductions ?? null,
//             p.approved_no ?? null,
//           ],
//         );
//       }
//     }

//     await conn.commit();

//     const [freshRows] = await conn.query(
//       `
//   SELECT *
//   FROM advertiser_billing
//   WHERE adv_id = ? AND month = ?
//   ORDER BY id
//   `,
//       [adv_id, month],
//     );

//     res.json({
//       success: true,
//       billingIdMap,
//       rows: freshRows, // 👈 send updated data
//     });
//   } catch (err) {
//     await conn.rollback();
//     console.error("Advertiser save error:", err);
//     res.status(500).json({ success: false });
//   } finally {
//     conn.release();
//   }
// });
// app.post("/api/billing/advertiser-lock", async (req, res) => {
//   const { adv_id, month } = req.body;
//   const conn = await pool.getConnection();

//   try {
//     await conn.query(
//       `
//       UPDATE advertiser_billing
//       SET status = 'locked'
//       WHERE adv_id = ? AND month = ?
//       `,
//       [adv_id, month],
//     );

//     res.json({ success: true });
//   } catch (e) {
//     res.status(500).json({ success: false });
//   } finally {
//     conn.release();
//   }
// });
// app.post("/api/billing/advertiser", async (req, res) => {
//   const { user_id, role = [], assigned_subadmins = [], month } = req.body;

//   console.log("Advertiser billing request:", {
//     user_id,
//     role,
//     assigned_subadmins,
//     month,
//   });

//   try {
//     let rows;
//     let monthFilter = "";
//     let params = [];

//     if (month) {
//       monthFilter = " AND ab.month = ? ";
//       params.push(month);
//     }

//     // ✅ ADMIN → ALL LOCKED DATA (FILTERED BY MONTH)
//     if (role.includes("admin")) {
//       let query = `
//         SELECT *
//         FROM advertiser_billing ab
//         WHERE ab.status = 'locked'
//         ${month ? "AND ab.month = ?" : ""}
//         ORDER BY ab.month DESC
//       `;

//       const [result] = await pool.query(query, month ? [month] : []);
//       rows = result;
//     }

//     // ✅ ADVERTISER MANAGER → OWN + SUBADMINS
//     else if (role.includes("advertiser_manager")) {
//       const allowedUsers = [user_id, ...assigned_subadmins];

//       let query = `
//         SELECT ab.*
//         FROM advertiser_billing ab
//         JOIN advids a ON ab.adv_id = a.adv_id
//         WHERE ab.status = 'locked'
//           AND a.user_id IN (?)
//           ${month ? "AND ab.month = ?" : ""}
//         ORDER BY ab.month DESC
//       `;

//       const queryParams = month ? [allowedUsers, month] : [allowedUsers];

//       const [result] = await pool.query(query, queryParams);
//       rows = result;
//     } else {
//       return res.status(403).json({ message: "Unauthorized" });
//     }

//     res.json({
//       success: true,
//       data: rows,
//     });
//   } catch (err) {
//     console.error("Advertiser billing error:", err);
//     res.status(500).json({ success: false, message: "Server error" });
//   }
// });
// app.post("/api/billing/publisher-data", async (req, res) => {
//   let { id: pub_id, month } = req.body;
//   month = month.trim();
//   try {
//     /* 0️⃣ snapshot exists? */
//     const [[exists]] = await pool.query(
//       `SELECT id FROM publisher_billing WHERE pub_id=? AND month=? LIMIT 1`,
//       [pub_id, month],
//     );

//     let data = [];

//     /* 1️⃣ FROM SNAPSHOT */
//     if (exists) {
//       const [rows] = await pool.query(
//         `
//         SELECT
//           b.id AS billing_id,
//            b.status,
//           b.campaign_name,
//           b.geo,
//           b.os,
//           b.payable_event,
//           b.pub_payout,

//           b.adv_total_number,
//           b.pub_apno,
//           (b.pub_apno * b.pub_payout) AS payout_amount,

//           p.pid,
//           p.adv_total_number AS pid_total,
//           p.pub_apno AS pid_apno
//         FROM publisher_billing b
//         LEFT JOIN publisher_billing_pid p
//           ON p.billing_id = b.id
//         WHERE b.pub_id=? AND b.month=?
//         ORDER BY b.campaign_name, p.pid
//         `,
//         [pub_id, month],
//       );

//       const map = {};
//       for (const r of rows) {
//         if (!map[r.billing_id]) {
//           map[r.billing_id] = {
//             billing_id: r.billing_id,
//             status: r.status,
//             campaign_name: r.campaign_name,
//             geo: r.geo,
//             os: r.os,
//             payable_event: r.payable_event,
//             pub_payout: r.pub_payout,
//             adv_total_number: r.adv_total_number,
//             pub_apno: r.pub_apno,
//             payout_amount: r.payout_amount,
//             pid_data: [],
//           };
//         }

//         if (r.pid) {
//           map[r.billing_id].pid_data.push({
//             pid: r.pid,
//             adv_total_number: r.pid_total,
//             pub_apno: r.pid_apno,
//             payout_amount: Number(r.pid_apno || 0) * Number(r.pub_payout),
//           });
//         }
//       }

//       data = Object.values(map);
//     }

//     /* 2️⃣ LIVE (from adv_data only if no snapshot) */
//     if (!exists) {
//       const [summary] = await pool.query(
//         `
//         SELECT
//           campaign_name,
//           geo,
//           GROUP_CONCAT(DISTINCT os) AS os,
//           payable_event,
//           CAST(pay_out AS DECIMAL(10,2)) AS pub_payout,
//           SUM(CAST(adv_total_no AS UNSIGNED)) AS adv_total_number,
//           SUM(NULLIF(CAST(pub_Apno AS UNSIGNED), 0)) AS pub_apno
//         FROM adv_data
//         WHERE pub_id=? AND shared_date LIKE CONCAT(?, '%')
//         GROUP BY campaign_name, geo, payable_event, pub_payout
//         `,
//         [pub_id, month],
//       );

//       const [pidRows] = await pool.query(
//         `
//         SELECT
//           campaign_name, geo, os, payable_event, pid,
//           CAST(pay_out AS DECIMAL(10,2)) AS pub_payout,
//           SUM(CAST(adv_total_no AS UNSIGNED)) AS adv_total_number,
//        SUM(NULLIF(CAST(pub_Apno AS UNSIGNED), 0)) AS pub_apno
//         FROM adv_data
//         WHERE pub_id=? AND shared_date LIKE CONCAT(?, '%')
//         GROUP BY campaign_name, geo, os, payable_event, pub_payout, pid
//         `,
//         [pub_id, month],
//       );

//       data = summary.map((s) => ({
//         ...s,
//         payout_amount:
//           s.pub_apno === null
//             ? null
//             : Number(s.pub_apno) * Number(s.pub_payout),
//         pid_data: pidRows
//           .filter(
//             (p) =>
//               p.campaign_name === s.campaign_name &&
//               p.geo === s.geo &&
//               Number(p.pub_payout) === Number(s.pub_payout),
//           )
//           .map((p) => ({
//             pid: p.pid,
//             adv_total_number: p.adv_total_number,
//             pub_apno: p.pub_apno,
//             payout_amount:
//               p.pub_apno === null
//                 ? null
//                 : Number(p.pub_apno) * Number(p.pub_payout),
//           })),
//       }));
//     }

//     /* 3️⃣ TOTALS */
//     const totals = data.reduce(
//       (a, r) => {
//         a.adv_total_number += Number(r.adv_total_number || 0);
//         a.pub_apno += Number(r.pub_apno || 0);
//         a.payout += Number(r.payout_amount || 0);
//         return a;
//       },
//       { adv_total_number: 0, pub_apno: 0, payout: 0 },
//     );

//     res.json({
//       source: exists ? "snapshot" : "live",
//       data,
//       totals,
//     });
//   } catch (e) {
//     console.error(e);
//     res.status(500).json({ message: "Server error" });
//   }
// });
// app.post("/api/billing/publisher-save", async (req, res) => {
//   const { pub_id, month, data } = req.body;
//   const conn = await pool.getConnection();
//   console.log("Data:", data);
//   try {
//     await conn.beginTransaction();
//     const billingIdMap = [];

//     for (const row of data) {
//       let adv_total_number = null;
//       let pub_apno = null;

//       for (const p of row.pid_data || []) {
//         if (p.adv_total_number != null)
//           adv_total_number =
//             (adv_total_number ?? 0) + Number(p.adv_total_number);

//         if (p.pub_apno != null) pub_apno = (pub_apno ?? 0) + Number(p.pub_apno);
//       }

//       let billing_id = row.billing_id || null;

//       /* =========================
//    2️⃣ UPSERT CAMPAIGN
// ========================= */
//       if (billing_id) {
//         await conn.query(
//           `
//     UPDATE publisher_billing
//     SET
//       campaign_name = ?,
//       geo = ?,
//       os = ?,
//       payable_event = ?,
//       pub_payout = ?,
//       adv_total_number = ?,
//       pub_apno = ?
//     WHERE id = ?
//     `,
//           [
//             row.campaign_name,
//             row.geo,
//             row.os,
//             row.payable_event,
//             row.pub_payout,
//             adv_total_number,
//             pub_apno,
//             billing_id,
//           ],
//         );
//       } else {
//         const [result] = await conn.query(
//           `
//     INSERT INTO publisher_billing
//     (
//       pub_id, month,
//       campaign_name, geo, os,
//       payable_event, pub_payout,
//       adv_total_number, pub_apno
//     )
//     VALUES (?,?,?,?,?,?,?,?,?)
//     `,
//           [
//             pub_id,
//             month,
//             row.campaign_name,
//             row.geo,
//             row.os,
//             row.payable_event,
//             row.pub_payout,
//             adv_total_number,
//             pub_apno,
//           ],
//         );

//         billing_id = result.insertId;
//       }

//       billingIdMap.push({ tmp_id: row._tmp_id || null, billing_id });

//       for (const p of row.pid_data || []) {
//         await conn.query(
//           `
//           INSERT INTO publisher_billing_pid
//           (billing_id, pid, adv_total_number, pub_apno)
//           VALUES (?,?,?,?)
//           ON DUPLICATE KEY UPDATE
//             adv_total_number=VALUES(adv_total_number),
//             pub_apno=VALUES(pub_apno)
//           `,
//           [billing_id, p.pid, p.adv_total_number ?? null, p.pub_apno ?? null],
//         );
//       }
//     }

//     await conn.commit();
//     res.json({ success: true, billingIdMap });
//   } catch (e) {
//     console.error("PUBLISHER SAVE ERROR:", e);
//     await conn.rollback();
//     res.status(500).json({ success: false, error: e.message });
//   } finally {
//     conn.release();
//   }
// });
// app.post("/api/billing/publisher-lock", async (req, res) => {
//   const { pub_id, month } = req.body;

//   try {
//     await pool.query(
//       `
//       UPDATE publisher_billing
//       SET status='locked'
//       WHERE pub_id=? AND month=?
//       `,
//       [pub_id, month],
//     );

//     res.json({ success: true });
//   } catch (e) {
//     console.error(e);
//     res.status(500).json({ success: false });
//   }
// });
// app.post("/api/billing/publisher-verify-row", async (req, res) => {
//   console.log("Verifying publisher billing row request");
//   const { billing_id } = req.body;
//   console.log("Verifying publisher billing row:", billing_id);
//   try {
//     await pool.query(
//       `
//       UPDATE publisher_billing
//       SET status='verified'
//       WHERE id=?
//       `,
//       [billing_id],
//     );

//     res.json({ success: true });
//   } catch (e) {
//     console.error(e);
//     res.status(500).json({ success: false });
//   }
// });
// app.post("/api/billing/publisher", async (req, res) => {
//   const { user_id, role = [], assigned_subadmins = [], month } = req.body;

//   console.log("Publisher billing request:", {
//     user_id,
//     role,
//     assigned_subadmins,
//     month,
//   });

//   try {
//     let rows;

//     // ✅ ADMIN → ALL LOCKED (WITH MONTH FILTER)
//     if (role.includes("admin")) {
//       const query = `
//         SELECT *
//         FROM publisher_billing pb
//         WHERE pb.status = 'locked'
//         ${month ? "AND pb.month = ?" : ""}
//         ORDER BY pb.month DESC
//       `;

//       const [result] = await pool.query(query, month ? [month] : []);

//       rows = result;
//     }

//     // ✅ PUBLISHER MANAGER
//     else if (role.includes("publisher_manager")) {
//       const allowedUsers = [user_id, ...assigned_subadmins];

//       const query = `
//         SELECT pb.*
//         FROM publisher_billing pb
//         JOIN publids p ON pb.pub_id = p.pub_id
//         WHERE pb.status = 'locked'
//           AND p.user_id IN (?)
//           ${month ? "AND pb.month = ?" : ""}
//         ORDER BY pb.month DESC
//       `;

//       const params = month ? [allowedUsers, month] : [allowedUsers];

//       const [result] = await pool.query(query, params);

//       rows = result;
//     } else {
//       return res.status(403).json({ message: "Unauthorized" });
//     }

//     res.json({
//       success: true,
//       data: rows,
//     });
//   } catch (err) {
//     console.error("Publisher billing error:", err);
//     res.status(500).json({ success: false, message: "Server error" });
//   }
// });

// routes/advertiserAccount.js

// app.get("/api/advertiser/account", async (req, res) => {
//   const { month } = req.query; // yyyy-mm

//   try {
//     // 1️⃣ Calculate totals
//     const [totals] = await pool.query(
//       `
//       SELECT
//         adv_id,
//         month,
//         SUM(approved_no * adv_payout) AS total_amount
//       FROM advertiser_billing
//       WHERE status = 'locked'
//       ${month ? "AND month = ?" : ""}
//       GROUP BY adv_id, month
//     `,
//       month ? [month] : [],
//     );

//     // 2️⃣ Upsert into advertiser_account
//     for (const row of totals) {
//       await pool.query(
//         `
//         INSERT INTO advertiser_account (adv_id, month, total_amount)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE total_amount = ?
//       `,
//         [row.adv_id, row.month, row.total_amount, row.total_amount],
//       );
//     }

//     // 3️⃣ Final data
//     const [finalData] = await pool.query(
//       `
//       SELECT
//         aa.*,
//         ad.adv_name,
//         ad.note
//       FROM advertiser_account aa
//       JOIN advids ad ON aa.adv_id = ad.adv_id
//       ${month ? "WHERE aa.month = ?" : ""}
//       ORDER BY aa.month DESC
//     `,
//       month ? [month] : [],
//     );

//     res.json(finalData);
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Server Error" });
//   }
// });

// app.put("/api/advertiser/account/update", async (req, res) => {
//   const {
//     adv_id,
//     month,
//     payment_terms,
//     payment_status,
//     amount_raised,
//     invoice_number,
//     invoice_date,
//     invoice_from,
//     invoice_to,
//     currency,
//     payment_date,
//   } = req.body;

//   try {
//     await pool.query(
//       `
//       UPDATE advertiser_account
//       SET
//         payment_terms = ?,
//         payment_status = ?,
//         amount_raised = ?,
//         invoice_number = ?,
//         invoice_date = ?,
//         invoice_from = ?,
//         invoice_to = ?,
//         currency = ?,
//         payment_date = ?
//       WHERE adv_id = ? AND month = ?
//       `,
//       [
//         payment_terms,
//         payment_status,
//         amount_raised,
//         invoice_number,
//         invoice_date,
//         invoice_from,
//         invoice_to,
//         currency,
//         payment_date,
//         adv_id,
//         month,
//       ],
//     );

//     res.json({ message: "Updated Successfully" });
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Update failed" });
//   }
// });

// app.get("/api/publisher/account", async (req, res) => {
//   const { month } = req.query;

//   try {
//     // 1️⃣ Calculate totals from locked billing
//     const [totals] = await pool.query(
//       `
//       SELECT
//         pub_id,
//         month,
//         SUM(pub_apno * pub_payout) AS total_amount
//       FROM publisher_billing
//       WHERE status = 'locked'
//       ${month ? "AND month = ?" : ""}
//       GROUP BY pub_id, month
//     `,
//       month ? [month] : [],
//     );

//     // 2️⃣ Upsert into publisher_account
//     for (const row of totals) {
//       await pool.query(
//         `
//         INSERT INTO publisher_account (pub_id, month, total_amount)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE total_amount = ?
//       `,
//         [row.pub_id, row.month, row.total_amount, row.total_amount],
//       );
//     }

//     // 3️⃣ Final data with publisher details
//     const [finalData] = await pool.query(
//       `
//       SELECT
//         pa.*,
//         p.pub_name,
//         p.note
//       FROM publisher_account pa
//       JOIN publids p ON pa.pub_id = p.pub_id
//       ${month ? "WHERE pa.month = ?" : ""}
//       ORDER BY pa.month DESC
//     `,
//       month ? [month] : [],
//     );

//     res.json(finalData);
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Server Error" });
//   }
// });
// app.put("/api/publisher/account/update", async (req, res) => {
//   const {
//     pub_id,
//     month,
//     payment_terms,
//     payment_status,
//     amount_paid,
//     invoice_number,
//     invoice_date,
//     payment_date,
//   } = req.body;

//   try {
//     await pool.query(
//       `
//       UPDATE publisher_account
//       SET
//         payment_terms = ?,
//         payment_status = ?,
//         amount_paid = ?,
//         invoice_number = ?,
//         invoice_date = ?,
//         payment_date = ?
//       WHERE pub_id = ? AND month = ?
//       `,
//       [
//         payment_terms,
//         payment_status,
//         amount_paid,
//         invoice_number,
//         invoice_date,
//         payment_date,
//         pub_id,
//         month,
//       ],
//     );

//     res.json({ message: "Updated Successfully" });
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Update failed" });
//   }
// });

// new code end

// // Daily at 1 AM
// cron.schedule("0 1 * * *", async () => {
//   console.log("🔔 Notification cron started");

//   try {
//     const result = await runNotificationJob({ dryRun: false });
//     console.log("✅ Notifications sent:", result.summary);
//   } catch (err) {
//     console.error("❌ Notification cron error:", err);
//   }
// });
const PORT = process.env.PORT || 2001;
server.listen(PORT, () => {
  console.log(`🚀 Server + Socket.IO running on http://localhost:${PORT}`);
});
