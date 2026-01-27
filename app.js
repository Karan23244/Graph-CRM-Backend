const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const uploadRoutes = require("./routes/uploadRoutes");
const adjustuploadRoutes = require("./routes/adjustUploadRoutes");
const singularuploadRoutes = require("./routes/singularUploadRoutes");
const campaignRoutes = require("./routes/campaignRoutes");
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
// app.use(cors());
// app.use(express.json());

//  ^|^e CORS FIRST

//  ^|^e Increase body size limit
app.use(express.json({ limit: "500mb" }));
app.use(express.urlencoded({ limit: "500mb", extended: true }));

//  ^|^e Routes
app.use("/api", uploadRoutes);
app.use("/api", campaignRoutes);
app.use("/api", adjustuploadRoutes);
app.use("/api", singularuploadRoutes);

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

app.post("/dashboard-adv-data", async (req, res) => {
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

    // 🔐 Role filters
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
});

/**
 * Helper: normalize role
 */
const normalizeRole = (role) => {
  if (Array.isArray(role)) role = role[0];

  if (["publisher", "publisher_manager"].includes(role)) return role;
  if (["advertiser", "advertiser_manager"].includes(role)) return role;

  return role; // admin or others
};

/**
 * Helper: get sub-admin user IDs
 * users.manager_id -> manager user id
 */
const getSubAdminIds = async (managerId) => {
  const [rows] = await pool.query("SELECT id FROM users WHERE manager_id = ?", [
    managerId,
  ]);
  return rows.map((r) => r.id);
};

/**
 * =========================
 * DROPDOWNS API
 * =========================
 */
app.post("/api/billing/dropdowns", async (req, res) => {
  let { role, user_id } = req.body;
  role = normalizeRole(role);

  try {
    let publishers = [];
    let advertisers = [];

    // ===== ADMIN =====
    if (role === "admin") {
      [publishers] = await pool.query(
        "SELECT pub_id, pub_name FROM publids ORDER BY pub_name",
      );

      [advertisers] = await pool.query(
        "SELECT adv_id, adv_name FROM advids ORDER BY adv_name",
      );
    }

    // ===== PUBLISHER MANAGER =====
    if (role === "publisher_manager") {
      const subAdmins = await getSubAdminIds(user_id);
      const userIds = [user_id, ...subAdmins];

      [publishers] = await pool.query(
        `
        SELECT DISTINCT pub_id, pub_name
        FROM publids
        WHERE user_id IN (?)
        ORDER BY pub_name
        `,
        [userIds],
      );
    }

    // ===== ADVERTISER MANAGER =====
    if (role === "advertiser_manager") {
      const subAdmins = await getSubAdminIds(user_id);
      const userIds = [user_id, ...subAdmins];

      [advertisers] = await pool.query(
        `
        SELECT DISTINCT adv_id, adv_name
        FROM advids
        WHERE user_id IN (?)
        ORDER BY adv_name
        `,
        [userIds],
      );
    }

    // ===== NORMAL PUBLISHER =====
    if (role === "publisher") {
      [publishers] = await pool.query(
        `
        SELECT pub_id, pub_name
        FROM publids
        WHERE user_id = ?
        ORDER BY pub_name
        `,
        [user_id],
      );
    }

    // ===== NORMAL ADVERTISER =====
    if (role === "advertiser") {
      [advertisers] = await pool.query(
        `
        SELECT adv_id, adv_name
        FROM advids
        WHERE user_id = ?
        ORDER BY adv_name
        `,
        [user_id],
      );
    }

    res.json({ publishers, advertisers });
  } catch (err) {
    console.error("Billing dropdown error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

/**
 * =========================
 * BILLING DATA API
 * =========================
 */
app.post("/api/billing/data", async (req, res) => {
  const { type, id, month } = req.body;
  console.log(type, id, month);
  try {
    let rows = [];

    // ===============================
    // 🔵 ADVERTISER BILLING (PAYOUT-WISE)
    // ===============================
    if (type === "advertiser") {
      [rows] = await pool.query(
        `
        SELECT
          campaign_name,
          adv_payout AS payout_rate,
          SUM(CAST(adv_approved_no AS UNSIGNED)) AS approved_no,
          SUM(
            CAST(adv_approved_no AS UNSIGNED) *
            CAST(adv_payout AS DECIMAL(10,2))
          ) AS payout
        FROM adv_data
        WHERE adv_id = ?
          AND DATE_FORMAT(shared_date, '%Y-%m') = ?
        GROUP BY campaign_name, adv_payout
        HAVING approved_no > 0
           AND payout > 0
        ORDER BY campaign_name, adv_payout
        `,
        [id, month],
      );
    }

    // ===============================
    // 🟢 PUBLISHER BILLING (PAYOUT-WISE)
    // ===============================
    if (type === "publisher") {
      [rows] = await pool.query(
        `
        SELECT
          campaign_name,
          pay_out AS payout_rate,
          SUM(CAST(pub_Apno AS UNSIGNED)) AS approved_no,
          SUM(
            CAST(pub_Apno AS UNSIGNED) *
            CAST(pay_out AS DECIMAL(10,2))
          ) AS payout
        FROM adv_data
        WHERE pub_id = ?
          AND DATE_FORMAT(shared_date, '%Y-%m') = ?
        GROUP BY campaign_name, pay_out
        HAVING approved_no > 0
           AND payout > 0
        ORDER BY campaign_name, pay_out
        `,
        [id, month],
      );
    }

    // ===============================
    // 🧮 TOTALS (FROM FILTERED DATA)
    // ===============================
    const totals = rows.reduce(
      (acc, r) => {
        acc.approved_no += Number(r.approved_no);
        acc.payout += Number(r.payout);
        return acc;
      },
      { approved_no: 0, payout: 0 },
    );

    res.json({ data: rows, totals });
  } catch (err) {
    console.error("Billing data error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

// Daily at 1 AM
cron.schedule("0 1 * * *", async () => {
  console.log("🔔 Notification cron started");

  try {
    const result = await runNotificationJob({ dryRun: false });
    console.log("✅ Notifications sent:", result.summary);
  } catch (err) {
    console.error("❌ Notification cron error:", err);
  }
});
const PORT = process.env.PORT || 2001;
server.listen(PORT, () => {
  console.log(`🚀 Server + Socket.IO running on http://localhost:${PORT}`);
});
