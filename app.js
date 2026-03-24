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
const multer = require("multer");
const XLSX = require("xlsx");
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
// ✅ MULTER CONFIG
const upload = multer({ dest: "uploads/" });

// ✅ FILE TYPE MAP
const FILE_MAP = {
  installs: "noi",
  "blocked-installs": "rti",
  "fraud-post-inapps": "pe",
  detection: "pi",
  "in-app-event": "noe",
  "non-organic-in-app-event": "noe",
};

// ✅ DATE HELPERS
function excelDateToJSDate(serial) {
  if (!serial) return null;

  const utc_days = Math.floor(serial - 25569);
  const utc_value = utc_days * 86400;
  const date_info = new Date(utc_value * 1000);

  const fractional_day = serial - Math.floor(serial);
  let total_seconds = Math.floor(86400 * fractional_day);

  const seconds = total_seconds % 60;
  total_seconds -= seconds;

  const hours = Math.floor(total_seconds / 3600);
  const minutes = Math.floor((total_seconds % 3600) / 60);

  return new Date(
    date_info.getFullYear(),
    date_info.getMonth(),
    date_info.getDate(),
    hours,
    minutes,
    seconds,
  );
}

function formatDateToMySQL(date) {
  if (!date) return null;

  const pad = (n) => (n < 10 ? "0" + n : n);

  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate(),
  )} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(
    date.getSeconds(),
  )}`;
}

// ✅ PARSE MULTIPLE FILES INTO MAP
function parseMultipleFiles(files, isEvent = false) {
  const map = new Map();

  for (const file of files) {
    const workbook = XLSX.readFile(file.path);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(sheet);

    for (const row of data) {
      const key = row["Advertising ID"] || row["IDFA"];
      if (!key) continue;

      let timeRaw = isEvent ? row["Event Time"] : row["Install Time"];
      let formattedTime = null;

      if (typeof timeRaw === "number") {
        formattedTime = formatDateToMySQL(excelDateToJSDate(timeRaw));
      } else if (typeof timeRaw === "string") {
        const [datePart, timePart] = timeRaw.split(" ");
        const [d, m, y] = datePart.split("/");
        formattedTime = `${y}-${m}-${d} ${timePart}:00`;
      }

      // ✅ keep first occurrence
      if (!map.has(key)) {
        map.set(key, formattedTime);
      }
    }
  }

  return map;
}

/* ============================= */
/* 🚀 MAIN API */
/* ============================= */

// app.post("/api/upload", upload.array("files"), async (req, res) => {
//   try {
//     const { campaignname, os, daterange, geo } = req.body;

//     if (!req.files || req.files.length === 0) {
//       return res.status(400).json({ error: "Files required" });
//     }

//     // ✅ STEP 1: Categorize files (MULTIPLE SUPPORT)
//     const categorizedFiles = {};

//     for (const file of req.files) {
//       const name = file.originalname.toLowerCase();

//       for (const key in FILE_MAP) {
//         if (name.includes(key)) {
//           const type = FILE_MAP[key];

//           if (!categorizedFiles[type]) {
//             categorizedFiles[type] = [];
//           }

//           categorizedFiles[type].push(file);
//         }
//       }
//     }

//     // ❗ Install file required
//     const installFiles = categorizedFiles.noi;

//     if (!installFiles || installFiles.length === 0) {
//       return res.status(400).json({ error: "Install file missing" });
//     }

//     // ✅ DEBUG (VERY IMPORTANT)
//     console.log({
//       installFiles: categorizedFiles.noi?.length || 0,
//       rtiFiles: categorizedFiles.rti?.length || 0,
//       piFiles: categorizedFiles.pi?.length || 0,
//       noeFiles: categorizedFiles.noe?.length || 0,
//       peFiles: categorizedFiles.pe?.length || 0,
//     });

//     // ✅ STEP 2: Create lookup maps
//     const rtiMap = categorizedFiles.rti
//       ? parseMultipleFiles(categorizedFiles.rti)
//       : new Map();

//     const piMap = categorizedFiles.pi
//       ? parseMultipleFiles(categorizedFiles.pi)
//       : new Map();

//     const noeMap = categorizedFiles.noe
//       ? parseMultipleFiles(categorizedFiles.noe, true)
//       : new Map();

//     const peMap = categorizedFiles.pe
//       ? parseMultipleFiles(categorizedFiles.pe, true)
//       : new Map();

//     // ✅ STEP 3: Process ALL install files
//     const rows = [];

//     for (const installFile of installFiles) {
//       const workbook = XLSX.readFile(installFile.path);
//       const sheet = workbook.Sheets[workbook.SheetNames[0]];
//       const data = XLSX.utils.sheet_to_json(sheet);

//       for (const row of data) {
//         const key = row["Advertising ID"] || row["IDFA"];

//         let installTimeRaw = row["Install Time"];
//         let installTime = null;

//         if (typeof installTimeRaw === "number") {
//           installTime = formatDateToMySQL(
//             excelDateToJSDate(installTimeRaw)
//           );
//         } else if (typeof installTimeRaw === "string") {
//           const [datePart, timePart] = installTimeRaw.split(" ");
//           const [d, m, y] = datePart.split("/");
//           installTime = `${y}-${m}-${d} ${timePart}:00`;
//         }

//         rows.push([
//           campaignname,
//           os,
//           daterange,
//           geo,
//           row["Country Code"] || null,
//           row["State"] || null,
//           row["City"] || null,
//           row["IP"] || null,
//           row["Advertising ID"] || null,
//           row["IDFA"] || null,
//           row["User Agent"] || null,
//           row["Device Model"] || null,
//           installTime,
//           row["Media Source"] || null,

//           // FLAGS
//           "yes",
//           rtiMap.has(key) ? "yes" : "no",
//           piMap.has(key) ? "yes" : "no",
//           noeMap.has(key) ? "yes" : "no",
//           peMap.has(key) ? "yes" : "no",

//           // TIMES
//           rtiMap.get(key) || null,
//           piMap.get(key) || null,
//           noeMap.get(key) || null,
//           peMap.get(key) || null,
//         ]);
//       }
//     }

//     // ❗ Safety check
//     if (rows.length === 0) {
//       return res.status(400).json({ error: "No data found in install file" });
//     }

//     // ✅ STEP 4: Insert into DB
//     const query = `
//       INSERT INTO campaign_uploads (
//         campaign_name, os, date_range, geo,
//         country_code, state, city, ip,
//         advertising_id, idfa, user_agent, device_model,
//         install_time, media_source,

//         noi, rti, pi, noe, pe,
//         rti_time, pi_time, noe_time, pe_time
//       ) VALUES ?
//     `;

//     const conn = await pool.getConnection();
//     await conn.query(query, [rows]);
//     conn.release();

//     res.json({
//       success: true,
//       inserted: rows.length,
//     });

//   } catch (err) {
//     console.error("ERROR:", err);
//     res.status(500).json({ error: "Server error" });
//   }
// });
app.post("/api/upload", upload.array("files"), async (req, res) => {
  const conn = await pool.getConnection();

  try {
    const { campaignname, os, daterange, geo } = req.body;

    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: "Files required" });
    }

    // ✅ Categorize files
    const categorizedFiles = {};

    for (const file of req.files) {
      const name = file.originalname.toLowerCase();

      for (const key in FILE_MAP) {
        if (name.includes(key)) {
          const type = FILE_MAP[key];

          if (!categorizedFiles[type]) {
            categorizedFiles[type] = [];
          }

          categorizedFiles[type].push(file);
        }
      }
    }

    // ❗ Install required
    const installFiles = categorizedFiles.noi;

    if (!installFiles || installFiles.length === 0) {
      return res.status(400).json({ error: "Install file missing" });
    }

    console.log("File counts:", {
      noi: categorizedFiles.noi?.length || 0,
      rti: categorizedFiles.rti?.length || 0,
      pi: categorizedFiles.pi?.length || 0,
      noe: categorizedFiles.noe?.length || 0,
      pe: categorizedFiles.pe?.length || 0,
    });

    // ✅ Create lookup maps
    const rtiMap = categorizedFiles.rti
      ? parseMultipleFiles(categorizedFiles.rti)
      : new Map();

    const piMap = categorizedFiles.pi
      ? parseMultipleFiles(categorizedFiles.pi)
      : new Map();

    const noeMap = categorizedFiles.noe
      ? parseMultipleFiles(categorizedFiles.noe, true)
      : new Map();

    const peMap = categorizedFiles.pe
      ? parseMultipleFiles(categorizedFiles.pe, true)
      : new Map();

    // ✅ Prepare rows
    const rows = [];

    for (const installFile of installFiles) {
      const workbook = XLSX.readFile(installFile.path);
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const data = XLSX.utils.sheet_to_json(sheet);

      for (const row of data) {
        const rawKey = row["Advertising ID"] || row["IDFA"] || row["IP"];

        if (!rawKey) continue;

        const key = `${campaignname}_${rawKey}`;

        if (!key) continue;

        let installTimeRaw = row["Install Time"];
        let installTime = null;

        if (typeof installTimeRaw === "number") {
          installTime = formatDateToMySQL(excelDateToJSDate(installTimeRaw));
        } else if (typeof installTimeRaw === "string") {
          const [datePart, timePart] = installTimeRaw.split(" ");
          const [d, m, y] = datePart.split("/");
          installTime = `${y}-${m}-${d} ${timePart}:00`;
        }

        rows.push([
          key,
          campaignname,
          os,
          daterange,
          geo,
          row["Country Code"] || null,
          row["State"] || null,
          row["City"] || null,
          row["IP"] || null,
          row["Advertising ID"] || null,
          row["IDFA"] || null,
          row["User Agent"] || null,
          row["Device Model"] || null,
          installTime,
          row["Media Source"] || null,

          "yes",
          rtiMap.has(key) ? "yes" : "no",
          piMap.has(key) ? "yes" : "no",
          noeMap.has(key) ? "yes" : "no",
          peMap.has(key) ? "yes" : "no",

          rtiMap.get(key) || null,
          piMap.get(key) || null,
          noeMap.get(key) || null,
          peMap.get(key) || null,
        ]);
      }
    }

    if (rows.length === 0) {
      return res.status(400).json({ error: "No data found" });
    }

    const query = `
INSERT INTO campaign_uploads (
  unique_key,
  campaign_name, os, date_range, geo,
  country_code, state, city, ip,
  advertising_id, idfa, user_agent, device_model,
  install_time, media_source,

  noi, rti, pi, noe, pe,
  rti_time, pi_time, noe_time, pe_time
) VALUES ?
ON DUPLICATE KEY UPDATE

  os = VALUES(os),
  date_range = VALUES(date_range),
  geo = VALUES(geo),
  country_code = VALUES(country_code),
  state = VALUES(state),
  city = VALUES(city),
  ip = VALUES(ip),
  user_agent = VALUES(user_agent),
  device_model = VALUES(device_model),
  media_source = VALUES(media_source),

  install_time = COALESCE(campaign_uploads.install_time, VALUES(install_time)),

  noi = 'yes',
  rti = IF(VALUES(rti) = 'yes', 'yes', campaign_uploads.rti),
  pi  = IF(VALUES(pi) = 'yes', 'yes', campaign_uploads.pi),
  noe = IF(VALUES(noe) = 'yes', 'yes', campaign_uploads.noe),
  pe  = IF(VALUES(pe) = 'yes', 'yes', campaign_uploads.pe),

  rti_time = COALESCE(VALUES(rti_time), campaign_uploads.rti_time),
  pi_time  = COALESCE(VALUES(pi_time), campaign_uploads.pi_time),
  noe_time = COALESCE(VALUES(noe_time), campaign_uploads.noe_time),
  pe_time  = COALESCE(VALUES(pe_time), campaign_uploads.pe_time)
`;

    await conn.query(query, [rows]);

    res.json({
      success: true,
      processed: rows.length,
      message: "Data inserted/updated successfully",
    });
  } catch (err) {
    console.error("ERROR:", err);
    res.status(500).json({ error: "Server error" });
  } finally {
    conn.release();
  }
});

app.get("/api/campaign-data", async (req, res) => {
  try {
    const { campaignname, os, startDate, endDate } = req.query;

    let query = `SELECT * FROM campaign_uploads`;
    const [rows] = await pool.query(query);

    res.json({
      success: true,
      count: rows.length,
      data: rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

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
