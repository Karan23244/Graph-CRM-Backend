// optimizedUploadHandler.js
const ExcelJS = require("exceljs");
const fs = require("fs");
const path = require("path");
const csv = require("fast-csv");
const pool = require("../config/db");
const { processCampaignUploads } = require("../services/campaignUploadService");
const FILE_MAP = {
  installs: "noi",
  "blocked-installs": "rti",
  "fraud-post-inapps": "pe",
  detection: "pi",
  "in-app-event": "noe",
  "non-organic-in-app-event": "noe",
  clicks: "clicks",
};

// ---------------------------
// Helpers
// ---------------------------
async function streamXlsxRows(filePath, onRow) {
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
    entries: "emit",
    sharedStrings: "cache",
    styles: "cache",
    hyperlinks: "ignore",
    worksheets: "emit",
  });

  let lastSheetReader = null;
  for await (const ws of reader) lastSheetReader = ws;
  if (!lastSheetReader) return;

  let headers = [];
  for await (const row of lastSheetReader) {
    if (row.number === 1) {
      // normalize header names (no spaces, lowercased)
      headers = row.values.map((h) =>
        (h || "").toString().replace(/\s+/g, "").toLowerCase(),
      );
      continue;
    }
    const rowObj = {};
    row.values.forEach((v, i) => {
      if (i === 0) return;
      rowObj[headers[i] || `col${i}`] = v ?? "";
    });
    await onRow(rowObj, headers);
  }
}
function formatEventDateTime(value) {
  if (!value) return null;

  const match = String(value).match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})$/,
  );

  if (!match) return null;

  let [, dd, mm, yyyy, hh, min] = match;

  // convert 2-digit year → 20xx
  if (yyyy.length === 2) {
    yyyy = `20${yyyy}`;
  }

  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")} ${hh.padStart(2, "0")}:${min}:00`;
}
// --- Add this helper at the top of your file ---
function formatLocalDate(date) {
  if (!(date instanceof Date) || isNaN(date)) return null;
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`; // Pure local date, no UTC shift
}

function streamCsvRows(filePath, onRow) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    const parser = csv
      .parse({ headers: true, renameHeaders: false, trim: true })
      .on("error", (err) => reject(err))
      .on("data", async (row) => {
        // normalize header keys as in xlsx path
        const normalized = {};
        for (const key in row) {
          normalized[key.replace(/\s+/g, "").toLowerCase()] = row[key];
        }
        parser.pause();
        try {
          await onRow(normalized, Object.keys(normalized));
        } catch (e) {
          parser.destroy(e);
        } finally {
          parser.resume();
        }
      })
      .on("end", () => resolve());

    stream.pipe(parser);
  });
}

async function streamFileRows(filePath, onRow) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".xlsx") return streamXlsxRows(filePath, onRow);
  else return streamCsvRows(filePath, onRow);
}

async function batchInsert(sql, data, batchSize = 500) {
  if (!data.length) return;
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    await pool.query(sql, [batch]);
  }
}
function excelSerialToDate(serial) {
  if (typeof serial !== "number" || isNaN(serial)) return null;

  // Excel starts on 1899-12-30
  const base = new Date(1899, 11, 30);
  const jsDate = new Date(base.getTime() + serial * 86400000);

  // Force extract local date parts — no timezone shift
  const year = jsDate.getFullYear();
  const month = String(jsDate.getMonth() + 1).padStart(2, "0");
  const day = String(jsDate.getDate()).padStart(2, "0");

  return `${year}-${month}-${day}`;
}

// Robust normalization for a raw date value -> 'YYYY-MM-DD' or null
function normalizeDateRaw(raw) {
  if (raw == null || raw === "") return null;

  // If already a Date object
  if (raw instanceof Date) {
    if (isNaN(raw)) return null;
    return `${raw.getFullYear()}-${String(raw.getMonth() + 1).padStart(
      2,
      "0",
    )}-${String(raw.getDate()).padStart(2, "0")}`;
  }

  // ExcelJS sometimes returns objects like { richText: ... } or { text: '...' } - handle common cases
  if (typeof raw === "object") {
    if (raw.text) raw = raw.text;
    else if (raw.result) raw = raw.result;
    else if (raw.richText && Array.isArray(raw.richText)) {
      raw = raw.richText.map((t) => t.text).join("");
    } else {
      return null;
    }
  }

  // If number -> treat as excel serial
  if (typeof raw === "number") {
    const d = excelSerialToDate(raw);
    if (d && !isNaN(d))
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(
        2,
        "0",
      )}-${String(d.getDate()).padStart(2, "0")}`;

    return null;
  }

  // If string -> try multiple formats
  let s = String(raw).trim();

  // Clean common artifacts
  s = s.replace(/\.(?=\d{2,4}\b)/g, "/"); // e.g. 02.07.25 -> 02/07/25
  s = s.replace(/(\d)st|\d(nd)|\d(th)|\d(rd)/i, ""); // remove ordinal suffixes
  s = s.replace(/\s+/, " ");

  // Try Date.parse first (handles ISO formats)
  const parsed = new Date(s);
  if (!isNaN(parsed)) {
    return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(
      2,
      "0",
    )}-${String(parsed.getDate()).padStart(2, "0")}`;
  }

  // Try dd/mm/yy or dd/mm/yyyy
  const m1 = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
  if (m1) {
    let day = parseInt(m1[1], 10);
    let month = parseInt(m1[2], 10);
    let year = parseInt(m1[3], 10);
    if (year < 100) year += 2000;
    const d = new Date(year, month - 1, day);
    if (!isNaN(d)) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    }
  }

  // Try yyyy/mm/dd
  const m2 = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m2) {
    const d = new Date(
      parseInt(m2[1], 10),
      parseInt(m2[2], 10) - 1,
      parseInt(m2[3], 10),
    );
    if (!isNaN(d)) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    }
  }

  // If not parsed, return null
  return null;
}

// metricName-aware extraction of date from row/headers
function parseExcelDate(value, metricName, pid) {
  if (!value) {
    console.log(`⚠️ [${metricName}] Missing date for PID=${pid}`);
    return null;
  }

  // Case 1: Already a Date object
  if (value instanceof Date) {
    const iso = `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(
      2,
      "0",
    )}-${String(value.getDate()).padStart(2, "0")}`;
    console.log(`📅 [${metricName}] Parsed Date object → ${iso} (PID=${pid})`);
    return iso;
  }

  // Case 2: Excel serial number
  if (!isNaN(value) && Number(value) > 30000) {
    const base = new Date(1899, 11, 30);
    const dt = new Date(base.getTime() + Number(value) * 86400 * 1000);
    const iso = formatLocalDate(dt);
    console.log(
      `📅 [${metricName}] Parsed Excel serial=${value} → ${iso} (PID=${pid})`,
    );
    return iso;
  }

  // Case 3: String values
  if (typeof value === "string") {
    let cleaned = value.trim();

    // YYYY-MM-DD
    let m = cleaned.match(/^(\d{4})[-/](\d{2})[-/](\d{2})$/);
    if (m) {
      const iso = `${m[1]}-${m[2]}-${m[3]}`;
      console.log(
        `📅 [${metricName}] Parsed YYYY-MM-DD='${cleaned}' → ${iso} (PID=${pid})`,
      );
      return iso;
    }

    // DD/MM/YYYY
    m = cleaned.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
    if (m) {
      const day = m[1].padStart(2, "0");
      const month = m[2].padStart(2, "0");
      const iso = `${m[3]}-${month}-${day}`;
      console.log(
        `📅 [${metricName}] Parsed DD/MM/YYYY='${cleaned}' → ${iso} (PID=${pid})`,
      );
      return iso;
    }

    // fallback
    const dt = new Date(cleaned);
    if (!isNaN(dt)) {
      const iso = formatLocalDate(dt);
      console.log(
        `📅 [${metricName}] JS fallback parsed '${cleaned}' → ${iso} (PID=${pid})`,
      );
      return iso;
    }
  }

  console.log(
    `❌ [${metricName}] Could not parse date='${value}' (PID=${pid})`,
  );
  return null;
}

function extractDate(row, headers, metricName) {
  let rawDate = null;

  if (metricName === "noi" || metricName === "pi" || metricName === "rti") {
    const key = headers.find((c) => c.toLowerCase().includes("installtime"));
    rawDate = key ? row[key] : null;
  } else if (metricName === "noe" || metricName === "pe") {
    const key = headers.find((c) => c.toLowerCase().includes("eventtime"));
    rawDate = key ? row[key] : null;
  } else if (metricName === "clicks") {
    const key = headers.find((c) => c.toLowerCase() === "date");
    rawDate = key ? row[key] : null;
  }

  if (!rawDate) return null;

  if (typeof rawDate === "string") {
    const s = rawDate.trim();

    // Match formats like "29/09/2025 20:10" or "29/09/2025"
    const m1 = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
    if (m1) {
      let [_, d, m, y] = m1;
      if (y.length === 2) y = "20" + y;
      // Return directly without timezone shift
      return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
    }

    // Match "2025-09-29" or "2025/09/29"
    const m2 = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
    if (m2) {
      const [_, y, m, d] = m2;
      return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
    }

    // As a fallback, don't use toISOString — just trust local parts
    const parts = s.split(/[\/\-\s:]/);
    if (parts.length >= 3) {
      let [p1, p2, p3] = parts;
      if (p1.length === 4) {
        // YYYY-MM-DD
        return `${p1}-${p2.padStart(2, "0")}-${p3.padStart(2, "0")}`;
      } else {
        // DD-MM-YYYY
        if (p3.length === 2) p3 = "20" + p3;
        return `${p3}-${p2.padStart(2, "0")}-${p1.padStart(2, "0")}`;
      }
    }
    return null;
  }

  // Excel numeric serials
  if (typeof rawDate === "number" && rawDate > 30000) {
    const excelEpoch = new Date(1899, 11, 30);
    const jsDate = new Date(excelEpoch.getTime() + rawDate * 86400000);
    // Get local date parts (not UTC)
    const y = jsDate.getFullYear();
    const m = String(jsDate.getMonth() + 1).padStart(2, "0");
    const d = String(jsDate.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  // If Date object
  if (rawDate instanceof Date && !isNaN(rawDate)) {
    const y = rawDate.getFullYear();
    const m = String(rawDate.getMonth() + 1).padStart(2, "0");
    const d = String(rawDate.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  return null;
}

// ---------------------------
// Fetch adv_data by campaign + dateRange
// ---------------------------
// async function getAdvDataFromDB(campaignName, startDate, endDate, os) {
//   const [rows] = await pool.query(
//     `SELECT pid, pub_id, pub_name, campaign_name, paused_date, flag, os
// FROM adv_data
// WHERE REPLACE(REPLACE(REPLACE(campaign_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), '') =
//       REPLACE(REPLACE(REPLACE(?, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
//   AND DATE(shared_date) BETWEEN ? AND ?
//   AND os = ?;`,
//     [campaignName, startDate, endDate, os],
//   );

//   const pidMap = new Map();
//   for (const r of rows) {
//     const pidKey = String(r.pid || "")
//       .trim()
//       .toLowerCase();
//     if (!pidMap.has(pidKey)) {
//       pidMap.set(pidKey, {
//         pid: String(r.pid || "").trim(),
//         pidLower: pidKey,
//         pubid: String(r.pub_id || "").trim(),
//         pubam: String(r.pub_name || "").trim(),
//         campaign_name: r.campaign_name,
//         pause: r.paused_date ? 1 : 0,
//         nocrm: 0,
//         os: r.os,
//       });
//     }
//   }

//   return Array.from(pidMap.values());
// }

// ==============================
// 1. UPDATE getAdvDataFromDB()
// ==============================

async function getAdvDataFromDB(
  campaignName,
  startDate,
  endDate,
  os,
  campaignIds,
) {
  const [rows] = await pool.query(
    `SELECT 
      pid,
      pub_id,
      pub_name,
      campaign_name,
      paused_date,
      flag,
      os,
      campaign_id,
      shared_date
   FROM adv_data
   WHERE REPLACE(REPLACE(REPLACE(campaign_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), '') =
         REPLACE(REPLACE(REPLACE(?, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
     AND campaign_id IN (?)
     AND DATE(shared_date) BETWEEN ? AND ?
     AND os = ?`,
    [campaignName, campaignIds, startDate, endDate, os],
  );

  const pidMap = new Map();

  for (const r of rows) {
    const pidKey = String(r.pid || "")
      .trim()
      .toLowerCase();

    if (!pidMap.has(pidKey)) {
      pidMap.set(pidKey, {
        pid: String(r.pid || "").trim(),
        pidLower: pidKey,
        pubid: String(r.pub_id || "").trim(),
        pubam: String(r.pub_name || "").trim(),
        campaign_name: r.campaign_name,
        campaign_id: r.campaign_id, // ✅ NEW
        shared_date: r.shared_date, // ✅ NEW
        pause: r.paused_date ? 1 : 0,
        nocrm: 0,
        os: r.os,
      });
    }
  }

  return Array.from(pidMap.values());
}

// ---------------------------
// Main upload handler
// ---------------------------
// ---------------------------
// Main upload handler
// ---------------------------
// ---------------------------
// Main upload handler
// ---------------------------
const handleUpload = async (req, res) => {
  try {
    const { campaignName, os, geo, dateRange, socketId } = req.body;
    const campaignIds = JSON.parse(req.body.campaign_ids || "[]");
    console.log(socketId);
    const fullCampaignName = campaignName;
    const baseCampaignName = campaignName.split(",")[0];

    const [startDate, endDate] = dateRange.split(" - ").map((d) => d.trim());
    if (!startDate || !endDate) {
      return res.status(400).json({ msg: "Invalid dateRange format" });
    }

    let uploaded = [];
    if (Array.isArray(req.files)) uploaded = req.files;
    else if (req.files) uploaded = Object.values(req.files).flat();
    else if (req.file) uploaded = [req.file];

    if (!campaignName || uploaded.length === 0) {
      return res.status(400).json({ msg: "Missing fields or files" });
    }

    console.log("📂 Fetching adv_data for campaign:", campaignName);
    // 🔥 CALL HERE
    // const rowsInsertedUploads = await processCampaignUploads({
    //   files: uploaded,
    //   campaignname: campaignName,
    //   os,
    //   daterange: dateRange,
    //   geo,
    //   conn: pool, // use same DB connection
    // });

    // console.log("✅ campaign_uploads inserted:", rowsInsertedUploads);

    const advData = await getAdvDataFromDB(
      baseCampaignName,
      startDate,
      endDate,
      os,
      campaignIds,
    );
    // ✅ Step 2: Fetch adv_data from 30 days before startDate
    const prev30Start = new Date(startDate);
    prev30Start.setDate(prev30Start.getDate() - 30);
    const prev30Str = prev30Start.toISOString().split("T")[0];
    console.log(
      `📅 Fetching additional adv_data from ${prev30Str} to ${startDate} (30 days before)`,
    );
    const advDataPrev30 = await getAdvDataFromDB(
      baseCampaignName,
      prev30Str,
      startDate,
      os,
    );
    // ✅ Step 3: Create a map of all advData by PID
    const advPidMap = new Map();
    for (const d of advData) advPidMap.set(d.pidLower, d);
    // ✅ Step 4: Collect all PIDs that appear in uploaded files
    const filePids = new Set();
    if (!advData.length)
      return res
        .status(400)
        .json({ msg: "No valid PID entries found in adv_data." });

    const metricCounts = {
      noi: new Map(),
      rti: new Map(),
      pe: new Map(),
      pi: new Map(),
      noe: new Map(),
      clicks: new Map(),
    };
    const eventCountsByPidDate = new Map();
    const eventTypeCounts = new Map();

    const incEventCount = (pid, date, eventName, eventType, inc = 1) => {
      if (!pid || !date || !eventName || !eventType) return;

      if (!eventTypeCounts.has(pid)) {
        eventTypeCounts.set(pid, new Map());
      }

      const dateMap = eventTypeCounts.get(pid);

      if (!dateMap.has(date)) {
        dateMap.set(date, new Map());
      }

      const eventMap = dateMap.get(date);

      const key = `${eventType}__${eventName}`;

      eventMap.set(key, (eventMap.get(key) || 0) + inc);
    };
    const incIfPidDate = (map, pid, date, inc = 1) => {
      if (!pid || !date) return;
      if (!map.has(pid)) map.set(pid, new Map());
      const inner = map.get(pid);
      inner.set(date, (inner.get(date) || 0) + inc);
    };

    // Track which files were uploaded
    const uploadedMetricNames = new Set();
    // =========================================
    // ADD BEFORE streamFileRows LOOP
    // =========================================

    const rawDateStore = {
      install: new Map(), // pid -> date -> raw install time
      event: new Map(), // pid -> date -> raw event time
      clicks: new Map(), // pid -> date -> clicks date
    };

    function setNestedDate(map, pid, date, value) {
      if (!map.has(pid)) map.set(pid, new Map());

      const inner = map.get(pid);

      // store first value only
      if (!inner.has(date) || !inner.get(date)) {
        inner.set(date, value);
      }
    }
    const [configRows] = await pool.query(
      `
  SELECT events
  FROM campaign_configs
  WHERE (
    campaign_name = ? 
    OR JSON_CONTAINS(campaign_name, JSON_ARRAY(?))
  )
  AND (os = ? OR os IS NULL OR os = '')
  LIMIT 1;
  `,
      [fullCampaignName, fullCampaignName, os], // ✅ FIX
    );

    let allowedEvents = [];

    if (configRows.length && configRows[0].events) {
      let rawEvents = configRows[0].events;

      // Handle Buffer
      if (Buffer.isBuffer(rawEvents)) {
        rawEvents = rawEvents.toString("utf8");
      }

      // Already array
      if (Array.isArray(rawEvents)) {
        allowedEvents = rawEvents;
      }

      // Stringified JSON
      else if (typeof rawEvents === "string") {
        try {
          const parsed = JSON.parse(rawEvents);

          if (Array.isArray(parsed)) {
            allowedEvents = parsed;
          } else {
            allowedEvents = rawEvents.split(",");
          }
        } catch (e) {
          allowedEvents = rawEvents.split(",");
        }
      }
    }

    allowedEvents = allowedEvents
      .map((e) =>
        String(e)
          .trim()
          .replace(/^"+|"+$/g, "") // remove accidental quotes
          .toLowerCase(),
      )
      .filter(Boolean);

    console.log("✅ Allowed Events:", allowedEvents);
    // ---------------------------
    // Process uploaded files
    // ---------------------------
    for (const file of uploaded) {
      // 🔹 Normalize helper
      const normalize = (str) => str.toLowerCase().replace(/[\s\-_]/g, "");

      const sortedKeys = Object.keys(FILE_MAP).sort(
        (a, b) => b.length - a.length,
      );
      const fileNameNorm = normalize(file.originalname);

      const key = sortedKeys.find((k) => fileNameNorm.includes(normalize(k)));

      if (key) {
        const metricName = FILE_MAP[key];
        uploadedMetricNames.add(metricName);
        console.log(
          `📂 Processing file: ${file.originalname} → matched key="${key}" → metric=${metricName}`,
        );
      } else {
        console.log(
          `❌ No key matched for: ${file.originalname} (normalized=${fileNameNorm})`,
        );
      }

      const metricName = FILE_MAP[key];
      uploadedMetricNames.add(metricName);
      console.log(
        `📂 Processing file: ${file.originalname} → matched key="${key}" → metric=${metricName}`,
      );
      await streamFileRows(file.path, async (row, headers) => {
        const mediaSourceKey = headers.find((c) =>
          /media[-_\s]?source/i.test(c),
        );
        const sourceVal = mediaSourceKey ? row[mediaSourceKey] : null;
        if (!sourceVal) return;
        const pid = String(sourceVal).trim().toLowerCase();
        // ✅ Add this line here
        filePids.add(pid);
        const metricsDate = extractDate(row, headers, metricName);
        if (!metricsDate) return;

        // check once if installs file is present
        // Detect presence of key files once
        const hasNoiFile = uploaded.some((f) =>
          f.originalname.toLowerCase().includes("installs"),
        );

        if (metricName === "clicks") {
          // ---- Normalize headers once ----
          const normalizeHeader = (s) =>
            s
              .toString()
              .toLowerCase()
              .replace(/[\s\-]/g, ""); // remove spaces, dashes, underscores
          const headers = Object.keys(row);
          const normHeaders = headers.map((h) => normalizeHeader(h));

          // ---- Normal clicks ----
          const clicksIdx = normHeaders.findIndex(
            (c) => c === "clicks" || c === "click",
          );
          if (clicksIdx !== -1) {
            const clicksKey = headers[clicksIdx]; // use original header to access row value
            const clicksVal =
              Number((row[clicksKey] || "").toString().replace(/,/g, "")) || 0;
            incIfPidDate(metricCounts.clicks, pid, metricsDate, clicksVal);
          } else {
            incIfPidDate(metricCounts.clicks, pid, metricsDate, 0);
          }
          setNestedDate(rawDateStore.clicks, pid, metricsDate, metricsDate);
          // ---- Fallback NOI (if installs file not uploaded, get from installs appsflyer column in clicks) ----
          if (!uploadedMetricNames.has("noi")) {
            const noiIdx = normHeaders.findIndex((c) =>
              c.includes("installsappsflyer"),
            );
            if (noiIdx !== -1) {
              const noiKey = headers[noiIdx];
              const noiVal =
                Number((row[noiKey] || "").toString().replace(/,/g, "")) || 0;
              incIfPidDate(metricCounts.noi, pid, metricsDate, noiVal);
              console.log(
                `📊 [Fallback NOI] PID=${pid}, date=${metricsDate}, val=${noiVal}`,
              );
            }
          }

          // ---- Fallback NOE: SUM all columns starting with
          // "uniqueusersltvdayscumulativeappsflyer"
          // ----
          // if (!uploadedMetricNames.has("noe")) {
          //   let noeSum = 0;

          //   normHeaders.forEach((normHeader, idx) => {
          //     if (
          //       normHeader.startsWith("uniqueusersltvdayscumulativeappsflyer")
          //     ) {
          //       const originalKey = headers[idx];
          //       const val =
          //         Number(
          //           (row[originalKey] || "").toString().replace(/,/g, ""),
          //         ) || 0;
          //       noeSum += val;
          //     }
          //   });

          //   if (noeSum > 0) {
          //     incIfPidDate(metricCounts.noe, pid, metricsDate, noeSum);
          //     console.log(
          //       `📊 [Fallback NOE SUM] PID=${pid}, date=${metricsDate}, noe=${noeSum}`,
          //     );
          //   }
          // }
          // ---- Fallback NOE from Clicks File Dynamic Event Columns ----
          if (!uploadedMetricNames.has("noe")) {
            const PREFIX = "uniqueusersltvdayscumulativeappsflyer";

            let totalNoe = 0;

            normHeaders.forEach((normHeader, idx) => {
              // Match columns starting with:
              // uniqueusersltvdayscumulativeappsflyer
              if (normHeader.startsWith(PREFIX)) {
                const originalKey = headers[idx];

                // Extract event name after prefix
                let extractedEvent = normHeader.replace(PREFIX, "");

                extractedEvent = extractedEvent
                  .replace(/^[_\-\s]+/, "")
                  .trim()
                  .toLowerCase();

                // skip if no event name
                if (!extractedEvent) return;

                // Match only allowed campaign config events
                if (!allowedEvents.includes(extractedEvent)) {
                  return;
                }

                const val =
                  Number(
                    (row[originalKey] || "").toString().replace(/,/g, ""),
                  ) || 0;

                if (val <= 0) return;

                // Total NOE
                totalNoe += val;

                // Store event-wise NOE
                incEventCount(pid, metricsDate, extractedEvent, "noe", val);

                console.log(
                  `📊 [Fallback Clicks Event] PID=${pid}, date=${metricsDate}, event=${extractedEvent}, val=${val}`,
                );
              }
            });

            // store overall NOE metric
            if (totalNoe > 0) {
              incIfPidDate(metricCounts.noe, pid, metricsDate, totalNoe);

              console.log(
                `📊 [Fallback NOE TOTAL] PID=${pid}, date=${metricsDate}, total=${totalNoe}`,
              );
            }
          }
          return;
        }

        if (metricName === "noi") {
          // Use Media Source + Install Time
          const mediaSourceKey = headers.find((c) => /media\s*source/i.test(c));
          const installTimeKey = headers.find((c) => c === "installtime");

          const pid = mediaSourceKey
            ? (row[mediaSourceKey] || "").trim().toLowerCase()
            : null;
          if (!pid) return;

          const installTimeRaw = installTimeKey ? row[installTimeKey] : null;
          const dateVal = installTimeRaw ? new Date(installTimeRaw) : null;
          if (!dateVal || isNaN(dateVal)) return;

          const metricsDate = `${dateVal.getFullYear()}-${String(
            dateVal.getMonth() + 1,
          ).padStart(2, "0")}-${String(dateVal.getDate()).padStart(2, "0")}`;

          // Count 1 install per row
          incIfPidDate(metricCounts.noi, pid, metricsDate, 1);
          setNestedDate(rawDateStore.install, pid, metricsDate, installTimeRaw);
          // Debug log
          console.log(`📊 [NOI] PID=${pid}, date=${metricsDate}, +1`);
          return;
        }

        // if (metricName === "pe" || metricName === "noe") {
        //   const evKey = headers.find((c) => /event[_\s]?name/i.test(c));
        //   if (!eventCountsByPidDate.has(pid))
        //     eventCountsByPidDate.set(pid, new Map());
        //   const pidDateMap = eventCountsByPidDate.get(pid);
        //   const dateMap = pidDateMap.get(metricsDate) || new Map();
        //   if (evKey) {
        //     const ev = String(row[evKey] || "")
        //       .trim()
        //       .toLowerCase();
        //     dateMap.set(ev, (dateMap.get(ev) || 0) + 1);
        //   }
        //   pidDateMap.set(metricsDate, dateMap);
        //   const eventTimeKey = headers.find((c) => /event\s*time/i.test(c));

        //   const rawEventTime = eventTimeKey ? row[eventTimeKey] : null;

        //   setNestedDate(rawDateStore.event, pid, metricsDate, rawEventTime);
        //   incIfPidDate(metricCounts[metricName], pid, metricsDate, 1);
        //   return;
        // }
        if (metricName === "pe" || metricName === "noe") {
          const evKey = headers.find((c) => /event[_\s]?name/i.test(c));

          const eventName = evKey
            ? String(row[evKey] || "")
                .trim()
                .toLowerCase()
            : null;

          // store raw event counts
          if (!eventCountsByPidDate.has(pid)) {
            eventCountsByPidDate.set(pid, new Map());
          }

          const pidDateMap = eventCountsByPidDate.get(pid);

          if (!pidDateMap.has(metricsDate)) {
            pidDateMap.set(metricsDate, new Map());
          }

          const dateMap = pidDateMap.get(metricsDate);

          if (eventName) {
            dateMap.set(eventName, (dateMap.get(eventName) || 0) + 1);
          }

          // store raw event time
          const eventTimeKey = headers.find((c) => /event\s*time/i.test(c));

          const rawEventTime = eventTimeKey
            ? formatEventDateTime(row[eventTimeKey])
            : null;
          console.log(
            `📅 [${metricName.toUpperCase()}] Extracted event time for PID=${pid}, date=${metricsDate} → raw="${row[eventTimeKey]}" → formatted="${rawEventTime}"`,
          );
          setNestedDate(rawDateStore.event, pid, metricsDate, rawEventTime);

          // =========================
          // NOE LOGIC
          // =========================
          if (
            metricName === "noe" &&
            eventName &&
            allowedEvents.includes(eventName)
          ) {
            // total NOE count
            incIfPidDate(metricCounts.noe, pid, metricsDate, 1);

            // event-wise NOE count
            incEventCount(pid, metricsDate, eventName, "noe", 1);
          }

          // =========================
          // PE LOGIC
          // =========================
          if (
            metricName === "pe" &&
            eventName &&
            allowedEvents.includes(eventName)
          ) {
            // total PE count
            incIfPidDate(metricCounts.pe, pid, metricsDate, 1);

            // event-wise PE count
            incEventCount(pid, metricsDate, eventName, "pe", 1);
          }

          return;
        }
        // default
        incIfPidDate(metricCounts[metricName], pid, metricsDate, 1);
      });
    }
    // ✅ Step 5: Merge 30-day advData only for PIDs that exist in uploaded files
    const mergedAdvData = [...advData];
    for (const prev of advDataPrev30) {
      if (filePids.has(prev.pidLower) && !advPidMap.has(prev.pidLower)) {
        mergedAdvData.push(prev);
        advPidMap.set(prev.pidLower, prev);
      }
    }

    // ✅ Step 6: Add placeholder "N/A" entries for PIDs present in files but missing in both adv_data sets
    for (const pid of filePids) {
      if (!advPidMap.has(pid)) {
        mergedAdvData.push({
          pid,
          pidLower: pid,
          pubid: "N/A",
          pubam: "N/A",
          campaign_id: null,
          shared_date: null,
          campaign_name: baseCampaignName,
          pause: 0,
          nocrm: 0,
          os,
        });
      }
    }

    console.log(
      `📊 Total adv_data combined (main + 30-day): ${mergedAdvData.length}`,
    );

    // ✅ Use mergedAdvData instead of advData going forward

    // ... rest of your DB insert logic unchanged ...

    // ... rest of your DB insert logic unchanged ...

    // ---------------------------
    // Prepare DB inserts (per pid + date)
    // ---------------------------
    const metricsData = [];

    for (const d of mergedAdvData) {
      const pidLower = d.pidLower;

      // Collect all dates where this PID has any metric OR events
      const allDates = new Set();

      for (const map of Object.values(metricCounts)) {
        if (map.has(pidLower)) {
          for (const date of map.get(pidLower).keys()) {
            allDates.add(date);
          }
        }
      }

      // Also include dates from events map
      if (eventCountsByPidDate.has(pidLower)) {
        for (const date of eventCountsByPidDate.get(pidLower).keys()) {
          allDates.add(date);
        }
      }

      // If no dates found, still insert one zero row for startDate (baseline)
      if (allDates.size === 0) {
        allDates.add(startDate); // ensures pid stored even with 0 data
      }

      // loop through all dates for this pid
      for (const date of allDates) {
        const metrics = {
          noi: metricCounts.noi.get(pidLower)?.get(date) || 0,
          rti: metricCounts.rti.get(pidLower)?.get(date) || 0,
          pe: metricCounts.pe.get(pidLower)?.get(date) || 0,
          pi: metricCounts.pi.get(pidLower)?.get(date) || 0,
          noe: metricCounts.noe.get(pidLower)?.get(date) || 0,
          clicks: metricCounts.clicks.get(pidLower)?.get(date) || 0,
        };

        metricsData.push([
          fullCampaignName,
          d.campaign_id,
          d.shared_date,
          os,
          geo,
          date,
          d.pubam,
          d.pid,
          d.pubid,
          metrics.noi,
          metrics.rti,
          metrics.pe,
          metrics.pi,
          metrics.noe,
          metrics.clicks,
          d.pause,
          d.nocrm,

          rawDateStore.install.get(pidLower)?.get(date) || null,
          rawDateStore.event.get(pidLower)?.get(date) || null,
          rawDateStore.clicks.get(pidLower)?.get(date) || null,
        ]);
      }
    }

    // ---------------------------
    // Insert into DB
    // ---------------------------
    const sqlMetrics = `
  INSERT INTO campaign_metrics_new
  (
    campaign_name,
    campaign_id,
    shared_date,
    os,
    geo,
    metrics_date,
    pubam,
    pid,
    pubid,
    noi,
    rti,
    pe,
    pi,
    noe,
    clicks,
    is_paused,
    nocrm,

    install_time,
    event_time,
    clicks_date
  )
  VALUES ?
  ON DUPLICATE KEY UPDATE
    noi = VALUES(noi),
    rti = VALUES(rti),
    pe = VALUES(pe),
    pi = VALUES(pi),
    noe = VALUES(noe),
    clicks = VALUES(clicks),
    install_time = VALUES(install_time),
    event_time = VALUES(event_time),
    clicks_date = VALUES(clicks_date),
    is_paused = VALUES(is_paused),
    nocrm = VALUES(nocrm),
    shared_date = VALUES(shared_date)
`;
    await batchInsert(sqlMetrics, metricsData, 500);
    // 🔥 Build mapping: (pid + date) → campaign_metrics_id
    const [cmRows] = await pool.query(
      `
SELECT id, campaign_name, pid, metrics_date, os
FROM campaign_metrics_new
WHERE campaign_name = ? AND os = ?
`,
      [fullCampaignName],
    );

    const cmMap = new Map();

    for (const row of cmRows) {
      const key = `${row.campaign_name}_${String(row.pid).trim().toLowerCase()}_${row.metrics_date}_${row.os}`;
      cmMap.set(key, row.id);
    }
    const eventData = [];

    for (const d of mergedAdvData) {
      const pidLower = d.pidLower;

      const pidDateMap = eventTypeCounts.get(pidLower);

      if (!pidDateMap) continue;

      for (const [date, eventMap] of pidDateMap.entries()) {
        for (const [compoundKey, count] of eventMap.entries()) {
          const [eventType, eventName] = compoundKey.split("__");

          const key = `${fullCampaignName}_${String(d.pid).trim().toLowerCase()}_${date}_${d.os}`;

          const campaignMetricsId = cmMap.get(key) || null;

          eventData.push([
            d.pid,
            date,
            eventName,
            count,
            eventType,
            campaignMetricsId,
          ]);
        }
      }
    }
    if (eventData.length > 0) {
      const sqlEvents = `
      INSERT INTO campaign_event_metrics_new
(pid, metrics_date, event_name, count, event_type, campaign_metrics_id)
VALUES ?
        ON DUPLICATE KEY UPDATE count = VALUES(count), event_type = VALUES(event_type)
      `;
      await batchInsert(sqlEvents, eventData, 500);
    }
    // Emit socket event if socketId provided
    const io = req.app.get("io");
    if (socketId && io && io.sockets && io.sockets.sockets.get(socketId)) {
      io.to(socketId).emit("uploadComplete", {
        status: "success",
        message: "Upload successful",
        campaignName,
        dateRange,
        rowsInserted: metricsData.length,
      });
      console.log("✅ Emitted uploadComplete to:", socketId);
    } else {
      console.log("⚠️ Socket not connected or invalid:", socketId);
    }

    res
      .status(200)
      .json({ msg: "Upload successful", rowsInserted: metricsData.length });
    // res
    //   .status(200)
    //   .json({ msg: "Upload successful", rowsInserted: metricsData.length });
  } catch (err) {
    console.error("❌ Error in handleUpload:", err);
    res.status(500).json({ msg: "Server error", error: err.message });
  } finally {
    let uploaded = [];
    if (Array.isArray(req.files)) uploaded = req.files;
    else if (req.files) uploaded = Object.values(req.files).flat();
    else if (req.file) uploaded = [req.file];

    uploaded.forEach((f) => {
      try {
        fs.unlinkSync(f.path);
      } catch (e) {
        console.warn("⚠️ Failed to delete file:", f.path);
      }
    });
  }
};

module.exports = { handleUpload };
