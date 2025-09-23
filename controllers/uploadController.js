// optimizedUploadHandler.js
const ExcelJS = require("exceljs");
const fs = require("fs");
const path = require("path");
const csv = require("fast-csv");
const pool = require("../config/db");

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
        (h || "").toString().replace(/\s+/g, "").toLowerCase()
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

// Convert Excel serial -> JS Date (Excel epoch 1899-12-30)
function excelSerialToDate(serial) {
  // handle negative/invalid serials gracefully
  if (typeof serial !== "number" || isNaN(serial)) return null;
  const epoch = Date.UTC(1899, 11, 30);
  const ms = Math.round(serial * 24 * 60 * 60 * 1000);
  return new Date(epoch + ms);
}

// Robust normalization for a raw date value -> 'YYYY-MM-DD' or null
function normalizeDateRaw(raw) {
  if (raw == null || raw === "") return null;

  // If already a Date object
  if (raw instanceof Date) {
    if (isNaN(raw)) return null;
    return raw.toISOString().split("T")[0];
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
    if (d && !isNaN(d)) return d.toISOString().split("T")[0];
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
  if (!isNaN(parsed)) return parsed.toISOString().split("T")[0];

  // Try dd/mm/yy or dd/mm/yyyy
  const m1 = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
  if (m1) {
    let day = parseInt(m1[1], 10);
    let month = parseInt(m1[2], 10);
    let year = parseInt(m1[3], 10);
    if (year < 100) year += 2000;
    const d = new Date(year, month - 1, day);
    if (!isNaN(d)) return d.toISOString().split("T")[0];
  }

  // Try yyyy/mm/dd
  const m2 = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m2) {
    const d = new Date(parseInt(m2[1], 10), parseInt(m2[2], 10) - 1, parseInt(m2[3], 10));
    if (!isNaN(d)) return d.toISOString().split("T")[0];
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
    const iso = value.toISOString().split("T")[0];
    console.log(`📅 [${metricName}] Parsed Date object → ${iso} (PID=${pid})`);
    return iso;
  }

  // Case 2: Excel serial number
  if (!isNaN(value) && Number(value) > 30000) {
    const base = new Date(1899, 11, 30);
    const dt = new Date(base.getTime() + (Number(value) * 86400 * 1000));
    const iso = dt.toISOString().split("T")[0];
    console.log(`📅 [${metricName}] Parsed Excel serial=${value} → ${iso} (PID=${pid})`);
    return iso;
  }

  // Case 3: String values
  if (typeof value === "string") {
    let cleaned = value.trim();

    // YYYY-MM-DD
    let m = cleaned.match(/^(\d{4})[-/](\d{2})[-/](\d{2})$/);
    if (m) {
      const iso = `${m[1]}-${m[2]}-${m[3]}`;
      console.log(`📅 [${metricName}] Parsed YYYY-MM-DD='${cleaned}' → ${iso} (PID=${pid})`);
      return iso;
    }

    // DD/MM/YYYY
    m = cleaned.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
    if (m) {
      const day = m[1].padStart(2, "0");
      const month = m[2].padStart(2, "0");
      const iso = `${m[3]}-${month}-${day}`;
      console.log(`📅 [${metricName}] Parsed DD/MM/YYYY='${cleaned}' → ${iso} (PID=${pid})`);
      return iso;
    }

    // fallback
    const dt = new Date(cleaned);
    if (!isNaN(dt)) {
      const iso = dt.toISOString().split("T")[0];
      console.log(`📅 [${metricName}] JS fallback parsed '${cleaned}' → ${iso} (PID=${pid})`);
      return iso;
    }
  }

  console.log(`❌ [${metricName}] Could not parse date='${value}' (PID=${pid})`);
  return null;
}
function extractDate(row, headers, metricName) {
  let rawDate = null;

  if (metricName === "noi" || metricName === "pi" || metricName === "rti") {
    const key = headers.find(c => c.toLowerCase().includes("installtime"));
    rawDate = key ? row[key] : null;
  } else if (metricName === "noe" || metricName === "pe") {
    const key = headers.find(c => c.toLowerCase().includes("eventtime"));
    rawDate = key ? row[key] : null;
  } else if (metricName === "clicks") {
    const key = headers.find(c => c.toLowerCase() === "date");
    rawDate = key ? row[key] : null;
  }

  if (!rawDate) {
    console.log(`⚠️ No date found for metric=${metricName}, row=`, row);
    return null;
  }

  // Handle string formats like "2025-09-14 15:53:15" or "2025-09-07"
  if (typeof rawDate === "string") {
    const dateOnly = rawDate.trim().substring(0, 10);
    // console.log(`📅 Extracted date=${dateOnly} for metric=${metricName}, raw=${rawDate}`);
    return dateOnly;
  }

  // Fallback: if Excel parser gave a Date object
  const d = new Date(rawDate);
  if (isNaN(d)) {
    console.log(`⚠️ Invalid date format metric=${metricName}, value=${rawDate}`);
    return null;
  }

  const formatted = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  console.log(`📅 Extracted date=${formatted} (from Date object) for metric=${metricName}, raw=${rawDate}`);
  return formatted;
}




// ---------------------------
// Fetch adv_data by campaign + dateRange
// ---------------------------
async function getAdvDataFromDB(campaignName, startDate, endDate, os) {
  const [rows] = await pool.query(
    `SELECT pid, pub_id, pub_name, campaign_name, paused_date, flag, os
     FROM adv_data
     WHERE campaign_name = ?
       AND shared_date BETWEEN ? AND ?
       AND os = ?`,
    [campaignName, startDate, endDate, os]
  );

  const pidMap = new Map();
  for (const r of rows) {
    const pidKey = String(r.pid || "").trim().toLowerCase();
    if (!pidMap.has(pidKey)) {
      pidMap.set(pidKey, {
        pid: String(r.pid || "").trim(),
        pidLower: pidKey,
        pubid: String(r.pub_id || "").trim(),
        pubam: String(r.pub_name || "").trim(),
        campaign_name: r.campaign_name,
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
    const { campaignName, os, geo, dateRange } = req.body;
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
    const advData = await getAdvDataFromDB(baseCampaignName, startDate, endDate, os);
    if (!advData.length)
      return res.status(400).json({ msg: "No valid PID entries found in adv_data." });

    const metricCounts = {
      noi: new Map(),
      rti: new Map(),
      pe: new Map(),
      pi: new Map(),
      noe: new Map(),
      clicks: new Map(),
    };
    const eventCountsByPidDate = new Map();

    const incIfPidDate = (map, pid, date, inc = 1) => {
      if (!pid || !date) return;
      if (!map.has(pid)) map.set(pid, new Map());
      const inner = map.get(pid);
      inner.set(date, (inner.get(date) || 0) + inc);
    };

    // Track which files were uploaded
    const uploadedMetricNames = new Set();

    // ---------------------------
    // Process uploaded files
    // ---------------------------
    for (const file of uploaded) {
     // 🔹 Normalize helper
     const normalize = (str) => str.toLowerCase().replace(/[\s\-_]/g, "");

     const sortedKeys = Object.keys(FILE_MAP).sort((a, b) => b.length - a.length);
     const fileNameNorm = normalize(file.originalname);
     
     const key = sortedKeys.find((k) =>
       fileNameNorm.includes(normalize(k))
     );
     
     if (key) {
       const metricName = FILE_MAP[key];
       uploadedMetricNames.add(metricName);
       console.log(`📂 Processing file: ${file.originalname} → matched key="${key}" → metric=${metricName}`);
     } else {
       console.log(`❌ No key matched for: ${file.originalname} (normalized=${fileNameNorm})`);
     }
     

const metricName = FILE_MAP[key];
uploadedMetricNames.add(metricName);
console.log(`📂 Processing file: ${file.originalname} → matched key="${key}" → metric=${metricName}`);

      await streamFileRows(file.path, async (row, headers) => {
        const mediaSourceKey = headers.find((c) => /media[-_\s]?source/i.test(c));
        const sourceVal = mediaSourceKey ? row[mediaSourceKey] : null;
        if (!sourceVal) return;
        const pid = String(sourceVal).trim().toLowerCase();

        const metricsDate = extractDate(row, headers, metricName);
        if (!metricsDate) return;


        // check once if installs file is present
// Detect presence of key files once
const hasNoiFile = uploaded.some(f =>
  f.originalname.toLowerCase().includes("installs")
);
// const hasNoeFile = uploaded.some(f =>
//   f.originalname.toLowerCase().includes("in-app-event")
// );

if (metricName === "clicks") {
  // normal clicks
  const clicksKey = headers.find((c) => /clicks?/i.test(c));
  const clicksVal = clicksKey
    ? Number((row[clicksKey] || "").toString().replace(/,/g, "")) || 0
    : 0;
  incIfPidDate(metricCounts.clicks, pid, metricsDate, clicksVal);

  // ✅ fallback NOI only if NO installs file uploaded at all
  if (!hasNoiFile) {
    const noiKey = headers.find((c) => /installsappsflyer/i.test(c));
    if (noiKey) {
      const noiVal =
        Number((row[noiKey] || "").toString().replace(/,/g, "")) || 0;
      incIfPidDate(metricCounts.noi, pid, metricsDate, noiVal);
      console.log(
        `📊 [Fallback NOI] PID=${pid}, date=${metricsDate}, val=${noiVal}`
      );
    }
  }

  // Normalize headers (lowercase, remove spaces/dashes/underscores)
  const normalize = (str) =>
    str.toLowerCase().replace(/[\s\-_]/g, "");
  const normHeaders = headers.map((h) => normalize(h));

  // ✅ fallback NOE only if NO in-app-event file uploaded
  // if (!hasNoeFile) {
  //   const idx = normHeaders.findIndex((h) =>
  //     h.includes("uniqueusersltvdayscumulativeappsflyer")
  //   );
  //   if (idx !== -1) {
  //     const noeKey = headers[idx];
  //     const noeVal =
  //       Number((row[noeKey] || "").toString().replace(/,/g, "")) || 0;
  //     if (noeVal > 0) {
  //       incIfPidDate(metricCounts.noe, pid, metricsDate, noeVal);
  //       console.log(
  //         `📊 [Fallback NOE] PID=${pid}, date=${metricsDate}, key=${noeKey}, value=${noeVal}`
  //       );
  //     } else {
  //       console.log(
  //         `⚠️ NOE fallback header found (${noeKey}) but value empty for PID=${pid}`
  //       );
  //     }
  //   } else {
  //     console.log("❌ No NOE fallback header found in clicks file");
  //   }
  // }

  return;
}


  //       if (metricName === "clicks") {
  //         // normal clicks
  //         const clicksKey = headers.find((c) => /clicks?/i.test(c));
  //         const clicksVal = clicksKey
  //           ? Number((row[clicksKey] || "").toString().replace(/,/g, "")) || 0
  //           : 0;
  //         incIfPidDate(metricCounts.clicks, pid, metricsDate, clicksVal);

  //         // fallback NOI if installs file missing
  //         if (!uploadedMetricNames.has("noi")) {
  //           const noiKey = headers.find((c) =>
  //             /installsappsflyer/i.test(c)
  //           );
  //           if (noiKey) {
  //             const noiVal = Number((row[noiKey] || "").toString().replace(/,/g, "")) || 0;
  //             incIfPidDate(metricCounts.noi, pid, metricsDate, noiVal);
  //             // console.log(`📊 [Fallback NOI] PID=${pid}, date=${metricsDate}, val=${noiVal}`);
  //           }
  //         }
  //          // Normalize headers (lowercase, remove spaces/dashes/underscores)
  // const normalize = (str) =>
  //   str.toLowerCase().replace(/[\s\-_]/g, "");

  // const normHeaders = headers.map((h) => normalize(h));


  //                 // ✅ Fallback NOE (unique-users ltv days cumulative appsflyer)
  // if (!uploaded.some((f) => f.originalname.toLowerCase().includes("in-app-event"))) {
  //   const idx = normHeaders.findIndex((h) =>
  //     h.includes("uniqueusersltvdayscumulativeappsflyer")
  //   );
  //   if (idx !== -1) {
  //     const noeKey = headers[idx];
  //     const noeVal =
  //       Number((row[noeKey] || "").toString().replace(/,/g, "")) || 0;
  //     if (noeVal > 0) {
  //       incIfPidDate(metricCounts.noe, pid, metricsDate, noeVal);
  //       console.log(
  //         `📊 [Fallback NOE] PID=${pid}, date=${metricsDate}, key=${noeKey}, value=${noeVal}`
  //       );
  //     } else {
  //       console.log(
  //         `⚠️ NOE fallback header found (${noeKey}) but value empty for PID=${pid}`
  //       );
  //     }
  //   } else {
  //     console.log("❌ No NOE fallback header found in clicks file");
  //   }
  // }

  //         return;
  //       }



        if (metricName === "noi") {
          // Use Media Source + Install Time
          const mediaSourceKey = headers.find((c) => /media\s*source/i.test(c));
          const installTimeKey = headers.find((c) => /install\s*time/i.test(c));
        
          const pid = mediaSourceKey ? (row[mediaSourceKey] || "").trim().toLowerCase() : null;
          if (!pid) return;
        
          const installTimeRaw = installTimeKey ? row[installTimeKey] : null;
          const dateVal = installTimeRaw ? new Date(installTimeRaw) : null;
          if (!dateVal || isNaN(dateVal)) return;
        
          const metricsDate = dateVal.toISOString().split("T")[0];
        
          // Count 1 install per row
          incIfPidDate(metricCounts.noi, pid, metricsDate, 1);
        
          // Debug log
          console.log(`📊 [NOI] PID=${pid}, date=${metricsDate}, +1`);
          return;
        }

        
        if (metricName === "pe" || metricName === "noe") { 
          const evKey = headers.find((c) => /event[_\s]?name/i.test(c)); if (!eventCountsByPidDate.has(pid)) 
            eventCountsByPidDate.set(pid, new Map()); const pidDateMap = eventCountsByPidDate.get(pid); 
          const dateMap = pidDateMap.get(metricsDate) || new Map(); if (evKey) {
             const ev = String(row[evKey] || "").trim().toLowerCase(); dateMap.set(ev, (dateMap.get(ev) || 0) + 1); } 
          pidDateMap.set(metricsDate, dateMap); 
          incIfPidDate(metricCounts[metricName], pid, metricsDate, 1); return; }
        // if (metricName === "pe" || metricName === "noe") {
        //   // Find event time column
        //   const dateKey = headers.find((c) => /event[_\s]?time/i.test(c));
        //   let rawDate = dateKey ? row[dateKey] : null;
        
        //   // Normalize to YYYY-MM-DD
        //   const metricsDate = normalizeDateRaw(rawDate);
        //   if (!metricsDate) return; // skip if invalid
        
        //   // Find event name and count
        //   const evKey = headers.find((c) => /event[_\s]?name/i.test(c));
        //   const countKey =
        //     headers.find((c) => /event[_\s]?count/i.test(c)) ||
        //     headers.find((c) => /uniqueusers/i.test(c));
        
        //   if (!eventCountsByPidDate.has(pid)) eventCountsByPidDate.set(pid, new Map());
        //   const pidDateMap = eventCountsByPidDate.get(pid);
        //   const dateMap = pidDateMap.get(metricsDate) || new Map();
        
        //   if (evKey) {
        //     const ev = String(row[evKey] || "").trim().toLowerCase();
        //     const evCount = countKey
        //       ? Number((row[countKey] || "").toString().replace(/,/g, "")) || 0
        //       : 1;
        
        //     dateMap.set(ev, (dateMap.get(ev) || 0) + evCount);
        //     incIfPidDate(metricCounts[metricName], pid, metricsDate, evCount);
        
        //     console.log(
        //       `📊 [${metricName.toUpperCase()}] PID=${pid}, date=${metricsDate}, event=${ev}, count=${evCount}`
        //     );
        //   }
        
        //   pidDateMap.set(metricsDate, dateMap);
        //   return;
        // }
        
        // if (metricName === "pe" || metricName === "noe") {
        //      // Find event time column
        //   const dateKey = headers.find((c) => /event[_\s]?time/i.test(c));
        //   let rawDate = dateKey ? row[dateKey] : null;
        
        //   // Normalize to YYYY-MM-DD
        //   const metricsDate = normalizeDateRaw(rawDate);
        //   if (!metricsDate) return; // skip if invalid
        
        //   // Find event name and count
        //   const evKey = headers.find((c) => /event[_\s]?name/i.test(c));
        //   const countKey =
        //     headers.find((c) => /event[_\s]?count/i.test(c)) ||
        //     headers.find((c) => /uniqueusers/i.test(c));
            
        //   if (!eventCountsByPidDate.has(pid)) eventCountsByPidDate.set(pid, new Map());
        //   const pidDateMap = eventCountsByPidDate.get(pid);
        //   const dateMap = pidDateMap.get(metricsDate) || new Map();
        
        //   if (evKey) {
        //     const ev = String(row[evKey] || "").trim().toLowerCase();
        //     const evCount = countKey
        //       ? Number((row[countKey] || "").toString().replace(/,/g, "")) || 0
        //       : 1; // fallback
        
        //     dateMap.set(ev, (dateMap.get(ev) || 0) + evCount);
        //     incIfPidDate(metricCounts[metricName], pid, metricsDate, evCount);
        
        //     console.log(
        //       `📊 [${metricName.toUpperCase()}] PID=${pid}, date=${metricsDate}, event=${ev}, count=${evCount}`
        //     );
        //   }
        
        //   pidDateMap.set(metricsDate, dateMap);
        //   return;
        // }
        


        // default
        // incIfPidDate(metricCounts[metricName], pid, metricsDate, 1);
      });
    }

    // ... rest of your DB insert logic unchanged ...


    // ... rest of your DB insert logic unchanged ...


    // ---------------------------
    // Prepare DB inserts (per pid + date)
    // ---------------------------
    const metricsData = [];
    const eventData = [];

    for (const d of advData) {
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

      // If no dates found for this pid, skip
      if (allDates.size === 0) continue;

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
        ]);

        const pidEvents = eventCountsByPidDate.get(pidLower)?.get(date) || new Map();
        for (const [eventName, count] of pidEvents.entries()) {
          // store with metrics_date in events table (so it's also date keyed)
          eventData.push([d.pid, date, eventName, count, "event"]);
        }
      }
    }

    // ---------------------------
    // Insert into DB
    // ---------------------------
    const sqlMetrics = `
      INSERT INTO campaign_metrics
      (campaign_name, os, geo, metrics_date, pubam, pid, pubid,
       noi, rti, pe, pi, noe, clicks, is_paused, nocrm)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        noi = VALUES(noi),
        rti = VALUES(rti),
        pe = VALUES(pe),
        pi = VALUES(pi),
        noe = VALUES(noe),
        clicks = VALUES(clicks),
        is_paused = VALUES(is_paused),
        nocrm = VALUES(nocrm)
    `;
    await batchInsert(sqlMetrics, metricsData, 500);

    if (eventData.length > 0) {
      const sqlEvents = `
        INSERT INTO campaign_event_metrics (pid, metrics_date, event_name, count, event_type)
        VALUES ?
        ON DUPLICATE KEY UPDATE count = VALUES(count), event_type = VALUES(event_type)
      `;
      await batchInsert(sqlEvents, eventData, 500);
    }

    res.status(200).json({ msg: "Upload successful", rowsInserted: metricsData.length });
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
