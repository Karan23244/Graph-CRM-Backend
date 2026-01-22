// optimizedUploadHandler.js
const ExcelJS = require("exceljs");
const fs = require("fs");
const path = require("path");
const csv = require("fast-csv");
const pool = require("../config/db");
const iconv = require("iconv-lite");
const detect = require("detect-csv");

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
function excelSerialToDate(serial) {
  // Excel serial starts from Jan 1, 1900
  const excelEpoch = new Date(Date.UTC(1899, 11, 30));
  return new Date(excelEpoch.getTime() + serial * 86400000);
}

function detectDelimiter(filePath) {
  const sample = fs.readFileSync(filePath, "utf8").split("\n")[0];
  return detect(sample) || ",";
}

async function streamXlsxRows(filePath, onRow) {
  try {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(filePath);

    const sheet = workbook.worksheets[0];
    if (!sheet) {
      console.log("❌ XLSX contains no sheets");
      return;
    }

    // read header row
    const headerRow = sheet.getRow(1);
    console.log("Raw Header Values:", headerRow.values);
    const headers = headerRow.values
      .slice(1)
      .map((h) =>
        h ? h.toString().trim().toLowerCase().replace(/\s+/g, "") : "",
      );
    console.log("Processed Headers:", headers);
    // read data rows
    sheet.eachRow({ includeEmpty: false }, async (row, rowNumber) => {
      if (rowNumber === 1) return;

      const rowObj = {};
      row.values.slice(1).forEach((cell, idx) => {
        const key = headers[idx] || `col${idx + 1}`;
        rowObj[key] = cell === undefined || cell === null ? "" : cell;
      });

      try {
        await onRow(rowObj, headers);
      } catch (err) {
        console.log("❌ XLSX row error:", err);
      }
    });
  } catch (err) {
    console.log("❌ XLSX parsing failed:", err.message);
  }
}

function streamCsvRows(filePath, onRow) {
  return new Promise((resolve, reject) => {
    let headerRow = null;

    const stream = fs
      .createReadStream(filePath)
      .pipe(iconv.decodeStream("utf8"))
      .pipe(iconv.encodeStream("utf8"));

    const parser = csv
      .parse({
        headers: (headers) => {
          // Handle corrupted headers like "PK 03 04" (ZIP file signature)
          if (!headers || headers.length === 1) {
            console.log("⚠️ Invalid CSV header detected:", headers);

            // Try to recover: create fake headers
            const fakeHeaders = [];
            for (let i = 0; i < headers.length; i++) {
              fakeHeaders.push(`col${i + 1}`);
            }

            headerRow = fakeHeaders;
            return fakeHeaders;
          }

          // Normalize headers
          const clean = headers.map((h, i) => {
            if (!h || h.includes("pk") || h.includes("\x03")) {
              return `col${i + 1}`; // fallback header
            }
            return String(h).replace(/\s+/g, "").toLowerCase();
          });

          headerRow = clean;
          return clean;
        },
        trim: true,
        ignoreEmpty: true,
      })
      .on("error", (err) => {
        console.log("❌ CSV Parse Error:", err);
        resolve(); // don't crash
      })
      .on("data", async (row) => {
        if (!headerRow) return; // skip broken CSV

        parser.pause();
        await onRow(row, headerRow);
        parser.resume();
      })
      .on("end", () => {
        console.log("✅ CSV processed safely.");
        resolve();
      });

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

function findClosestPidMatch(pid, advPidMap) {
  if (!pid) return null;
  const pidLower = pid.toLowerCase();

  for (const [advPid, data] of advPidMap.entries()) {
    // Bidirectional fuzzy match
    if (advPid.includes(pidLower) || pidLower.includes(advPid)) {
      return data;
    }
  }

  return null;
}

const normalizePid = (val) =>
  String(val || "")
    .toLowerCase()
    .replace(/\s+/g, "") // remove all spaces
    .replace(/[^a-z0-9]/g, "") // remove special chars
    .trim();
// ---------------------------
// Fetch adv_data by campaign + dateRange
// ---------------------------
async function getAdvDataFromDB(campaignName, startDate, endDate, os) {
  console.log("campaignName", campaignName);
  console.log(startDate);
  console.log(endDate);
  console.log(os);
  const [rows] = await pool.query(
    `SELECT pid, pub_id, pub_name, campaign_name, paused_date, flag, os
FROM adv_data
WHERE REPLACE(REPLACE(REPLACE(campaign_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), '') =
      REPLACE(REPLACE(REPLACE(?, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
  AND DATE(shared_date) BETWEEN ? AND ?
  AND os = ?;`,
    [campaignName, startDate, endDate, os],
  );

  const pidMap = new Map();
  for (const r of rows) {
    const pidKey = normalizePid(r.pid);
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

//----------------------------------------------------------
// MAIN UPDATED UPLOAD HANDLER WITH NEW FILE TYPE SUPPORT
//----------------------------------------------------------
const handlesingularUpload = async (req, res) => {
  try {
    const { campaignName, os, geo, dateRange, socketId, event_name } = req.body;
    const fullCampaignName = campaignName;
    const baseCampaignName = campaignName.split(",")[0];

    // Validate dateRange
    const [startDate, endDate] = (dateRange || "")
      .split(" - ")
      .map((d) => d && d.trim());
    if (!startDate || !endDate) {
      return res.status(400).json({ msg: "Invalid dateRange format" });
    }

    // Normalize uploaded files
    let uploaded = [];
    if (Array.isArray(req.files)) uploaded = req.files;
    else if (req.files) uploaded = Object.values(req.files).flat();
    else if (req.file) uploaded = [req.file];

    if (!campaignName || uploaded.length === 0) {
      return res.status(400).json({ msg: "Missing fields or files" });
    }

    console.log("📂 Fetching adv_data for campaign:", baseCampaignName);
    const advData = await getAdvDataFromDB(
      baseCampaignName,
      startDate,
      endDate,
      os,
    );

    // fetch additional 30-days before (same as your previous logic)
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

    // Build advPidMap from advData
    const advPidMap = new Map();
    for (const d of advData) advPidMap.set(d.pidLower, d);

    // Containers for metric counts (per pid -> per date)
    const metricCounts = {
      noi: new Map(), // installs
      rti: new Map(),
      pe: new Map(),
      pi: new Map(),
      noe: new Map(), // event counts (from event_name column)
      clicks: new Map(),
    };

    const eventCountsByPidDate = new Map();
    const filePids = new Set();

    const incIfPidDate = (map, pid, date, inc = 1) => {
      if (!pid || !date) return;
      if (!map.has(pid)) map.set(pid, new Map());
      const inner = map.get(pid);
      inner.set(date, (inner.get(date) || 0) + inc);
    };

    // Helper: parse date from various formats. Returns YYYY-MM-DD or null
    // const parseDateToISO = (raw) => {
    //   if (!raw && raw !== 0) return null;
    //   raw = String(raw).trim();
    //   // common formats: dd/mm/yyyy or yyyy-mm-dd or dd-mm-yyyy
    //   // try dd/mm/yyyy
    //   const dmy = raw.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    //   if (dmy) {
    //     const dd = dmy[1].padStart(2, "0");
    //     const mm = dmy[2].padStart(2, "0");
    //     const yyyy = dmy[3];
    //     return `${yyyy}-${mm}-${dd}`;
    //   }
    //   // try ISO-like yyyy-mm-dd
    //   const iso = raw.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
    //   if (iso) {
    //     const yyyy = iso[1];
    //     const mm = String(iso[2]).padStart(2, "0");
    //     const dd = String(iso[3]).padStart(2, "0");
    //     return `${yyyy}-${mm}-${dd}`;
    //   }
    //   // fallback try Date parse
    //   const d = new Date(raw);
    //   if (!isNaN(d)) {
    //     const yyyy = d.getFullYear();
    //     const mm = String(d.getMonth() + 1).padStart(2, "0");
    //     const dd = String(d.getDate()).padStart(2, "0");
    //     return `${yyyy}-${mm}-${dd}`;
    //   }
    //   return null;
    // };
    const parseDateToISO = (raw) => {
      if (raw === null || raw === undefined || raw === "") return null;
      console.log(raw)
      // Excel serial
      if (typeof raw === "number") {
        const d = excelSerialToDate(raw);
        return d.toISOString().split("T")[0];
      }

      raw = String(raw).trim();

      // DD/MM/YYYY
      let m = raw.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (m) {
        return `${m[3]}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
      }

      // DD/MM/YY  ← THIS is what Singular/Excel gives you
      m = raw.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2})$/);
      if (m) {
        const yy = Number(m[3]);
        const yyyy = yy <= 69 ? 2000 + yy : 1900 + yy;
        return `${yyyy}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
      }

      // YYYY-MM-DD
      m = raw.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
      if (m) {
        return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}`;
      }

      return null;
    };

    // Decide if incoming file is the new 'form file' type by scanning headers of files.
    // We'll process each uploaded file, but for "new-format" files we look for 'campaign_network' header.
    // If any row has campaign_network header present -> treat file as new-format.
    let uploadedMetricNames = new Set();

    for (const file of uploaded) {
      // If your project already has detection logic via FILE_MAP, keep that intact.
      // But for new-type file, detect via header name inside the CSV/TSV/excel.
      console.log(file);
      await streamFileRows(file.path, async (row, headers) => {
        // headers is array of column names
        const headerLower = headers.map((h) => String(h).toLowerCase());
        console.log("Headers in file:", headerLower);
        // Detect your new file format by checking for "campaign name" + "source"
        const isNewFormat = headerLower.includes("campaignname");
        console.log(isNewFormat);
        if (isNewFormat) {
          console.log("🔥 Detected New Vivus Format File");

          const normalizeKey = (rx) => {
            const idx = headerLower.findIndex((h) => rx.test(h));
            return idx === -1 ? null : headers[idx];
          };

          const dateKey = normalizeKey(/^date$/i);
          const campaignNameKey = normalizeKey(/^campaignname$/i);
          const clicksKey = normalizeKey(/^clicks$/i);
          const installsKey = normalizeKey(/^installs$/i);

          // event column
          let eventKey = null;
          if (event_name) {
            const evNorm = event_name.toLowerCase().replace(/\s+/g, "");
            for (const h of headers) {
              const hNorm = h.toLowerCase().replace(/\s+/g, "");
              if (hNorm === evNorm) {
                eventKey = h;
                break;
              }
            }
          }

          const extractPubId = (campaign) => {
            const m = String(campaign || "").match(/_(\d{2,})$/);
            return m ? m[1] : null;
          };
          let pid = null;
          let advMatch = null;

          // 1️⃣ Try campaign-name PID token
          const campaignVal = String(row[campaignNameKey] || "");
          const pidTokenMatch = campaignVal.match(/_([a-zA-Z]+)_\d+/);

          if (pidTokenMatch) {
            const tokenPid = normalizePid(pidTokenMatch[1]);
            advMatch = advPidMap.get(tokenPid);
            if (advMatch) pid = tokenPid;
          }
          // 2️⃣ Try Source (normalized + fuzzy match)
          if (!pid) {
            const sourceKey = normalizeKey(/^source$/i);
            if (sourceKey) {
              const sourcePid = normalizePid(row[sourceKey]);

              // exact match
              advMatch = advPidMap.get(sourcePid);

              // 🔥 fallback fuzzy match
              if (!advMatch) {
                advMatch = findClosestPidMatch(sourcePid, advPidMap);
              }

              if (advMatch) {
                pid = advMatch.pidLower;
              }
            }
          }

          // 3️⃣ 🔥 FINAL FALLBACK: pubid match (CRITICAL)
          if (!pid) {
            const pubidFromCampaign = extractPubId(campaignVal);
            if (pubidFromCampaign) {
              advMatch = [...advPidMap.values()].find(
                (d) => String(d.pubid) === pubidFromCampaign,
              );
              if (advMatch) {
                pid = advMatch.pidLower;
              }
            }
          }

          if (!pid || !advMatch) {
            console.warn("❌ PID unresolved for row:", campaignVal);
            return;
          }

          filePids.add(pid);

          // Try find pub_name & pubid from adv_data
          let pubid = "N/A";
          let pubam = "N/A";

          // let advMatch = advPidMap.get(pid);

          if (!advMatch) {
            advMatch = findClosestPidMatch(pid, advPidMap);
          }

          if (advMatch) {
            pubid = advMatch.pubid;
            pubam = advMatch.pubam;
          }

          // Parse date
          let metricsDate = parseDateToISO(row[dateKey]);
          console.log("RAW DATE:", row[dateKey], "→", metricsDate);

          if (!metricsDate) metricsDate = startDate;

          const parseNum = (v) => Number(String(v).replace(/,/g, "")) || 0;

          const clicks = parseNum(row[clicksKey]);
          const installs = parseNum(row[installsKey]);
          const eventVal = eventKey ? parseNum(row[eventKey]) : 0;

          // Store metrics
          if (clicks > 0)
            incIfPidDate(metricCounts.clicks, pid, metricsDate, clicks);

          if (installs > 0)
            incIfPidDate(metricCounts.noi, pid, metricsDate, installs);

          if (eventVal > 0)
            incIfPidDate(metricCounts.noe, pid, metricsDate, eventVal);

          // Save pub info (so you don't overwrite later)
          advPidMap.set(pid, {
            pid,
            pidLower: pid,
            pubid,
            pubam,
            campaign_name: baseCampaignName,
            pause: 0,
            nocrm: 0,
            os,
          });

          console.log(
            `PID:${pid} DATE:${metricsDate} CLICKS:${clicks} INST:${installs} EVENT:${eventVal}`,
          );

          uploadedMetricNames.add("form_file");
          return;
        }

        // --- FALLBACK to your previous multi-file logic (clicks, noi, pe, etc.) ---
        // The code below replicates your earlier logic for other file types.
        // It uses FILE_MAP to map file name patterns (if available).
        // If your current code already handles this, this block will just reuse it.

        // Determine metricName using existing FILE_MAP logic (if available)
        const normalize = (str) =>
          String(str || "")
            .toLowerCase()
            .replace(/[\s\-_]/g, "");
        const sortedKeys = Object.keys(FILE_MAP || {}).sort(
          (a, b) => b.length - a.length,
        );
        const fileNameNorm = normalize(file.originalname || "");
        const key = sortedKeys.find((k) => fileNameNorm.includes(normalize(k)));
        const metricName = FILE_MAP && key ? FILE_MAP[key] : null;
        if (metricName) uploadedMetricNames.add(metricName);

        // your older per-row processing (clicks / noi / pe / etc.)
        // Keep the code path you had earlier here; for brevity I'm reusing
        // your existing logic but ensuring we collect filePids and metricCounts.
        // ---- copy of your prior behavior begins ----

        // Try to find media source like old code:
        const mediaSourceKey = headers.find((c) =>
          /media[-_\s]?source/i.test(c),
        );
        const sourceVal = mediaSourceKey ? row[mediaSourceKey] : null;
        if (!sourceVal) return;
        const pid = String(sourceVal).trim().toLowerCase();
        filePids.add(pid);
        const metricsDate = (() => {
          // try to extract using extractDate helper if available
          try {
            return extractDate ? extractDate(row, headers, metricName) : null;
          } catch (e) {
            return null;
          }
        })();
        if (!metricsDate) return;

        if (metricName === "clicks") {
          const normalizeHeader = (s) =>
            String(s)
              .toLowerCase()
              .replace(/[\s\-_]/g, "");
          const headersArr = Object.keys(row);
          const normHeaders = headersArr.map((h) => normalizeHeader(h));

          const clicksIdx = normHeaders.findIndex(
            (c) => c === "clicks" || c === "click",
          );
          if (clicksIdx !== -1) {
            const clicksKey2 = headersArr[clicksIdx];
            const clicksVal2 =
              Number((row[clicksKey2] || "").toString().replace(/,/g, "")) || 0;
            incIfPidDate(metricCounts.clicks, pid, metricsDate, clicksVal2);
          } else {
            incIfPidDate(metricCounts.clicks, pid, metricsDate, 0);
          }

          if (!uploadedMetricNames.has("noi")) {
            const noiIdx = normHeaders.findIndex((c) =>
              c.includes("installsappsflyer"),
            );
            if (noiIdx !== -1) {
              const noiKey = headersArr[noiIdx];
              const noiVal =
                Number((row[noiKey] || "").toString().replace(/,/g, "")) || 0;
              incIfPidDate(metricCounts.noi, pid, metricsDate, noiVal);
            }
          }

          if (!uploadedMetricNames.has("noe")) {
            const noeIdx = normHeaders.findIndex((c) =>
              c.includes("uniqueusersltvdayscumulativeappsflyer"),
            );
            if (noeIdx !== -1) {
              const noeKey = headersArr[noeIdx];
              const noeVal =
                Number((row[noeKey] || "").toString().replace(/,/g, "")) || 0;
              incIfPidDate(metricCounts.noe, pid, metricsDate, noeVal);
            }
          }

          return;
        }

        if (metricName === "noi") {
          const mediaSourceKey2 = headers.find((c) =>
            /media\s*source/i.test(c),
          );
          const installTimeKey = headers.find((c) => /install\s*time/i.test(c));
          const pid2 = mediaSourceKey2
            ? (row[mediaSourceKey2] || "").trim().toLowerCase()
            : null;
          if (!pid2) return;
          const installTimeRaw = installTimeKey ? row[installTimeKey] : null;
          const dateVal = installTimeRaw ? new Date(installTimeRaw) : null;
          if (!dateVal || isNaN(dateVal)) return;
          const mDate = `${dateVal.getFullYear()}-${String(
            dateVal.getMonth() + 1,
          ).padStart(2, "0")}-${String(dateVal.getDate()).padStart(2, "0")}`;
          incIfPidDate(metricCounts.noi, pid2, mDate, 1);
          return;
        }

        if (metricName === "pe" || metricName === "noe") {
          const evKey = headers.find((c) => /event[_\s]?name/i.test(c));
          if (!eventCountsByPidDate.has(pid))
            eventCountsByPidDate.set(pid, new Map());
          const pidDateMap = eventCountsByPidDate.get(pid);
          const dateMap = pidDateMap.get(metricsDate) || new Map();
          if (evKey) {
            const ev = String(row[evKey] || "")
              .trim()
              .toLowerCase();
            dateMap.set(ev, (dateMap.get(ev) || 0) + 1);
          }
          pidDateMap.set(metricsDate, dateMap);
          incIfPidDate(metricCounts[metricName], pid, metricsDate, 1);
          return;
        }

        // default
        incIfPidDate(metricCounts[metricName], pid, metricsDate, 1);
        // ---- copy of prior behavior ends ----
      });
    } // end for each uploaded file

    // Merge advData with prev30 only for PIDs seen in files (same as previous logic)
    const mergedAdvData = [...advData];
    for (const prev of advDataPrev30) {
      if (filePids.has(prev.pidLower) && !advPidMap.has(prev.pidLower)) {
        mergedAdvData.push(prev);
        advPidMap.set(prev.pidLower, prev);
      }
    }

    // Add placeholder entries for PIDs present in files but missing in adv_data sets.
    for (const pid of filePids) {
      if (!advPidMap.has(pid)) {
        // attempt to set pubid by searching in file rows is non-trivial here (we parsed pubid earlier per-row)
        // so we'll set pubid to "N/A" and pid to pid string; if you prefer storing pubid from earlier parse,
        // we could store a map pid->pubid while parsing above. For now, store pubid "N/A".
        mergedAdvData.push({
          pid,
          pidLower: pid,
          pubid: pid,
          pubam: "N/A",
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

    // Prepare DB inserts (per pid + date)
    const metricsData = [];
    const eventData = [];

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

      // also include dates from events map
      if (eventCountsByPidDate.has(pidLower)) {
        for (const date of eventCountsByPidDate.get(pidLower).keys()) {
          allDates.add(date);
        }
      }

      if (allDates.size === 0) {
        allDates.add(startDate); // ensure pid stored even with 0 data
      }

      for (const date of allDates) {
        const numOrZero = (val) =>
          val === undefined || val === null ? 0 : val;

        const numOrNull = (val) =>
          val === undefined || val === null || val === 0 ? null : val;

        const metrics = {
          noi: numOrZero(metricCounts.noi.get(pidLower)?.get(date)), // always 0 if no value
          noe: numOrZero(metricCounts.noe.get(pidLower)?.get(date)), // always 0 if no value
          clicks: numOrZero(metricCounts.clicks.get(pidLower)?.get(date)), // always 0 if no value

          rti: numOrNull(metricCounts.rti.get(pidLower)?.get(date)), // NULL when no value
          pe: numOrNull(metricCounts.pe.get(pidLower)?.get(date)), // NULL when no value
          pi: numOrNull(metricCounts.pi.get(pidLower)?.get(date)), // NULL when no value
        };

        metricsData.push([
          fullCampaignName,
          os,
          geo,
          date,
          d.pubam || "N/A",
          d.pid,
          d.pubid || pid,
          metrics.noi,
          metrics.rti,
          metrics.pe,
          metrics.pi,
          metrics.noe,
          metrics.clicks,
          d.pause || 0,
          d.nocrm || 0,
        ]);

        const pidEvents =
          eventCountsByPidDate.get(pidLower)?.get(date) || new Map();
        for (const [eventName, count] of pidEvents.entries()) {
          eventData.push([d.pid, date, eventName, count, "event"]);
        }
      }
    }

    // Insert into DB
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

    // Emit socket event if socketId provided
    const io = req.app.get("io");
    if (socketId && io && io.sockets && io.sockets.sockets.get(socketId)) {
      io.to(socketId).emit("uploadsingularComplete", {
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
  } catch (err) {
    console.error("❌ Error in handleUpload:", err);
    res.status(500).json({ msg: "Server error", error: err.message });
  } finally {
    // cleanup uploaded files
    let uploaded = [];
    if (Array.isArray(req.files)) uploaded = req.files;
    else if (req.files) uploaded = Object.values(req.files).flat();
    else if (req.file) uploaded = [req.file];

    uploaded.forEach((f) => {
      try {
        if (f && f.path && fs.existsSync(f.path)) fs.unlinkSync(f.path);
      } catch (e) {
        console.warn("⚠️ Failed to delete file:", f && f.path, e.message);
      }
    });
  }
};

module.exports = { handlesingularUpload };
