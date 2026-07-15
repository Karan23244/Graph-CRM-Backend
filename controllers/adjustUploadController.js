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

function detectDelimiter(filePath) {
  const sample = fs.readFileSync(filePath, "utf8").split("\n")[0];
  return detect(sample) || ",";
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

// ---------------------------
// Fetch adv_data by campaign + dateRange
// ---------------------------
async function getAdvDataFromDB(campaignName, startDate, endDate, os, campaignIds) {
  const [rows] = await pool.query(
    `SELECT pid, pub_id, pub_name, campaign_id, campaign_name, paused_date,shared_date, flag, os
FROM adv_data
WHERE REPLACE(REPLACE(REPLACE(campaign_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), '') =
      REPLACE(REPLACE(REPLACE(?, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')
  AND campaign_id IN (?)
  AND DATE(shared_date) BETWEEN ? AND ?
  AND os = ?;`,
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
        campaign_id: r.campaign_id,
        shared_date: r.shared_date,
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
const handleAdjustUpload = async (req, res) => {
  try {
    const { campaignName, os, geo, dateRange, socketId, event_name } = req.body;
    const campaignIds = JSON.parse(req.body.campaign_ids || "[]");
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
      campaignIds,
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
      campaignIds,
    );
    const [configRows] = await pool.query(
      `
SELECT events
FROM campaign_configs
WHERE (
    campaign_name = ?
    OR JSON_CONTAINS(campaign_name, JSON_ARRAY(?))
)
AND (os = ? OR os IS NULL OR os = '')
LIMIT 1
`,
      [fullCampaignName, fullCampaignName, os],
    );

    let allowedEvents = [];

    if (configRows.length && configRows[0].events) {
      let rawEvents = configRows[0].events;

      if (Buffer.isBuffer(rawEvents)) {
        rawEvents = rawEvents.toString("utf8");
      }

      if (typeof rawEvents === "string") {
        try {
          allowedEvents = JSON.parse(rawEvents);
        } catch {
          allowedEvents = rawEvents.split(",");
        }
      }
    }

    allowedEvents = allowedEvents
      .map((e) => String(e).trim().toLowerCase())
      .filter(Boolean);

    console.log("Allowed Events:", allowedEvents);
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
      impressions: new Map(),
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
    const parseDateToISO = (raw) => {
      if (!raw && raw !== 0) return null;
      raw = String(raw).trim();
      // common formats: dd/mm/yyyy or yyyy-mm-dd or dd-mm-yyyy
      // try dd/mm/yyyy
      const dmy = raw.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (dmy) {
        const dd = dmy[1].padStart(2, "0");
        const mm = dmy[2].padStart(2, "0");
        const yyyy = dmy[3];
        return `${yyyy}-${mm}-${dd}`;
      }
      // try ISO-like yyyy-mm-dd
      const iso = raw.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
      if (iso) {
        const yyyy = iso[1];
        const mm = String(iso[2]).padStart(2, "0");
        const dd = String(iso[3]).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}`;
      }
      // fallback try Date parse
      const d = new Date(raw);
      if (!isNaN(d)) {
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const dd = String(d.getDate()).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}`;
      }
      return null;
    };

    // Helper: parse campaign_network value into { pidLower, pid, pubid }
    const parseCampaignNetwork = (val) => {
      if (!val) return null;

      const s = String(val);
      const m = s.match(/(\d{2,})/);

      if (!m) {
        const pidStr = s.trim().toLowerCase();
        return { pidLower: pidStr, pid: pidStr, pubid: "N/A" };
      }

      const numeric = m[1]; // use full number as PID

      return {
        pidLower: numeric.toLowerCase(),
        pid: numeric,
        pubid: numeric, // or "N/A"
      };
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

        // If this file contains 'campaign_network' header -> treat as new form file
        if (headerLower.some((h) => /campaign[_\s]?network/i.test(h))) {
          // --- NEW-FORM FILE PROCESSING ---
          // Normalized keys to find data
          const findKey = (rx) => {
            const idx = headers.findIndex((h) => rx.test(h));
            return idx === -1 ? null : headers[idx];
          };

          const campaignNetworkKey = findKey(/campaign[_\s]?network/i);
          const dayKey =
            findKey(/^day$/i) ||
            findKey(/date/i) ||
            findKey(/metrics[_\s]?date/i);
          const installsKey =
            findKey(/^installs$/i) ||
            findKey(/install/i) ||
            findKey(/number[_\s]?of[_\s]?installs/i);
          const clicksKey =
            findKey(/attribution[_\s]?clicks/i) ||
            findKey(/clicks/i) ||
            findKey(/click/i);
          const impressionsKey =
            findKey(/attribution[_\s]?impressions/i) || findKey(/impressions/i);
          // event column name comes from req.body.event_name
          let eventKey = null;
          if (event_name) {
            const evNorm = event_name
              .toString()
              .toLowerCase()
              .replace(/\s+/g, "");

            for (const h of headers) {
              const hNorm = String(h).toLowerCase().replace(/\s+/g, "");
              if (hNorm === evNorm) {
                eventKey = h;
                break;
              }
            }
          }

          // Read values
          const campaignNetworkVal = campaignNetworkKey
            ? row[campaignNetworkKey]
            : null;
          if (!campaignNetworkVal) return; // row not useful

          const parsed = parseCampaignNetwork(campaignNetworkVal);
          if (!parsed) return;

          // Derive pidLower and add to filePids
          const pidLower = parsed.pidLower;
          filePids.add(pidLower);

          // if date is number → Excel serial date, convert it
          let metricsDate = null;
          let rawDay = row["day"];

          // 1) If numeric AND looks like Excel serial (>40000)
          if (!isNaN(rawDay) && Number(rawDay) > 40000) {
            metricsDate = excelSerialToDate(Number(rawDay))
              .toISOString()
              .slice(0, 10);
          }
          // 2) If numeric BUT small number (like 06, 09) → treat as invalid date
          else if (!isNaN(rawDay)) {
            metricsDate = null;
          }
          // 3) If string date like "2025-11-26" or "10/11/2025"
          else {
            metricsDate = parseDateToISO(rawDay);
          }

          // fallback for safety
          if (!metricsDate) metricsDate = startDate;

          console.log("📅 Final DATE =>", metricsDate);

          // parse numeric values robustly
          const parseNum = (v) => {
            if (v === null || v === undefined || v === "") return 0;
            return Number(String(v).replace(/,/g, "").trim()) || 0;
          };

          const installsVal = installsKey ? parseNum(row[installsKey]) : 0;
          const clicksVal = clicksKey ? parseNum(row[clicksKey]) : 0;
          const impressionsVal = impressionsKey
            ? parseNum(row[impressionsKey])
            : 0;
          let totalNoe = 0;

          for (const eventName of allowedEvents) {
            const normalizeEvent = (v) =>
              String(v)
                .toLowerCase()
                .replace(/[\s_]+/g, "");

            const matchingHeader = headers.find(
              (h) => normalizeEvent(h) === normalizeEvent(eventName),
            );

            if (!matchingHeader) continue;

            const count = parseNum(row[matchingHeader]);

            if (count <= 0) continue;

            totalNoe += count;

            if (!eventCountsByPidDate.has(pidLower)) {
              eventCountsByPidDate.set(pidLower, new Map());
            }

            const pidMap = eventCountsByPidDate.get(pidLower);

            if (!pidMap.has(metricsDate)) {
              pidMap.set(metricsDate, new Map());
            }

            const dateMap = pidMap.get(metricsDate);

            dateMap.set(eventName, (dateMap.get(eventName) || 0) + count);
          }

          // 🔥 store only when greater than zero
          if (installsVal > 0) {
            incIfPidDate(metricCounts.noi, pidLower, metricsDate, installsVal);
            console.log(
              `📌 NOI => PID: ${pidLower}, DATE: ${metricsDate}, VALUE: ${installsVal}`,
            );
          }

          if (clicksVal > 0) {
            incIfPidDate(metricCounts.clicks, pidLower, metricsDate, clicksVal);
            console.log(
              `📌 CLICKS => PID: ${pidLower}, DATE: ${metricsDate}, VALUE: ${clicksVal}`,
            );
          }

          if (impressionsVal > 0) {
            incIfPidDate(
              metricCounts.impressions,
              pidLower,
              metricsDate,
              impressionsVal,
            );
          }
          // mark that we've processed a 'form' style file
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
              if (totalNoe > 0) {
                incIfPidDate(metricCounts.noe, pidLower, metricsDate, totalNoe);
              }
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
          impressions: numOrZero(
            metricCounts.impressions.get(pidLower)?.get(date),
          ),
          rti: numOrNull(metricCounts.rti.get(pidLower)?.get(date)), // NULL when no value
          pe: numOrNull(metricCounts.pe.get(pidLower)?.get(date)), // NULL when no value
          pi: numOrNull(metricCounts.pi.get(pidLower)?.get(date)), // NULL when no value
        };
        const finalCampaignId = d.campaign_id ?? campaignIds[0];
        metricsData.push([
          fullCampaignName,
          finalCampaignId,
          d.shared_date,
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
          metrics.impressions,
          d.pause || 0,
          d.nocrm || 0,
        ]);
      }
    }

    // Insert into DB
    const sqlMetrics = `
      INSERT INTO campaign_metrics_new
      (campaign_name,campaign_id,shared_date, os, geo, metrics_date, pubam, pid, pubid,
       noi, rti, pe, pi, noe, clicks,impressions, is_paused, nocrm)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        noi = VALUES(noi),
        rti = VALUES(rti),
        pe = VALUES(pe),
        pi = VALUES(pi),
        noe = VALUES(noe),
        clicks = VALUES(clicks),
        impressions = VALUES(impressions),
        is_paused = VALUES(is_paused),
        nocrm = VALUES(nocrm)
    `;
    await batchInsert(sqlMetrics, metricsData, 500);

    const [cmRows] = await pool.query(
      `
SELECT id, campaign_name, pid, metrics_date, os
FROM campaign_metrics_new
WHERE campaign_name = ? AND os = ?
`,
      [fullCampaignName, os],
    );
    const cmMap = new Map();

    for (const row of cmRows) {
      const key =
        `${row.campaign_name}_` +
        `${String(row.pid).trim().toLowerCase()}_` +
        `${row.metrics_date}_` +
        `${row.os}`;

      cmMap.set(key, row.id);
    }
    eventData.length = 0;

    for (const d of mergedAdvData) {
      const pidLower = d.pidLower;

      const pidDateMap = eventCountsByPidDate.get(pidLower);
      if (!pidDateMap) continue;

      for (const [date, events] of pidDateMap.entries()) {
        const key =
          `${fullCampaignName}_` +
          `${String(d.pid).trim().toLowerCase()}_` +
          `${date}_` +
          `${os}`;

        const campaignMetricsId = cmMap.get(key) || null;

        for (const [eventName, count] of events.entries()) {
          eventData.push([
            d.pid,
            date,
            eventName,
            count,
            "event",
            campaignMetricsId,
          ]);
        }
      }
    }
    if (eventData.length > 0) {
      const sqlEvents = `
        INSERT INTO campaign_event_metrics_new (  pid,
  metrics_date,
  event_name,
  count,
  event_type,
  campaign_metrics_id)
        VALUES ?
        ON DUPLICATE KEY UPDATE count = VALUES(count), event_type = VALUES(event_type),campaign_metrics_id = VALUES(campaign_metrics_id)
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

module.exports = { handleAdjustUpload };
