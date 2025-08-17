const XLSX = require("xlsx");
const pool = require("../config/db");
const fs = require("fs");

const FILE_MAP = {
  installs: "noi",
  "blocked-installs": "rti",
  "fraud-post-inapps": "pe",
  detection: "pi",
  "in-app-event": "noe",
  "non-organic-in-app-event": "noe",
  clicks: "clicks",
};

function normalizeRow(row) {
  const normalized = {};
  for (const key in row) {
    const cleanKey = key.replace(/\s+/g, "").toLowerCase();
    normalized[cleanKey] = row[key];
  }
  return normalized;
}

function parseXlsx(filePath) {
  const wb = XLSX.readFile(filePath, { cellDates: true });
  const sheet = wb.SheetNames[wb.SheetNames.length - 1];
  const raw = XLSX.utils.sheet_to_json(wb.Sheets[sheet], { defval: "" });

  if (raw.length > 0) {
    console.log("📄 Columns:", Object.keys(raw[0]));
  }

  return raw.map(normalizeRow);
}

const handleUpload = async (req, res) => {
  try {
    const { campaignName, os, geo, dateRange } = req.body;
    const uploaded = req.files?.files || [];
    console.log(os, geo, dateRange, campaignName);
    console.log(`📂 Files uploaded: ${uploaded}`);
    if (!campaignName || uploaded.length === 0) {
      return res.status(400).json({ msg: "Missing fields or files" });
    }

    const demoFile = uploaded.find((f) => /data/i.test(f.originalname));
    if (!demoFile) {
      return res.status(400).json({ msg: "data file required" });
    }

    const demoData = parseXlsx(demoFile.path);
    console.log(demoData);
    console.log(`📊 Total Rows in data: ${demoData.length}`);

    const demoIdentifiers = demoData
      .filter((r) => r.pid)
      .map((r, i) => {
        const pid = String(r.pid || "").trim();
        const pubid = String(r.pubid || "").trim();
        const pubam = String(r.pubam || "").trim();
        const pause = r.pause; // ✅ Added
        const nocrm = r.crmnumber || r.crm || 0;
        console.log(
          `🔹 Row ${
            i + 1
          }: PID="${pid}", Pub ID="${pubid}", Pub Am="${pubam}", CRM="${nocrm}", Pause="${pause}"`
        );
        return { pid, pubid, pubam, pause, nocrm };
      });

    if (demoIdentifiers.length === 0) {
      return res
        .status(400)
        .json({ msg: "No valid PID entries found in data file." });
    }

    const parsedFiles = {};
    const matchedKeys = new Set();

    for (const file of uploaded) {
      if (/data/i.test(file.originalname)) continue;

      const sortedKeys = Object.keys(FILE_MAP).sort(
        (a, b) => b.length - a.length
      );
      const key = sortedKeys.find((k) =>
        file.originalname.toLowerCase().includes(k)
      );

      if (!key) {
        console.warn(`⚠️ No FILE_MAP match for: ${file.originalname}`);
        continue;
      }

      matchedKeys.add(key);
      const metricName = FILE_MAP[key];
      const rows = parseXlsx(file.path);
      parsedFiles[metricName] = rows;

      console.log(
        `✅ Matched: ${file.originalname} ➝ ${metricName}, Rows: ${rows.length}`
      );
      if (!rows.length) {
        console.log(`⚠️ File ${file.originalname} has no data.`);
      }
    }

    const missingFiles = Object.keys(FILE_MAP).filter(
      (k) => !matchedKeys.has(k)
    );

    const hasInAppEvent =
      matchedKeys.has("in-app-event") ||
      matchedKeys.has("non-organic-in-app-event");
    const installsMissing = !matchedKeys.has("installs");
    const inAppEventMissing = !hasInAppEvent;

    if (installsMissing && inAppEventMissing && !parsedFiles.clicks) {
      return res.status(400).json({
        msg: "❌ 'installs' and 'in-app-event' files are missing, and fallback 'clicks' file is also not available.",
      });
    }

    const clicksMap = {};
    const noiMap = {};
    const noeMap = {};

    const clicksRows = parsedFiles.clicks || [];

    if (clicksRows.length > 0) {
      const headers = Object.keys(clicksRows[0]);

      const mediaSourceKey = headers.find((c) => /media[_\s]?source/i.test(c));
      const clicksKey = headers.find((c) => /clicks/i.test(c));
      const installsKey = headers.find((c) => /installs/i.test(c));
      const noeKey = headers.find((c) =>
        /event[_\s]?unique[_\s]?users[_\s]?submit[_\s]?success/i.test(c)
      );

      clicksRows.forEach((row) => {
        const source = String(row[mediaSourceKey] || "")
          .trim()
          .toLowerCase();

        clicksMap[source] =
          parseInt(String(row[clicksKey]).replace(/,/g, ""), 10) || 0;

        if (installsMissing && installsKey) {
          noiMap[source] =
            parseInt(String(row[installsKey]).replace(/,/g, ""), 10) || 0;
        }

        if (inAppEventMissing && noeKey) {
          noeMap[source] =
            parseInt(String(row[noeKey]).replace(/,/g, ""), 10) || 0;
        }
      });
    }

    for (const { pid, pubid, pubam, pause,nocrm } of demoIdentifiers) {
      const pidLower = pid.toLowerCase();

      const metrics = {
        noi: installsMissing ? noiMap[pidLower] || 0 : 0,
        rti: 0,
        pe: 0,
        pi: 0,
        noe: inAppEventMissing ? noeMap[pidLower] || 0 : 0,
        clicks: clicksMap[pidLower] || 0,
      };

      // Process metrics from other files
      for (const [metricName, rows] of Object.entries(parsedFiles)) {
        if (!rows.length || metricName === "clicks") continue;

        const mediaSourceKey = Object.keys(rows[0]).find((c) =>
          /media[_\s]?source/i.test(c)
        );
        if (!mediaSourceKey) continue;

        rows.forEach((row) => {
          const source = String(row[mediaSourceKey] || "")
            .trim()
            .toLowerCase();
          if (source === pidLower) {
            metrics[metricName]++;
          }
        });
      }

      // ✅ Pause check: if "true" (string or boolean), set 1, else 0
      const isPaused = String(pause).toLowerCase() === "true" ? 1 : 0;
      const sql = `
  INSERT INTO campaign_metrics
  (campaign_name, os, geo, date_range, pubam, pid, pubid,
   noi, rti, pe, pi, noe, clicks, is_paused, nocrm)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;

      const params = [
        campaignName,
        os,
        geo,
        dateRange,
        pubam,
        pid,
        pubid,
        metrics.noi,
        metrics.rti,
        metrics.pe,
        metrics.pi,
        metrics.noe,
        metrics.clicks,
        isPaused,
        nocrm, // ✅ Save CRM number
      ];

      const [result] = await pool.query(sql, params);
      const campaignId = result.insertId;

      // Additional event breakdown for 'pe' and 'noe'
      for (const eventFileKey of ["pe", "noe"]) {
        const rows = parsedFiles[eventFileKey] || [];

        if (!rows.length) {
          if (eventFileKey === "pe") {
            console.log(`❌ No rows found for event type: ${eventFileKey}`);
          }
          continue;
        }

        const mediaSourceKey = Object.keys(rows[0] || {}).find((c) =>
          /media[_\s]?source/i.test(c)
        );
        const eventNameKey = Object.keys(rows[0] || {}).find((c) =>
          /event[_\s]?name/i.test(c)
        );

        if (eventFileKey === "pe") {
          console.log(`🔍 Processing event type: ${eventFileKey}`);
          console.log(
            `📌 mediaSourceKey: ${mediaSourceKey}, eventNameKey: ${eventNameKey}`
          );
        }

        const eventCounts = {};

        rows.forEach((row, i) => {
          const source = String(row[mediaSourceKey] || "")
            .trim()
            .toLowerCase();
          if (source !== pidLower) return;

          if (eventNameKey) {
            const event = String(row[eventNameKey] || "")
              .trim()
              .toLowerCase();
            if (event) {
              eventCounts[event] = (eventCounts[event] || 0) + 1;

              if (eventFileKey === "pe") {
                console.log(
                  `✅ [${eventFileKey}] Row ${
                    i + 1
                  }: Found event "${event}" -> count: ${eventCounts[event]}`
                );
              }
            }
          } else {
            for (const [key, value] of Object.entries(row)) {
              if (/event[_\s]?count/i.test(key) && !isNaN(Number(value))) {
                const event = key
                  .toLowerCase()
                  .replace(/^event[_\s]?count[_\s]?/i, "")
                  .trim();
                if (event) {
                  eventCounts[event] =
                    (eventCounts[event] || 0) + Number(value);

                  if (eventFileKey === "pe") {
                    console.log(
                      ` ✅ [${eventFileKey}] Row ${
                        i + 1
                      }: Inferred event "${event}" -> count: ${
                        eventCounts[event]
                      }`
                    );
                  }
                }
              }
            }
          }
        });

        for (const [event, count] of Object.entries(eventCounts)) {
          const eventInsert = ` INSERT INTO campaign_event_metrics (campaign_id, pid, event_name, count, event_type)
    VALUES (?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE count = VALUES(count), event_type = VALUES(event_type)`;
          const values = [campaignId, pid, event, count, eventFileKey];

          if (eventFileKey === "pe") {
            console.log("📝 Trying to insert:", { values });
          }

          try {
            const [result] = await pool.query(eventInsert, values);
            if (eventFileKey === "pe") {
              console.log("✅ Insert result:", result);
            }
          } catch (err) {
            console.error(
              `❌ MySQL insert failed for event_type "${eventFileKey}" event "${event}":`,
              err
            );
          }
        }

        if (Object.keys(eventCounts).length === 0 && eventFileKey === "pe") {
          console.log(
            `⚠️ No matching events found for PID "${pid}" in file: ${eventFileKey}`
          );
        }
      }
    }
    // ✅ Send success response to frontend
    return res.status(200).json({
      msg: "Upload successful",
    });
  } catch (err) {
    console.error("❌ Error in handleUpload:", err);
    res.status(500).json({ msg: "Server error", error: err.message });
  } finally {
    if (Array.isArray(req.files?.files)) {
      req.files.files.forEach((f) => {
        try {
          fs.unlinkSync(f.path);
        } catch (e) {
          console.warn("⚠️ Failed to delete file:", f.path);
        }
      });
    }
  }
};

module.exports = { handleUpload };
