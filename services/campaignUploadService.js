const fs = require("fs");
const csv = require("fast-csv");
const FILE_MAP = {
  installs: "noi",
  "blocked-installs": "rti",
  "fraud-post-inapps": "pe",
  detection: "pi",
  "in-app-event": "noe",
  "non-organic-in-app-event": "noe",
  clicks: "clicks",
};
// ------------------
// DATE PARSER (same as API)
// ------------------
function parseDateTime(timeRaw) {
  if (!timeRaw) return null;

  if (typeof timeRaw === "number") {
    const utc_days = Math.floor(timeRaw - 25569);
    const d = new Date(utc_days * 86400000);
    const frac = timeRaw - Math.floor(timeRaw);

    let secs = Math.floor(86400 * frac);
    const hh = Math.floor(secs / 3600);
    secs -= hh * 3600;
    const mm = Math.floor(secs / 60);
    secs -= mm * 60;

    const p = (n) => (n < 10 ? "0" + n : n);

    return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(
      d.getUTCDate(),
    )} ${p(hh)}:${p(mm)}:${p(secs)}`;
  }

  if (typeof timeRaw === "string") {
    if (/^\d{4}-\d{2}-\d{2}/.test(timeRaw)) {
      return timeRaw.slice(0, 19);
    }

    const m = timeRaw.match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}:\d{2}:\d{2})/);
    if (m) {
      return `${m[3]}-${m[2]}-${m[1]} ${m[4]}`;
    }

    const parsed = new Date(timeRaw);
    if (!isNaN(parsed)) {
      const p = (n) => (n < 10 ? "0" + n : n);
      return `${parsed.getFullYear()}-${p(
        parsed.getMonth() + 1,
      )}-${p(parsed.getDate())} ${p(parsed.getHours())}:${p(
        parsed.getMinutes(),
      )}:${p(parsed.getSeconds())}`;
    }
  }

  return null;
}

// ------------------
// PARSE CSV → MAP (STREAM)
// ------------------
function parseCSVFile(filePath, isEvent = false) {
  return new Promise((resolve, reject) => {
    const map = new Map();
    const timeCol = isEvent ? "Event Time" : "Install Time";

    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true, trim: true }))
      .on("data", (row) => {
        const key = row["Advertising ID"] || row["IDFA"] || row["IP"];
        if (!key || map.has(key)) return;

        const formattedTime = parseDateTime(row[timeCol]);
        if (formattedTime) map.set(key, formattedTime);
      })
      .on("end", () => resolve(map))
      .on("error", reject);
  });
}

// ------------------
// BUILD MAPS (PARALLEL)
// ------------------
async function buildLookupMap(files, isEvent = false) {
  if (!files || files.length === 0) return new Map();

  const maps = await Promise.all(
    files.map((f) => parseCSVFile(f.path, isEvent)),
  );

  const merged = new Map();
  for (const m of maps) {
    for (const [k, v] of m) {
      if (!merged.has(k)) merged.set(k, v);
    }
  }

  return merged;
}

// ------------------
// PARSE INSTALL FILE (STREAM)
// ------------------
function parseInstallFileStream(
  filePath,
  campaignname,
  os,
  daterange,
  geo,
  rtiMap,
  piMap,
  noeMap,
  peMap,
) {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true, trim: true }))
      .on("data", (row) => {
        const rawKey = row["Advertising ID"] || row["IDFA"] || row["IP"];
        if (!rawKey) return;

        const installTime = parseDateTime(row["Install Time"]);
        if (!installTime) return;

        const key = `${campaignname}_${rawKey}`;

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
          rtiMap.has(rawKey) ? "yes" : "no",
          piMap.has(rawKey) ? "yes" : "no",
          noeMap.has(rawKey) ? "yes" : "no",
          peMap.has(rawKey) ? "yes" : "no",

          rtiMap.get(rawKey) || null,
          piMap.get(rawKey) || null,
          noeMap.get(rawKey) || null,
          peMap.get(rawKey) || null,
        ]);
      })
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

// ------------------
// MAIN FUNCTION (UPDATED)
// ------------------
async function processCampaignUploads({
  files,
  campaignname,
  os,
  daterange,
  geo,
  conn,
}) {
  const categorizedFiles = {};

  // ✅ categorize files
  for (const file of files) {
    const name = file.originalname.toLowerCase();

    for (const key in FILE_MAP) {
      if (name.includes(key)) {
        const type = FILE_MAP[key];
        (categorizedFiles[type] = categorizedFiles[type] || []).push(file);
      }
    }
  }

  const installFiles = categorizedFiles.noi;
  if (!installFiles || installFiles.length === 0) {
    throw new Error("Install file missing");
  }

  // ✅ build maps in parallel
  const [rtiMap, piMap, noeMap, peMap] = await Promise.all([
    buildLookupMap(categorizedFiles.rti),
    buildLookupMap(categorizedFiles.pi),
    buildLookupMap(categorizedFiles.noe, true),
    buildLookupMap(categorizedFiles.pe, true),
  ]);

  // ✅ parse install files in parallel
  const rowArrays = await Promise.all(
    installFiles.map((f) =>
      parseInstallFileStream(
        f.path,
        campaignname,
        os,
        daterange,
        geo,
        rtiMap,
        piMap,
        noeMap,
        peMap,
      ),
    ),
  );

  const rows = rowArrays.flat();

  if (!rows.length) return 0;

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
      rti = IF(VALUES(rti)='yes','yes',campaign_uploads.rti),
      pi  = IF(VALUES(pi)='yes','yes',campaign_uploads.pi),
      noe = IF(VALUES(noe)='yes','yes',campaign_uploads.noe),
      pe  = IF(VALUES(pe)='yes','yes',campaign_uploads.pe),
      rti_time = COALESCE(VALUES(rti_time), campaign_uploads.rti_time),
      pi_time  = COALESCE(VALUES(pi_time), campaign_uploads.pi_time),
      noe_time = COALESCE(VALUES(noe_time), campaign_uploads.noe_time),
      pe_time  = COALESCE(VALUES(pe_time), campaign_uploads.pe_time)
  `;

  await conn.query(query, [rows]);

  return rows.length;
}

module.exports = { processCampaignUploads };
