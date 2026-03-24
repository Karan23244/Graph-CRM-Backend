const XLSX = require("xlsx");

// ------------------
// FILE MAP
// ------------------
const FILE_MAP = {
  installs: "noi",
  "blocked-installs": "rti",
  "fraud-post-inapps": "pe",
  detection: "pi",
  "in-app-event": "noe",
  "non-organic-in-app-event": "noe",
};

// ------------------
// DATE HELPERS
// ------------------
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
    seconds
  );
}

function formatDateToMySQL(date) {
  if (!date) return null;

  const pad = (n) => (n < 10 ? "0" + n : n);

  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate()
  )} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(
    date.getSeconds()
  )}`;
}

// ------------------
// PARSE FILES → MAP
// ------------------
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
        formattedTime = formatDateToMySQL(
          excelDateToJSDate(timeRaw)
        );
      } else if (typeof timeRaw === "string") {
        const [datePart, timePart] = timeRaw.split(" ");
        const [d, m, y] = datePart.split("/");
        formattedTime = `${y}-${m}-${d} ${timePart}:00`;
      }

      if (!map.has(key)) {
        map.set(key, formattedTime);
      }
    }
  }

  return map;
}

// ------------------
// MAIN FUNCTION
// ------------------
async function processCampaignUploads({
  files,
  campaignname,
  os,
  daterange,
  geo,
  conn,
}) {
console.log("Processing campaign uploads with params:", {
  campaignname,
  os,
  daterange,
  geo,
  fileCount: files.length,
});
  const categorizedFiles = {};

  for (const file of files) {
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

  const installFiles = categorizedFiles.noi;

  if (!installFiles || installFiles.length === 0) {
    throw new Error("Install file missing");
  }

  // ✅ Build maps
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

  const rows = [];

  for (const installFile of installFiles) {
    const workbook = XLSX.readFile(installFile.path);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(sheet);

    for (const row of data) {
      const rawKey =
        row["Advertising ID"] || row["IDFA"] || row["IP"];

      if (!rawKey) continue;

      const key = `${campaignname}_${rawKey}`;

      let installTimeRaw = row["Install Time"];
      let installTime = null;

      if (typeof installTimeRaw === "number") {
        installTime = formatDateToMySQL(
          excelDateToJSDate(installTimeRaw)
        );
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
        rtiMap.has(rawKey) ? "yes" : "no",
        piMap.has(rawKey) ? "yes" : "no",
        noeMap.has(rawKey) ? "yes" : "no",
        peMap.has(rawKey) ? "yes" : "no",

        rtiMap.get(rawKey) || null,
        piMap.get(rawKey) || null,
        noeMap.get(rawKey) || null,
        peMap.get(rawKey) || null,
      ]);
    }
  }

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