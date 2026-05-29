// controllers/campaignController.js
const db = require("../config/db"); // your existing MySQL connection/pool

// ─────────────────────────────────────────────────────────────────────────────
// GET /campaigns_list
// Returns all campaigns for the dropdown selector
// ─────────────────────────────────────────────────────────────────────────────
exports.getCampaignList = async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT campaign_id, campaign_name FROM campaigns ORDER BY campaign_name ASC`,
    );
    return res.json(rows);
  } catch (err) {
    console.error("getCampaignList error:", err);
    return res.status(500).json({ message: "Failed to fetch campaigns" });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /campaign-config
// Creates a new campaign configuration
//
// Expected payload:
// {
//   campaign_id: string,
//   campaign_name: string,
//   clicks_per_day: number,
//   installs_per_day: number,
//   events: string[],                // e.g. ["Purchase", "Registration"]
//   rule1_params: object,            // { CTI: { green: { range1: {min, max} }, ... }, ... }
//   rule2_params: object,            // { RI: { green: { range1: {min, max} }, ... }, ... }
//   ignore_metrics: string[]         // e.g. ["C2I", "Install Fraud"]
// }
// ─────────────────────────────────────────────────────────────────────────────
exports.createCampaignConfig = async (req, res) => {
  const {
    campaign_ids,
    campaign_names,
    os,
    clicks_per_day,
    installs_per_day,
    events,
    rule1_params,
    rule2_params,
    ignore_metrics,
  } = req.body;

  try {
    // Prevent duplicate configs for same campaign IDs
    const [existing] = await db.query(
      `SELECT id, campaign_id
       FROM campaign_configs`,
    );

    const alreadyExists = existing.find((row) => {
      const parsed = JSON.parse(row.campaign_id || "[]");

      const ids = Array.isArray(parsed) ? parsed : [parsed];

      return (
        ids.length === campaign_ids.length &&
        ids.every((id) => campaign_ids.includes(id))
      );
    });

    if (alreadyExists) {
      return res.status(400).json({
        message: "Configuration already exists for selected campaigns",
      });
    }

    const [result] = await db.query(
      `INSERT INTO campaign_configs
        (
          campaign_id,
          campaign_name,
          os,
          clicks_per_day,
          installs_per_day,
          events,
          rule1_params,
          rule2_params,
          ignore_metrics,
          created_at,
          updated_at
        )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, NOW(), NOW())`,
      [
        JSON.stringify(campaign_ids),
        JSON.stringify(campaign_names),
        os,
        clicks_per_day,
        installs_per_day,
        JSON.stringify(events),
        JSON.stringify(rule1_params),
        JSON.stringify(rule2_params),
        JSON.stringify(ignore_metrics),
      ],
    );

    return res.status(201).json({
      message: "Campaign config created",
      id: result.insertId,
    });
  } catch (err) {
    console.error("createCampaignConfig error:", err);

    return res.status(500).json({
      message: "Failed to create config",
    });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// GET /campaign-config/:id
// Fetch existing config by campaign_id
// ─────────────────────────────────────────────────────────────────────────────
// exports.getCampaignConfig = async (req, res) => {
//   const { id } = req.params;

//   console.log("Fetching config for campaign ID:", id);

//   try {
//     const [rows] = await db.query(`SELECT * FROM campaign_configs`);

//     console.log(`Total configs in DB: ${rows.length}`);

//     const config = rows.find((row) => {
//       let parsed;

//       try {
//         parsed = JSON.parse(row.campaign_id || "[]");
//       } catch {
//         parsed = row.campaign_id;
//       }

//       const ids = Array.isArray(parsed) ? parsed : [parsed];

//       return ids.map(String).includes(String(id));
//     });

//     if (!config) {
//       return res.status(404).json({
//         message: "Config not found",
//       });
//     }

//     console.log("Config found:", config);

//     return res.json({
//       ...config,

//       campaign_ids: (() => {
//         try {
//           return JSON.parse(config.campaign_id || "[]");
//         } catch {
//           return [config.campaign_id];
//         }
//       })(),

//       campaign_names: (() => {
//         try {
//           return JSON.parse(config.campaign_name || "[]");
//         } catch {
//           return [config.campaign_name];
//         }
//       })(),

//       events: JSON.parse(config.events || "[]"),

//       rule1_params: JSON.parse(config.rule1_params || "{}"),

//       rule2_params: JSON.parse(config.rule2_params || "{}"),

//       ignore_metrics: JSON.parse(config.ignore_metrics || "[]"),
//     });
//   } catch (err) {
//     console.error("getCampaignConfig error:", err);

//     return res.status(500).json({
//       message: "Failed to fetch config",
//     });
//   }
// };
// POST /campaign-config/find
exports.getCampaignConfig = async (req, res) => {
  const { campaign_ids } = req.body;
  console.log("Fetching config for campaign IDs:", campaign_ids);
  try {
    const [rows] = await db.query(`SELECT * FROM campaign_configs`);

    const config = rows.find((row) => {
      let parsed = [];

      try {
        parsed = JSON.parse(row.campaign_id || "[]");
      } catch {
        parsed = [];
      }

      const dbIds = parsed.map(String).sort();

      const incomingIds = campaign_ids.map(String).sort();

      return incomingIds.every((id) => dbIds.includes(id));
    });

    if (!config) {
      return res.status(404).json({
        message: "Config not found",
      });
    }

    return res.json({
      ...config,

      campaign_ids: JSON.parse(config.campaign_id || "[]"),

      campaign_names: JSON.parse(config.campaign_name || "[]"),

      events: JSON.parse(config.events || "[]"),

      rule1_params: JSON.parse(config.rule1_params || "{}"),

      rule2_params: JSON.parse(config.rule2_params || "{}"),

      ignore_metrics: JSON.parse(config.ignore_metrics || "[]"),
    });
  } catch (err) {
    console.error("getCampaignConfig error:", err);

    return res.status(500).json({
      message: "Failed to fetch config",
    });
  }
};
// ─────────────────────────────────────────────────────────────────────────────
// PUT /campaign-config/:id
// Update existing config by row id (primary key)
// ─────────────────────────────────────────────────────────────────────────────
exports.updateCampaignConfig = async (req, res) => {
  const { id } = req.params;

  const {
    campaign_ids,
    campaign_names,
    clicks_per_day,
    installs_per_day,
    events,
    rule1_params,
    rule2_params,
    ignore_metrics,
  } = req.body;

  try {
    const [result] = await db.query(
      `UPDATE campaign_configs
       SET campaign_id = ?,
           campaign_name = ?,
           clicks_per_day = ?,
           installs_per_day = ?,
           events = ?,
           rule1_params = ?,
           rule2_params = ?,
           ignore_metrics = ?,
           updated_at = NOW()
       WHERE id = ?`,
      [
        JSON.stringify(campaign_ids),
        JSON.stringify(campaign_names),
        clicks_per_day,
        installs_per_day,
        JSON.stringify(events),
        JSON.stringify(rule1_params),
        JSON.stringify(rule2_params),
        JSON.stringify(ignore_metrics),
        id,
      ],
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({
        message: "Config not found",
      });
    }

    return res.json({
      message: "Campaign config updated",
    });
  } catch (err) {
    console.error("updateCampaignConfig error:", err);

    return res.status(500).json({
      message: "Failed to update config",
    });
  }
};

exports.getConfiguredCampaigns = async (req, res) => {
  try {
    const [rows] = await db.query(`
      SELECT
        id,
        campaign_id,
        campaign_name,
        os,
        created_at,
        updated_at
      FROM campaign_configs
      ORDER BY updated_at DESC
    `);

    const safeParseArray = (value) => {
      try {
        const parsed = JSON.parse(value);

        return Array.isArray(parsed) ? parsed : [parsed];
      } catch {
        return value ? [value] : [];
      }
    };

    const formatted = rows.flatMap((row) => {
      const campaignIds = safeParseArray(row.campaign_id);

      const campaignNames = [...new Set(safeParseArray(row.campaign_name))];

      return campaignNames.map((campaignName) => ({
        config_id: row.id,

        campaign_name: campaignName,

        campaign_ids: campaignIds,

        total_campaign_ids: campaignIds.length,

        os: row.os,

        created_at: row.created_at,

        updated_at: row.updated_at,
      }));
    });

    return res.json({
      success: true,
      count: formatted.length,
      data: formatted,
    });
  } catch (err) {
    console.error("getConfiguredCampaigns error:", err);

    return res.status(500).json({
      success: false,
      message: "Failed to fetch configured campaigns",
    });
  }
};


