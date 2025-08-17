const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const uploadRoutes = require("./routes/uploadRoutes");
const campaignRoutes = require("./routes/campaignRoutes");
const router = express.Router();
const pool = require("./config/db");
dotenv.config();
const app = express();

app.use(cors());
app.use(express.json());
app.use("/api", uploadRoutes);
app.use("/api", campaignRoutes);

// Helper: fetch campaign conditions; if not present, fall back to __DEFAULT__
app.get("/api/zone-conditions/:campaign", async (req, res) => {
  const campaign = req.params.campaign;
  try {
    const [rows] = await pool.query(
      'SELECT * FROM campaign_zone_conditions WHERE campaign_name = ? ORDER BY FIELD(zone_color, "Green","Yellow","Orange","Red")',
      [campaign]
    );

    if (rows.length > 0) return res.json(rows);

    // fallback to default
    const [defaults] = await pool.query(
      'SELECT * FROM campaign_zone_conditions WHERE campaign_name = "__DEFAULT__" ORDER BY FIELD(zone_color, "Green","Yellow","Orange","Red")'
    );
    return res.json(defaults);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "DB error" });
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
        'SELECT * FROM campaign_zone_conditions WHERE campaign_name = "__DEFAULT__"'
      );

      // insert/update each default row into campaign
      for (const d of defaults) {
        await pool.query(
          `INSERT INTO campaign_zone_conditions (campaign_name, zone_color, fraud_min, fraud_max, cti_min, cti_max, ite_min, ite_max, etc_min, etc_max)
         VALUES (?,?,?,?,?,?,?,?,?,?)
         ON DUPLICATE KEY UPDATE fraud_min=VALUES(fraud_min), fraud_max=VALUES(fraud_max), cti_min=VALUES(cti_min), cti_max=VALUES(cti_max), ite_min=VALUES(ite_min), ite_max=VALUES(ite_max), etc_min=VALUES(etc_min), etc_max=VALUES(etc_max)`,
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
          ]
        );
      }

      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: "DB error" });
    }
  }
);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
