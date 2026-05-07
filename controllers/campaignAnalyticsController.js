'use strict';
const db = require("../config/db");
const analyticsService = require('./campaignAnalyticsService');

exports.getCampaignAnalytics = async (req, res) => {
  try {

    console.log("REQ BODY:", req.body);

    const result = await analyticsService.getCampaignAnalytics(req.body);

    return res.status(200).json({
      success: true,
      ...result,
    });

  } catch (err) {
    console.error(err);

    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
};
exports.getUniqueCampaigns = async (req, res) => {
  try {
    const query = `
      SELECT DISTINCT campaign_name
      FROM campaign_metrics_new
      WHERE campaign_name IS NOT NULL
        AND campaign_name != ''
      ORDER BY campaign_name ASC
    `;

    const [rows] = await db.query(query);

    return res.status(200).json({
      success: true,
      count: rows.length,
      data: rows.map(row => row.campaign_name)
    });

  } catch (error) {
    console.error('Error fetching campaigns:', error);

    return res.status(500).json({
      success: false,
      message: 'Failed to fetch campaign names',
      error: error.message
    });
  }
}