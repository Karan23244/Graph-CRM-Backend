/**
 * decisionEngine.controller.js
 */

'use strict';

const { runDecisionEngine } = require('../services/decisionEngineService.js');

async function getDecision(req, res) {
  try {
    const { campaign_name, os, date } = req.body;

    if (!campaign_name || !os || !date) {
      return res.status(400).json({
        success: false,
        error: 'campaign_name, os, and date are required',
      });
    }

    // Basic date format guard (YYYY-MM-DD)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        success: false,
        error: 'date must be in YYYY-MM-DD format',
      });
    }

    const data = await runDecisionEngine({ campaign_name, os, date });

    return res.status(200).json({ success: true, data });
  } catch (err) {
    console.error('[DecisionEngine] Error:', err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = { getDecision };


