/**
 * decisionEngine.controller.js
 */

"use strict";

const { runDecisionEngine } = require("../services/decisionEngineService.js");

async function getDecision(req, res) {
  try {
    const { campaign_name, campaign_ids, geo, os, date } = req.body;

    if (!campaign_name || !os || !date) {
      return res.status(400).json({
        success: false,
        error: "campaign_name, os, and date are required",
      });
    }

    const data = await runDecisionEngine({
      campaign_name,
      campaign_ids,
      geo,
      os,
      date,
    });

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("[DecisionEngine] Error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

module.exports = { getDecision };
