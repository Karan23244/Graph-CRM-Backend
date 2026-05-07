// routes/campaignAnalyticsRoutes.js
const express = require("express");
const router = express.Router();
const campaignController = require("../controllers/campaignAnalyticsController");

router.post("/campaign_analytics", campaignController.getCampaignAnalytics);
router.get("/campaign_analytics/campaigns", campaignController.getUniqueCampaigns);
module.exports = router;