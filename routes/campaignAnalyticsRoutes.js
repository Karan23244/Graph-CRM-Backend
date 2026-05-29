// routes/campaignAnalyticsRoutes.js
const express = require("express");
const router = express.Router();
const campaignController = require("../controllers/campaignAnalyticsController");

router.post("/campaign_analytics", campaignController.getCampaignAnalytics);
router.post("/campaign_analytics/campaigns", campaignController.getUniqueCampaigns);
router.delete(
  "/campaign_analytics/delete-campaign-data",
  campaignController.deleteCampaignData
);
module.exports = router;