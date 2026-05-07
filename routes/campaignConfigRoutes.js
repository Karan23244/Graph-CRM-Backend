// routes/campaignConfigRoutes.js
const express = require("express");
const router = express.Router();
const campaignController = require("../controllers/campaignconfigController");

router.get("/campaigns_list", campaignController.getCampaignList);
router.post("/campaign-config", campaignController.createCampaignConfig);
router.get("/campaign-config/:id", campaignController.getCampaignConfig);
router.put("/campaign-config/:id", campaignController.updateCampaignConfig);
router.get(
  "/configured-campaigns",
  campaignController.getConfiguredCampaigns
);
module.exports = router;