const express    = require('express');
const router     = express.Router();

const campaignPublisherMapController = require("../controllers/testingController");
// router.post("/campaign-publisher-map",              campaignPublisherMapController.createCampaignPublisherMap);
// router.get("/campaign-publisher-map",               campaignPublisherMapController.getCampaignPublisherMap);
// router.put("/campaign-publisher-map/:access_id",    campaignPublisherMapController.updateCampaignPublisherMap);
// router.delete("/campaign-publisher-map/:access_id", campaignPublisherMapController.deleteCampaignPublisherMap);
// router.get("/getassigncampaign",    campaignPublisherMapController.getAssignCampaign);
// router.post("/get-allpub", campaignPublisherMapController.getPublishersByCampaign);
router.post("/update-pubid", campaignPublisherMapController.updatePublisher);
router.get("/publisher-status", campaignPublisherMapController.getPublisherStatus);
router.get("/get-Namepub", campaignPublisherMapController.getNamePublishers)
module.exports = router