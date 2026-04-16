const express = require("express");
const {
  getBillingDropdowns,
  getBillingData,
  getPublisherExternalBilling,
  getOldPublisherExternalBilling,
} = require("../controllers/billingController");
const router = express.Router();

router.post("/dropdowns", getBillingDropdowns);
router.post("/data", getBillingData);
router.post("/publisher-external-data", getPublisherExternalBilling);
router.post("/old-publisher-external-data", getOldPublisherExternalBilling);
module.exports = router;
