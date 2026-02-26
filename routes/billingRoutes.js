const express = require("express");
const {
  getBillingDropdowns,
  getBillingData,
  getPublisherExternalBilling,
} = require("../controllers/billingController");
const router = express.Router();

router.post("/dropdowns", getBillingDropdowns);
router.post("/data", getBillingData);
router.post("/publisher-external-data", getPublisherExternalBilling);
module.exports = router;
