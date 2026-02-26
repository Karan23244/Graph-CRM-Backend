const express = require("express");
const {
  getAdvertiserBillingData,
  saveAdvertiserBilling,
  lockAdvertiserBilling,
  listAdvertiserBilling,
} = require("../controllers/advertiserBillingController");
const router = express.Router();

router.post("/advertiser-data", getAdvertiserBillingData);
router.post("/advertiser-save", saveAdvertiserBilling);
router.post("/advertiser-lock", lockAdvertiserBilling);
router.post("/advertiser", listAdvertiserBilling);

module.exports = router;

