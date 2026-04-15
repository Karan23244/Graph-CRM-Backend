const express = require("express");
const {
  getPublisherBillingData,
  getoldPublisherBillingData,
  savePublisherBilling,
  lockPublisherBilling,
  verifyPublisherBillingRow,
  listPublisherBilling,
} = require("../controllers/publisherBillingController");
const router = express.Router();

router.post("/publisher-data", getPublisherBillingData);
router.post("/publisher-old-data", getoldPublisherBillingData);
router.post("/publisher-verify-pid", savePublisherBilling);
router.post("/publisher-lock", lockPublisherBilling);
router.post("/publisher-verify-row", verifyPublisherBillingRow);
router.post("/publisher", listPublisherBilling);

module.exports = router;

