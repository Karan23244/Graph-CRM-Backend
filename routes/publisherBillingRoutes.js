const express = require("express");
const {
  getPublisherBillingData,
  savePublisherBilling,
  lockPublisherBilling,
  verifyPublisherBillingRow,
  listPublisherBilling,
} = require("../controllers/publisherBillingController");
const router = express.Router();

router.post("/publisher-data", getPublisherBillingData);
router.post("/publisher-save", savePublisherBilling);
router.post("/publisher-lock", lockPublisherBilling);
router.post("/publisher-verify-row", verifyPublisherBillingRow);
router.post("/publisher", listPublisherBilling);

module.exports = router;

