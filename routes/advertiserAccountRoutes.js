const express = require("express");
const router = express.Router();

const {
  getAdvertiserAccount,
  updateAdvertiserAccount,
  createManualAdvertiserAccount
} = require("../controllers/advertiserAccountController");

router.post("/account", getAdvertiserAccount);
router.put("/account/update", updateAdvertiserAccount);
router.post("/account/manual", createManualAdvertiserAccount);
module.exports = router;
