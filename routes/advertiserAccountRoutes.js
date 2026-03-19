const express = require("express");
const router = express.Router();

const {
  getAdvertiserAccount,
  updateAdvertiserAccount,
} = require("../controllers/advertiserAccountController");

router.post("/account", getAdvertiserAccount);
router.put("/account/update", updateAdvertiserAccount);

module.exports = router;
