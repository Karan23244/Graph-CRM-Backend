const express = require("express");
const router = express.Router();

const {
  getPublisherAccount,
  updatePublisherAccount,
} = require("../controllers/publisherAccountController");

router.post("/account", getPublisherAccount);
router.put("/account/update", updatePublisherAccount);

module.exports = router;
