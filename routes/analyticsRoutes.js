const express = require("express");
const { getRevenueAnalytics, getPidHistory } = require("../controllers/analyticsController");
const router = express.Router();

router.post("/revenue", getRevenueAnalytics);
router.get("/pid-history/:pid", getPidHistory);
module.exports = router;
