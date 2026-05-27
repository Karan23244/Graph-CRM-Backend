const express = require("express");
const { getRevenueAnalytics } = require("../controllers/analyticsController");
const router = express.Router();

router.post("/revenue", getRevenueAnalytics);
module.exports = router;
