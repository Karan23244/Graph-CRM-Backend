const express = require("express");
const router = express.Router();

const {
  getAdvertiserDashboardData,
} = require("../controllers/dashboardController");

router.post("/dashboard-adv-data", getAdvertiserDashboardData);

module.exports = router;
