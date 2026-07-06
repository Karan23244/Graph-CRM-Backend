// ==========================
// 📁 controllers/analyticsController.js
// ==========================
const service = require("../services/analyticsService");

exports.getRevenueAnalytics = async (req, res) => {
  try {
    const { user_id, role, month, assign_subadmin } = req.body;
    if (!month) {
      return res.status(400).json({
        error: "month required",
      });
    }

    // 🔹 Admin → no filter
    // 🔹 Others → filter by assign_subadmin
    const pubIds = await service.getPublisherIds(
      user_id,
      role,
      assign_subadmin,
    );

    const [geo, vertical, os] = await Promise.all([
      service.getTopGeo(pubIds, month),
      service.getTopVertical(pubIds, month),
      service.getTopOS(pubIds, month),
    ]);

    res.json({ geo, vertical, os });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Internal Server Error",
    });
  }
};
