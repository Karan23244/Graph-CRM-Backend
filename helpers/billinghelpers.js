const pool = require("../config/db");

/**
 * Normalize role
 */
exports.normalizeRole = (role) => {
  if (Array.isArray(role)) role = role[0];

  if (["publisher", "publisher_manager"].includes(role)) return role;
  if (["advertiser", "advertiser_manager"].includes(role)) return role;

  return role; // admin or others
};

/**
 * Get sub-admin user IDs
 */
exports.getSubAdminIds = async (managerId) => {
  const [rows] = await pool.query(
    "SELECT sub_admin_id FROM manager_subadmins WHERE manager_id = ?",
    [managerId],
  );
  return rows.map((r) => r.sub_admin_id);
};
