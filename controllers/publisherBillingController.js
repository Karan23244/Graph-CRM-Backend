const pool = require("../config/db");

/* =====================================================
   FETCH PUBLISHER BILLING (SNAPSHOT / LIVE)
===================================================== */
exports.getPublisherBillingData = async (req, res) => {
  const { id: pub_id, month } = req.body; // month = "2025-04"

  if (!pub_id || !month) {
    return res
      .status(400)
      .json({ success: false, message: "pub_id and month are required" });
  }

  // Derive date range from "YYYY-MM"
  const [year, mon] = month.split("-");
  const startDate = `${year}-${mon}-01`;
  const endDate = new Date(year, Number(mon), 0).toISOString().slice(0, 10); // last day

  const sql = `
SELECT
  a.id AS adv_data_id,
  a.pid,
  a.campaign_id,
  a.pub_id,
  a.shared_date,

  COALESCE(v.campaign_name, a.campaign_name) AS campaign_name,
  COALESCE(v.geo, a.geo) AS geo,
  COALESCE(v.os, a.os) AS os,
  COALESCE(v.payable_event, a.payable_event) AS payable_event,
  COALESCE(v.pay_out, a.pay_out) AS pay_out,
  COALESCE(v.adv_total_no, a.adv_total_no) AS adv_total_no,
  COALESCE(v.pub_Apno, a.pub_Apno) AS pub_Apno,
  COALESCE(v.vertical, a.vertical) AS vertical,

CASE 
  WHEN v.is_verified = 2 THEN 'locked'
  WHEN v.is_verified = 1 THEN 'verified'
  ELSE 'unverified'
END AS status

FROM adv_data a
LEFT JOIN pub_data_verified v 
  ON a.id = v.adv_data_id

WHERE a.pub_id = ?
  AND STR_TO_DATE(a.shared_date, '%Y-%m-%d') BETWEEN ? AND ?

UNION

-- 🔥 include standalone verified rows (like testpid)

SELECT
  v.adv_data_id,
  v.pid,
  v.campaign_id,
  v.pub_id,
  v.shared_date,

  v.campaign_name,
  v.geo,
  v.os,
  v.payable_event,
  v.pay_out,
  v.adv_total_no,
  v.pub_Apno,
  v.vertical,

  CASE 
  WHEN v.is_verified = 2 THEN 'locked'
  WHEN v.is_verified = 1 THEN 'verified'
  ELSE 'unverified'
END AS status

FROM pub_data_verified v

WHERE v.adv_data_id IS NULL
  AND v.pub_id = ?
  AND STR_TO_DATE(v.shared_date, '%Y-%m-%d') BETWEEN ? AND ?

ORDER BY campaign_id, pid;
  `;

  try {
    const [rows] = await pool.query(sql, [
      pub_id,
      startDate,
      endDate,
      pub_id,
      startDate,
      endDate, // for UNION
    ]);
    return res.json({ success: true, data: rows });
  } catch (err) {
    console.error("publisher-data error:", err);
    return res
      .status(500)
      .json({ success: false, message: "DB error", error: err.message });
  }
};
/* =====================================================
   SAVE PUBLISHER BILLING
===================================================== */
exports.savePublisherBilling = async (req, res) => {
  const {
    adv_data_id,
    pid,
    campaign_id,
    pub_id,
    shared_date,
    campaign_name,
    geo,
    os,
    payable_event,
    pay_out,
    adv_total_no,
    pub_Apno,
    vertical,
    billing_month,
  } = req.body;

  // ── NEW: adv_data_id is optional now (manually added PIDs won't have one)
  const sql = adv_data_id
    ? `
        INSERT INTO pub_data_verified
          (adv_data_id, pid, campaign_id, pub_id, shared_date,
           campaign_name, geo, os, payable_event, pay_out,
           adv_total_no, pub_Apno, vertical, is_verified, verified_at,billing_month)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NOW(), ?)
        ON DUPLICATE KEY UPDATE
          pid           = VALUES(pid),
          campaign_id   = VALUES(campaign_id),
          pub_id        = VALUES(pub_id),
          shared_date   = VALUES(shared_date),
          campaign_name = VALUES(campaign_name),
          geo           = VALUES(geo),
          os            = VALUES(os),
          payable_event = VALUES(payable_event),
          pay_out       = VALUES(pay_out),
          adv_total_no  = VALUES(adv_total_no),
          pub_Apno      = VALUES(pub_Apno),
          vertical      = VALUES(vertical),
          is_verified   = 1,
          verified_at   = NOW();
      `
    : `
        INSERT INTO pub_data_verified
          (pid, campaign_id, pub_id, shared_date,
           campaign_name, geo, os, payable_event, pay_out,
           adv_total_no, pub_Apno, vertical, is_verified, verified_at, billing_month)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NOW(), ?);
      `;

  const params = adv_data_id
    ? [
        adv_data_id,
        pid,
        campaign_id,
        pub_id,
        shared_date,
        campaign_name,
        geo,
        os,
        payable_event,
        pay_out,
        adv_total_no,
        pub_Apno,
        vertical,
        billing_month,
      ]
    : [
        pid,
        campaign_id,
        pub_id,
        shared_date,
        campaign_name,
        geo,
        os,
        payable_event,
        pay_out,
        adv_total_no,
        pub_Apno,
        vertical,
        billing_month,
      ];

  try {
    const [result] = await pool.query(sql, params);

    // ── NEW: return the inserted id so the frontend can store it
    return res.json({
      success: true,
      insertId: result.insertId || null,
      title: "PID Verified",
      message: `PID "${pid}" has been locked successfully.`,
    });
  } catch (err) {
    console.error("publisher-verify-pid error:", err);
    return res
      .status(500)
      .json({ success: false, message: "DB error", error: err.message });
  }
};
/* =====================================================
   LOCK PUBLISHER BILLING
===================================================== */
exports.lockPublisherBilling = async (req, res) => {
  const { pub_id, month } = req.body;

  try {
    // ✅ ONLY update pub_data_verified
    await pool.query(
      `
      UPDATE pub_data_verified
      SET is_verified = 2
      WHERE pub_id = ?
      AND DATE_FORMAT(shared_date, '%Y-%m') = ?
      `,
      [pub_id, month],
    );

    res.json({ success: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false });
  }
};
/* =====================================================
   VERIFY SINGLE ROW
===================================================== */
exports.verifyPublisherBillingRow = async (req, res) => {
  console.log("Verifying publisher billing row request");
  const { billing_id } = req.body;
  console.log("Verifying publisher billing row:", billing_id);
  try {
    await pool.query(
      `
      UPDATE publisher_billing
      SET status='verified'
      WHERE id=?
      `,
      [billing_id],
    );

    res.json({ success: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false });
  }
};

/* =====================================================
   LIST LOCKED BILLINGS
===================================================== */
exports.listPublisherBilling = async (req, res) => {
  const { user_id, role = [], assigned_subadmins = [], month } = req.body;

  console.log("Publisher billing request:", {
    user_id,
    role,
    assigned_subadmins,
    month,
  });

  try {
    let rows;

    // ✅ ADMIN → ALL LOCKED (WITH MONTH FILTER)
    if (role.includes("admin")) {
      const query = `
        SELECT *
        FROM publisher_billing pb
        WHERE pb.status = 'locked'
        ${month ? "AND pb.month = ?" : ""}
        ORDER BY pb.month DESC
      `;

      const [result] = await pool.query(query, month ? [month] : []);

      rows = result;
    }

    // ✅ PUBLISHER MANAGER
    else if (role.includes("publisher_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      const query = `
        SELECT pb.*
        FROM publisher_billing pb
        JOIN publids p ON pb.pub_id = p.pub_id
        WHERE pb.status = 'locked'
          AND p.user_id IN (?)
          ${month ? "AND pb.month = ?" : ""}
        ORDER BY pb.month DESC
      `;

      const params = month ? [allowedUsers, month] : [allowedUsers];

      const [result] = await pool.query(query, params);

      rows = result;
    } else {
      return res.status(403).json({ message: "Unauthorized" });
    }

    res.json({
      success: true,
      data: rows,
    });
  } catch (err) {
    console.error("Publisher billing error:", err);
    res.status(500).json({ success: false, message: "Server error" });
  }
};
