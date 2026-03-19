const pool = require("../config/db");

// GET advertiser account
exports.getAdvertiserAccount = async (req, res) => {
  const { user_id, role = [], assigned_subadmins = [], month } = req.body;

  console.log("Advertiser account request:", {
    user_id,
    role,
    assigned_subadmins,
    month,
  });

  try {
    let totals = [];

    /* ============================= */
    /* 1️⃣ ROLE-BASED AGGREGATION */
    /* ============================= */

    // ✅ ADMIN + ACCOUNTS → ALL DATA
    if (role.includes("admin") || role.includes("accounts")) {
      const [result] = await pool.query(
        `
        SELECT 
          adv_id,
          month,
          SUM(approved_no * adv_payout) AS total_amount
        FROM advertiser_billing
        WHERE status = 'locked'
        ${month ? "AND month = ?" : ""}
        GROUP BY adv_id, month
      `,
        month ? [month] : [],
      );

      totals = result;
    }

    // ✅ ADVERTISER MANAGER
    else if (role.includes("advertiser_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      const [result] = await pool.query(
        `
        SELECT 
          ab.adv_id,
          ab.month,
          SUM(ab.approved_no * ab.adv_payout) AS total_amount
        FROM advertiser_billing ab
        JOIN advids ad ON ab.adv_id = ad.adv_id
        WHERE ab.status = 'locked'
          AND ad.user_id IN (?)
          ${month ? "AND ab.month = ?" : ""}
        GROUP BY ab.adv_id, ab.month
      `,
        month ? [allowedUsers, month] : [allowedUsers],
      );

      totals = result;
    }

    // ✅ NORMAL ADVERTISER
    else if (role.includes("advertiser")) {
      const [result] = await pool.query(
        `
        SELECT 
          ab.adv_id,
          ab.month,
          SUM(ab.approved_no * ab.adv_payout) AS total_amount
        FROM advertiser_billing ab
        JOIN advids ad ON ab.adv_id = ad.adv_id
        WHERE ab.status = 'locked'
          AND ad.user_id = ?
          ${month ? "AND ab.month = ?" : ""}
        GROUP BY ab.adv_id, ab.month
      `,
        month ? [user_id, month] : [user_id],
      );

      totals = result;
    } else {
      return res.status(403).json({ message: "Unauthorized" });
    }

    /* ============================= */
    /* 2️⃣ UPSERT */
    /* ============================= */

    for (const row of totals) {
      await pool.query(
        `
        INSERT INTO advertiser_account (adv_id, month, total_amount)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE total_amount = ?
      `,
        [row.adv_id, row.month, row.total_amount, row.total_amount],
      );
    }

    /* ============================= */
    /* 3️⃣ FINAL FETCH */
    /* ============================= */

    let finalData = [];

    // ✅ ADMIN + ACCOUNTS
    if (role.includes("admin") || role.includes("accounts")) {
      const [result] = await pool.query(
        `
        SELECT 
          aa.*,
          ad.adv_name,
          ad.note
        FROM advertiser_account aa
        JOIN advids ad ON aa.adv_id = ad.adv_id
        ${month ? "WHERE aa.month = ?" : ""}
        ORDER BY aa.month DESC
      `,
        month ? [month] : [],
      );

      finalData = result;
    }

    // ✅ MANAGER
    else if (role.includes("advertiser_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      const [result] = await pool.query(
        `
        SELECT 
          aa.*,
          ad.adv_name,
          ad.note
        FROM advertiser_account aa
        JOIN advids ad ON aa.adv_id = ad.adv_id
        WHERE ad.user_id IN (?)
        ${month ? "AND aa.month = ?" : ""}
        ORDER BY aa.month DESC
      `,
        month ? [allowedUsers, month] : [allowedUsers],
      );

      finalData = result;
    }

    // ✅ NORMAL ADVERTISER
    else if (role.includes("advertiser")) {
      const [result] = await pool.query(
        `
        SELECT 
          aa.*,
          ad.adv_name,
          ad.note
        FROM advertiser_account aa
        JOIN advids ad ON aa.adv_id = ad.adv_id
        WHERE ad.user_id = ?
        ${month ? "AND aa.month = ?" : ""}
        ORDER BY aa.month DESC
      `,
        month ? [user_id, month] : [user_id],
      );

      finalData = result;
    }

    res.json({
      success: true,
      data: finalData,
    });
  } catch (err) {
    console.error("Advertiser account error:", err);
    res.status(500).json({
      success: false,
      message: "Server Error",
    });
  }
};

// UPDATE advertiser account
exports.updateAdvertiserAccount = async (req, res) => {
  const {
    adv_id,
    month,
    payment_terms,
    payment_status,
    amount_raised,
    invoice_number,
    invoice_date,
    invoice_from,
    invoice_to,
    currency,
    payment_date,
  } = req.body;

  try {
    await pool.query(
      `
      UPDATE advertiser_account
      SET 
        payment_terms = ?,
        payment_status = ?,
        amount_raised = ?,
        invoice_number = ?,
        invoice_date = ?,
        invoice_from = ?,
        invoice_to = ?,
        currency = ?,
        payment_date = ?
      WHERE adv_id = ? AND month = ?
      `,
      [
        payment_terms,
        payment_status,
        amount_raised,
        invoice_number,
        invoice_date,
        invoice_from,
        invoice_to,
        currency,
        payment_date,
        adv_id,
        month,
      ],
    );

    res.json({ message: "Updated Successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Update failed" });
  }
};
