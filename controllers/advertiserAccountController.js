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
          LEFT JOIN advids ad ON aa.adv_id = ad.adv_id
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
          LEFT JOIN advids ad ON aa.adv_id = ad.adv_id
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
        LEFT JOIN advids ad ON aa.adv_id = ad.adv_id
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
exports.createManualAdvertiserAccount = async (req, res) => {
  console.log("MANUAL API HIT", req.body);
  try {
    const {
      adv_id,
      month,
      total_amount,
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

    if (!adv_id || !month) {
      return res.status(400).json({
        success: false,
        message: "adv_id and month are required",
      });
    }

  //   // Check existing record
  //   const [existing] = await pool.query(
  //     `
  // SELECT id
  // FROM advertiser_account
  // WHERE adv_id = ?
  //   AND month = ?
  //   AND is_manual = 1
  // `,
  //     [adv_id, month],
  //   );

  //   if (existing.length) {
  //     return res.status(400).json({
  //       success: false,
  //       message: "Manual record already exists",
  //     });
  //   }

  //   console.log("EXISTING:", existing);
  //   if (existing.length) {
  //     return res.status(400).json({
  //       success: false,
  //       message: "Record already exists for this advertiser and month",
  //     });
  //   }

    await pool.query(
      `
      INSERT INTO advertiser_account (
        adv_id,
        month,
        total_amount,
        payment_terms,
        payment_status,
        amount_raised,
        invoice_number,
        invoice_date,
        invoice_from,
        invoice_to,
        currency,
        payment_date,
        is_manual
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
      `,
      [
        adv_id,
        month,
        total_amount || 0,
        payment_terms || null,
        payment_status || "Pending",
        amount_raised || 0,
        invoice_number || null,
        invoice_date || null,
        invoice_from || null,
        invoice_to || null,
        currency || "USD",
        payment_date || null,
      ],
    );

    res.json({
      success: true,
      message: "Manual record created successfully",
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      message: "Server error",
    });
  }
};

exports.updateAdvertiserAccount = async (req, res) => {
  const { id, ...fields } = req.body;
  console.log("UPDATE API HIT", req.body);
  try {
    if (!id) {
      return res.status(400).json({
        success: false,
        message: "id is required",
      });
    }

    Object.keys(fields).forEach((key) => {
      if (fields[key] === undefined) {
        delete fields[key];
      }
    });

    const [existing] = await pool.query(
      `
  SELECT id
  FROM advertiser_account
  WHERE id = ?
  `,
      [id],
    );
    console.log("EXISTING:", existing);
    // Record exists → update
    if (existing.length > 0) {
      const keys = Object.keys(fields);

      if (keys.length) {
        const setClause = keys.map((key) => `${key} = ?`).join(", ");

        await pool.query(
          `
      UPDATE advertiser_account
      SET ${setClause}
      WHERE id = ?
      `,
          [...keys.map((k) => fields[k]), id],
        );
      }
    }

    // Record does not exist → create
    else {
      await pool.query(
        `
        INSERT INTO advertiser_account
        (
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
          payment_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          adv_id,
          month,
          fields.payment_terms || null,
          fields.payment_status || "Pending",
          fields.amount_raised || 0,
          fields.invoice_number || null,
          fields.invoice_date || null,
          fields.invoice_from || null,
          fields.invoice_to || null,
          fields.currency || "USD",
          fields.payment_date || null,
        ],
      );
    }

    // Fetch updated row
    const [rows] = await pool.query(
      `
SELECT
  id,
  adv_id,
  month,
  payment_terms,
  total_amount,
  payment_status,
  amount_raised,
  invoice_number,

  DATE_FORMAT(invoice_date,'%Y-%m-%d') AS invoice_date,
  DATE_FORMAT(invoice_from,'%Y-%m-%d') AS invoice_from,
  DATE_FORMAT(invoice_to,'%Y-%m-%d') AS invoice_to,
  DATE_FORMAT(payment_date,'%Y-%m-%d') AS payment_date,

  currency,
  is_manual,
  created_at,
  updated_at
FROM advertiser_account
WHERE id = ?
  `,
      [id],
    );
    res.json({
      success: true,
      data: rows[0],
    });
  } catch (err) {
    console.error(err);

    res.status(500).json({
      success: false,
      message: "Save failed",
    });
  }
};
