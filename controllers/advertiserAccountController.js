const pool = require("../config/db");

// GET advertiser account
exports.getAdvertiserAccount = async (req, res) => {
  const { month } = req.query; // yyyy-mm

  try {
    // 1️⃣ Calculate totals
    const [totals] = await pool.query(
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

    // 2️⃣ Upsert into advertiser_account
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

    // 3️⃣ Final data
    const [finalData] = await pool.query(
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

    res.json(finalData);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server Error" });
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
