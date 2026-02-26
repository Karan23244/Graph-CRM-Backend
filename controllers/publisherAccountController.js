// GET publisher account
const pool = require("../config/db");



exports.getPublisherAccount = async (req, res) => {
  const { month } = req.query;

  try {
    // 1️⃣ Calculate totals from locked billing
    const [totals] = await pool.query(
      `
      SELECT 
        pub_id,
        month,
        SUM(pub_apno * pub_payout) AS total_amount
      FROM publisher_billing
      WHERE status = 'locked'
      ${month ? "AND month = ?" : ""}
      GROUP BY pub_id, month
    `,
      month ? [month] : [],
    );

    // 2️⃣ Upsert into publisher_account
    for (const row of totals) {
      await pool.query(
        `
        INSERT INTO publisher_account (pub_id, month, total_amount)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE total_amount = ?
      `,
        [row.pub_id, row.month, row.total_amount, row.total_amount],
      );
    }

    // 3️⃣ Final data with publisher details
    const [finalData] = await pool.query(
      `
      SELECT 
        pa.*,
        p.pub_name,
        p.note
      FROM publisher_account pa
      JOIN publids p ON pa.pub_id = p.pub_id
      ${month ? "WHERE pa.month = ?" : ""}
      ORDER BY pa.month DESC
    `,
      month ? [month] : [],
    );

    res.json(finalData);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server Error" });
  }
};
// UPDATE publisher account
exports.updatePublisherAccount = async (req, res) => {
  const {
    pub_id,
    month,
    payment_terms,
    payment_status,
    amount_paid,
    invoice_number,
    invoice_date,
    payment_date,
  } = req.body;

  try {
    await pool.query(
      `
      UPDATE publisher_account
      SET
        payment_terms = ?,
        payment_status = ?,
        amount_paid = ?,
        invoice_number = ?,
        invoice_date = ?,
        payment_date = ?
      WHERE pub_id = ? AND month = ?
      `,
      [
        payment_terms,
        payment_status,
        amount_paid,
        invoice_number,
        invoice_date,
        payment_date,
        pub_id,
        month,
      ],
    );

    res.json({ message: "Updated Successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Update failed" });
  }
};
