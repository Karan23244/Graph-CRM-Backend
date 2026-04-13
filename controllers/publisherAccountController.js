// GET publisher account
const pool = require("../config/db");

// exports.getPublisherAccount = async (req, res) => {
//   const { month } = req.query;

//   try {
//     // 1️⃣ Calculate totals from locked billing
//     const [totals] = await pool.query(
//       `
//       SELECT
//         pub_id,
//         month,
//         SUM(pub_apno * pub_payout) AS total_amount
//       FROM publisher_billing
//       WHERE status = 'locked'
//       ${month ? "AND month = ?" : ""}
//       GROUP BY pub_id, month
//     `,
//       month ? [month] : [],
//     );

//     // 2️⃣ Upsert into publisher_account
//     for (const row of totals) {
//       await pool.query(
//         `
//         INSERT INTO publisher_account (pub_id, month, total_amount)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE total_amount = ?
//       `,
//         [row.pub_id, row.month, row.total_amount, row.total_amount],
//       );
//     }

//     // 3️⃣ Final data with publisher details
//     const [finalData] = await pool.query(
//       `
//       SELECT
//         pa.*,
//         p.pub_name,
//         p.note
//       FROM publisher_account pa
//       JOIN publids p ON pa.pub_id = p.pub_id
//       ${month ? "WHERE pa.month = ?" : ""}
//       ORDER BY pa.month DESC
//     `,
//       month ? [month] : [],
//     );

//     res.json(finalData);
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Server Error" });
//   }
// };

// exports.getPublisherAccount = async (req, res) => {
//   const {
//     user_id,
//     role = [],
//     assigned_subadmins = [],
//     month,
//   } = req.body;

//   console.log("Publisher account request:", {
//     user_id,
//     role,
//     assigned_subadmins,
//     month,
//   });

//   try {
//     let totals = [];

//     /* ============================= */
//     /* 1️⃣ ROLE-BASED AGGREGATION */
//     /* ============================= */

//     // ✅ ADMIN + ACCOUNTS → ALL DATA
//     if (role.includes("admin") || role.includes("accounts")) {
//       const [result] = await pool.query(
//         `
//         SELECT
//           pub_id,
//           month,
//           SUM(pub_apno * pub_payout) AS total_amount
//         FROM publisher_billing
//         WHERE status = 'locked'
//         ${month ? "AND month = ?" : ""}
//         GROUP BY pub_id, month
//       `,
//         month ? [month] : []
//       );

//       totals = result;
//     }

//     // ✅ PUBLISHER MANAGER → LIMITED DATA
//     else if (role.includes("publisher_manager")) {
//       const allowedUsers = [user_id, ...assigned_subadmins];

//       const [result] = await pool.query(
//         `
//         SELECT
//           pb.pub_id,
//           pb.month,
//           SUM(pb.pub_apno * pb.pub_payout) AS total_amount
//         FROM publisher_billing pb
//         JOIN publids p ON pb.pub_id = p.pub_id
//         WHERE pb.status = 'locked'
//           AND p.user_id IN (?)
//           ${month ? "AND pb.month = ?" : ""}
//         GROUP BY pb.pub_id, pb.month
//       `,
//         month ? [allowedUsers, month] : [allowedUsers]
//       );

//       totals = result;
//     }

//     // ✅ NORMAL PUBLISHER → ONLY OWN DATA
//     else if (role.includes("publisher")) {
//       const [result] = await pool.query(
//         `
//         SELECT
//           pb.pub_id,
//           pb.month,
//           SUM(pb.pub_apno * pb.pub_payout) AS total_amount
//         FROM publisher_billing pb
//         JOIN publids p ON pb.pub_id = p.pub_id
//         WHERE pb.status = 'locked'
//           AND p.user_id = ?
//           ${month ? "AND pb.month = ?" : ""}
//         GROUP BY pb.pub_id, pb.month
//       `,
//         month ? [user_id, month] : [user_id]
//       );

//       totals = result;
//     }

//     // ❌ UNAUTHORIZED
//     else {
//       return res.status(403).json({ message: "Unauthorized" });
//     }

//     /* ============================= */
//     /* 2️⃣ UPSERT INTO ACCOUNT TABLE */
//     /* ============================= */

//     for (const row of totals) {
//       await pool.query(
//         `
//         INSERT INTO publisher_account (pub_id, month, total_amount)
//         VALUES (?, ?, ?)
//         ON DUPLICATE KEY UPDATE total_amount = ?
//       `,
//         [row.pub_id, row.month, row.total_amount, row.total_amount]
//       );
//     }

//     /* ============================= */
//     /* 3️⃣ FINAL FETCH (ROLE-BASED) */
//     /* ============================= */

//     let finalData = [];

//     // ✅ ADMIN + ACCOUNTS
//     if (role.includes("admin") || role.includes("accounts")) {
//       const [result] = await pool.query(
//         `
//         SELECT
//           pa.*,
//           p.pub_name,
//           p.note
//         FROM publisher_account pa
//         JOIN publids p ON pa.pub_id = p.pub_id
//         ${month ? "WHERE pa.month = ?" : ""}
//         ORDER BY pa.month DESC
//       `,
//         month ? [month] : []
//       );

//       finalData = result;
//     }

//     // ✅ PUBLISHER MANAGER
//     else if (role.includes("publisher_manager")) {
//       const allowedUsers = [user_id, ...assigned_subadmins];

//       const [result] = await pool.query(
//         `
//         SELECT
//           pa.*,
//           p.pub_name,
//           p.note
//         FROM publisher_account pa
//         JOIN publids p ON pa.pub_id = p.pub_id
//         WHERE p.user_id IN (?)
//         ${month ? "AND pa.month = ?" : ""}
//         ORDER BY pa.month DESC
//       `,
//         month ? [allowedUsers, month] : [allowedUsers]
//       );

//       finalData = result;
//     }

//     // ✅ NORMAL PUBLISHER
//     else if (role.includes("publisher")) {
//       const [result] = await pool.query(
//         `
//         SELECT
//           pa.*,
//           p.pub_name,
//           p.note
//         FROM publisher_account pa
//         JOIN publids p ON pa.pub_id = p.pub_id
//         WHERE p.user_id = ?
//         ${month ? "AND pa.month = ?" : ""}
//         ORDER BY pa.month DESC
//       `,
//         month ? [user_id, month] : [user_id]
//       );

//       finalData = result;
//     }

//     /* ============================= */
//     /* 4️⃣ RESPONSE */
//     /* ============================= */

//     res.json({
//       success: true,
//       data: finalData,
//     });

//   } catch (err) {
//     console.error("Publisher account error:", err);
//     res.status(500).json({
//       success: false,
//       message: "Server Error",
//     });
//   }
// };

// UPDATE publisher account

exports.getPublisherAccount = async (req, res) => {
  const { user_id, role = [], assigned_subadmins = [], month } = req.body;

  try {
    let totals = [];

    /* ============================= */
    /* 1️⃣ ROLE-BASED AGGREGATION   */
    /* ============================= */

    // ✅ ADMIN + ACCOUNTS → ALL DATA
    if (role.includes("admin") || role.includes("accounts")) {
      const [result] = await pool.query(
        `
        SELECT 
          pub_id,
          billing_month AS month,
          SUM(
            CAST(pub_Apno AS DECIMAL(10,2)) * 
            CAST(pay_out AS DECIMAL(10,2))
          ) AS total_amount
        FROM pub_data_verified
        WHERE is_verified = 2
        ${month ? "AND billing_month = ?" : ""}
        GROUP BY pub_id, billing_month
      `,
        month ? [month] : [],
      );

      totals = result;
    }

    // ✅ PUBLISHER MANAGER
    else if (role.includes("publisher_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      const [result] = await pool.query(
        `
        SELECT 
          pb.pub_id,
          pb.billing_month AS month,
          SUM(
            CAST(pb.pub_Apno AS DECIMAL(10,2)) * 
            CAST(pb.pay_out AS DECIMAL(10,2))
          ) AS total_amount
        FROM pub_data_verified pb
        JOIN publids p ON pb.pub_id = p.pub_id
        WHERE pb.is_verified = 2
          AND p.user_id IN (?)
          ${month ? "AND pb.billing_month = ?" : ""}
        GROUP BY pb.pub_id, pb.billing_month
      `,
        month ? [allowedUsers, month] : [allowedUsers],
      );

      totals = result;
    }

    // ✅ NORMAL PUBLISHER
    else if (role.includes("publisher")) {
      const [result] = await pool.query(
        `
        SELECT 
          pb.pub_id,
          pb.billing_month AS month,
          SUM(
            CAST(pb.pub_Apno AS DECIMAL(10,2)) * 
            CAST(pb.pay_out AS DECIMAL(10,2))
          ) AS total_amount
        FROM pub_data_verified pb
        JOIN publids p ON pb.pub_id = p.pub_id
        WHERE pb.is_verified = 2
          AND p.user_id = ?
          ${month ? "AND pb.billing_month = ?" : ""}
        GROUP BY pb.pub_id, pb.billing_month
      `,
        month ? [user_id, month] : [user_id],
      );

      totals = result;
    }

    // ❌ UNAUTHORIZED
    else {
      return res.status(403).json({ message: "Unauthorized" });
    }

    /* ============================= */
    /* 2️⃣ BULK UPSERT              */
    /* ============================= */

    if (totals.length > 0) {
      const values = totals.map((row) => [
        row.pub_id,
        row.month,
        row.total_amount || 0,
      ]);

      await pool.query(
        `
        INSERT INTO publisher_account (pub_id, month, total_amount)
        VALUES ?
        ON DUPLICATE KEY UPDATE total_amount = VALUES(total_amount)
      `,
        [values],
      );
    }

    /* ============================= */
    /* 3️⃣ FINAL FETCH              */
    /* ============================= */

    let finalData = [];

    if (role.includes("admin") || role.includes("accounts")) {
      const [result] = await pool.query(
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

      finalData = result;
    } else if (role.includes("publisher_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      const [result] = await pool.query(
        `
        SELECT 
          pa.*,
          p.pub_name,
          p.note
        FROM publisher_account pa
        JOIN publids p ON pa.pub_id = p.pub_id
        WHERE p.user_id IN (?)
        ${month ? "AND pa.month = ?" : ""}
        ORDER BY pa.month DESC
      `,
        month ? [allowedUsers, month] : [allowedUsers],
      );

      finalData = result;
    } else if (role.includes("publisher")) {
      const [result] = await pool.query(
        `
        SELECT 
          pa.*,
          p.pub_name,
          p.note
        FROM publisher_account pa
        JOIN publids p ON pa.pub_id = p.pub_id
        WHERE p.user_id = ?
        ${month ? "AND pa.month = ?" : ""}
        ORDER BY pa.month DESC
      `,
        month ? [user_id, month] : [user_id],
      );

      finalData = result;
    }

    /* ============================= */
    /* 4️⃣ RESPONSE                */
    /* ============================= */

    return res.json({
      success: true,
      data: finalData,
    });
  } catch (err) {
    console.error("Publisher account error:", err);
    return res.status(500).json({
      success: false,
      message: "Server Error",
    });
  }
};

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
