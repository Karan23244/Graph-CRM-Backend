const pool = require("../config/db");
function isZeroValue(v) {
  return (
    v === null || v === undefined || String(v).trim() === "" || Number(v) === 0
  );
}

function isPositiveValue(v) {
  return (
    v !== null &&
    v !== undefined &&
    String(v).trim() !== "" &&
    !isNaN(Number(v)) &&
    Number(v) > 0
  );
}
/* =====================================================
   FETCH PUBLISHER BILLING (SNAPSHOT / LIVE)
===================================================== */
// exports.getPublisherBillingData = async (req, res) => {
//   const { id: pub_id, month } = req.body; // month = "2025-04"

//   if (!pub_id || !month) {
//     return res
//       .status(400)
//       .json({ success: false, message: "pub_id and month are required" });
//   }

//   // Derive date range from "YYYY-MM"
//   const [year, mon] = month.split("-");
//   const startDate = `${year}-${mon}-01`;
//   const endDate = new Date(year, Number(mon), 0).toISOString().slice(0, 10); // last day

//   const sql = `
// SELECT
//   a.id AS adv_data_id,
//   a.pid,
//   a.campaign_id,
//   a.pub_id,
//   a.shared_date,

//   COALESCE(v.campaign_name, a.campaign_name) AS campaign_name,
//   COALESCE(v.geo, a.geo) AS geo,
//   COALESCE(v.os, a.os) AS os,
//   COALESCE(v.payable_event, a.payable_event) AS payable_event,
//   COALESCE(v.pay_out, a.pay_out) AS pay_out,
//   COALESCE(v.adv_total_no, a.adv_total_no) AS adv_total_no,
//   COALESCE(v.pub_Apno, a.pub_Apno) AS pub_Apno,
//   COALESCE(v.vertical, a.vertical) AS vertical,

// CASE
//   WHEN v.is_verified = 2 THEN 'locked'
//   WHEN v.is_verified = 1 THEN 'verified'
//   ELSE 'unverified'
// END AS status

// FROM adv_data a
// LEFT JOIN pub_data_verified v
//   ON a.id = v.adv_data_id

// WHERE a.pub_id = ?
//   AND STR_TO_DATE(a.shared_date, '%Y-%m-%d') BETWEEN ? AND ?

// UNION

// -- 🔥 include standalone verified rows (like testpid)

// SELECT
//   v.adv_data_id,
//   v.pid,
//   v.campaign_id,
//   v.pub_id,
//   v.shared_date,

//   v.campaign_name,
//   v.geo,
//   v.os,
//   v.payable_event,
//   v.pay_out,
//   v.adv_total_no,
//   v.pub_Apno,
//   v.vertical,

//   CASE
//   WHEN v.is_verified = 2 THEN 'locked'
//   WHEN v.is_verified = 1 THEN 'verified'
//   ELSE 'unverified'
// END AS status

// FROM pub_data_verified v

// WHERE v.adv_data_id IS NULL
//   AND v.pub_id = ?
//   AND STR_TO_DATE(v.shared_date, '%Y-%m-%d') BETWEEN ? AND ?

// ORDER BY campaign_id, pid;
//   `;

//   try {
//     const [rows] = await pool.query(sql, [
//       pub_id,
//       startDate,
//       endDate,
//       pub_id,
//       startDate,
//       endDate, // for UNION
//     ]);
//     return res.json({ success: true, data: rows });
//   } catch (err) {
//     console.error("publisher-data error:", err);
//     return res
//       .status(500)
//       .json({ success: false, message: "DB error", error: err.message });
//   }
// };

exports.getPublisherBillingData = async (req, res) => {
  const { id: pub_id, month } = req.body;

  if (!pub_id || !month) {
    return res.status(400).json({
      success: false,
      message: "pub_id and month are required",
    });
  }
  console.log(
    "Fetching publisher billing data for pub_id:",
    pub_id,
    "month:",
    month,
  );
  function isZeroValue(v) {
    return (
      v === null ||
      v === "" ||
      v === undefined ||
      v === "0" ||
      Number(v || 0) === 0
    );
  }

  try {
    const [year, mon] = month.split("-");

    const startDate = `${year}-${mon}-01`;

    const lastDay = new Date(Number(year), Number(mon), 0).getDate();

    const endDate = `${year}-${mon}-${String(lastDay).padStart(2, "0")}`;

    console.log("Billing date range:", {
      month,
      startDate,
      endDate,
    });

    /* =====================================================
       ORIGINAL QUERY
    ===================================================== */

    const sql = `
SELECT
  a.id AS adv_data_id,
  a.pid,
  a.campaign_id,
  a.pub_id,
  a.shared_date,
  a.campaign_name,
  a.geo,
  a.os,
  a.payable_event,
  a.pay_out,
  a.adv_total_no,
  a.pub_Apno,
  a.vertical,
  'unverified' AS status
FROM adv_data a
WHERE a.pub_id = ?
  AND STR_TO_DATE(a.shared_date, '%Y-%m-%d') BETWEEN ? AND ?
  -- ✅ exclude any adv_data row that already has a verified/locked snapshot
  AND NOT EXISTS (
    SELECT 1 FROM pub_data_verified v
    WHERE v.adv_data_id = a.id
      AND v.is_verified IN (1,2)
  )

UNION

-- ✅ ALL verified/locked rows come purely from the snapshot table — never touch adv_data again
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
    WHEN v.is_verified = 3 THEN 'hold'
    WHEN v.is_verified = 2 THEN 'locked'
    WHEN v.is_verified = 1 THEN 'verified'
  END AS status
FROM pub_data_verified v
WHERE v.pub_id = ?
  AND v.billing_month = ?
  AND v.is_verified >= 1
ORDER BY campaign_id, pid;
`;

    const [rows] = await pool.query(sql, [
      pub_id, // adv_data
      startDate,
      endDate,

      pub_id, // pub_data_verified
      month,
    ]);

    const finalRows = [...rows];

    // check if this billing month is already fully closed
    const [[closedBilling]] = await pool.query(
      `
  SELECT
    CASE
      WHEN COUNT(*) > 0
       AND COUNT(*) = SUM(CASE WHEN is_verified = 2 THEN 1 ELSE 0 END)
      THEN 1
      ELSE 0
    END AS is_closed
  FROM pub_data_verified
  WHERE pub_id = ?
    AND billing_month = ?
  `,
      [pub_id, month],
    );
    /* =====================================================
       CHECK CARRY FORWARD
    ===================================================== */
    if (!closedBilling.is_closed) {
      const [advRows] = await pool.query(
        `
SELECT *
FROM adv_data
WHERE pub_id=?
`,
        [pub_id],
      );

      for (const adv of advRows) {
        const [[lastVerified]] = await pool.query(
          `
        SELECT *
        FROM pub_data_verified
        WHERE adv_data_id=?
        ORDER BY billing_month DESC
        LIMIT 1
        `,
          [adv.id],
        );
        if (!lastVerified) continue;

        /* already verified in current month */
        const [[exists]] = await pool.query(
          `
        SELECT id
        FROM pub_data_verified
        WHERE adv_data_id=?
        AND billing_month=?
        LIMIT 1
        `,
          [adv.id, month],
        );

        if (exists) continue;

        const advCarry =
          isZeroValue(lastVerified.adv_total_no) &&
          isPositiveValue(adv.adv_total_no);

        const pubCarry =
          isZeroValue(lastVerified.pub_Apno) && isPositiveValue(adv.pub_Apno);

        if (!advCarry && !pubCarry) {
          continue;
        }
        const duplicate = finalRows.find(
          (x) => x.adv_data_id == adv.id && x.carry_forward === true,
        );

        if (duplicate) continue;

        finalRows.push({
          adv_data_id: adv.id,

          pid: adv.pid,
          campaign_id: adv.campaign_id,
          pub_id: adv.pub_id,
          shared_date: adv.shared_date,

          campaign_name: `${adv.campaign_name} (${lastVerified.billing_month} Carry Forward)`,
          geo: adv.geo,
          os: adv.os,
          payable_event: adv.payable_event,
          pay_out: adv.pay_out,

          adv_total_no: advCarry ? adv.adv_total_no : "",

          pub_Apno: pubCarry ? adv.pub_Apno : "",

          vertical: adv.vertical,

          carry_forward: true,
          carried_from: lastVerified.billing_month,

          status: "carry_forward",
        });
      }
    }
    return res.json({
      success: true,
      data: finalRows,
    });
  } catch (err) {
    console.error("publisher-data error:", err);

    return res.status(500).json({
      success: false,
      message: "DB error",
      error: err.message,
    });
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
    carry_from,
  } = req.body;

  // ── NEW: adv_data_id is optional now (manually added PIDs won't have one)
  const sql = adv_data_id
    ? `
        INSERT INTO pub_data_verified
          (adv_data_id, pid, campaign_id, pub_id, shared_date,
           campaign_name, geo, os, payable_event, pay_out,
           adv_total_no, pub_Apno, vertical,carry_from, is_verified, verified_at,billing_month)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, 1, NOW(), ?)
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
           adv_total_no, pub_Apno, vertical,carry_from, is_verified, verified_at, billing_month)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, 1, NOW(), ?);
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
        carry_from,
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
        carry_from,
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
    await pool.query(
      `
      UPDATE pub_data_verified
      SET is_verified = 2
      WHERE pub_id = ?
        AND billing_month = ?
        AND is_verified = 1
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
  const { billing_id } = req.body;
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
exports.verifyAllPublisherBillingRows = async (req, res) => {
  try {
    const { records } = req.body;

    let verifiedCount = 0;
    let skippedHold = 0;

    for (const row of records) {
      // Skip held records
      const [[existing]] = await pool.query(
        `
        SELECT is_verified
        FROM pub_data_verified
        WHERE adv_data_id = ?
          AND billing_month = ?
        LIMIT 1
        `,
        [row.adv_data_id, row.billing_month],
      );

      if (existing?.is_verified === 3) {
        skippedHold++;
        continue;
      }

      await pool.query(
        `
        INSERT INTO pub_data_verified
        (
          adv_data_id,
          pid,
          campaign_id,
          pub_id,
          campaign_name,
          geo,
          os,
          payable_event,
          pay_out,
          adv_total_no,
          pub_Apno,
          vertical,
          billing_month,
          is_verified
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON DUPLICATE KEY UPDATE
          pay_out=VALUES(pay_out),
          adv_total_no=VALUES(adv_total_no),
          pub_Apno=VALUES(pub_Apno)
        `,
        [
          row.adv_data_id,
          row.pid,
          row.campaign_id,
          row.pub_id,
          row.campaign_name,
          row.geo,
          row.os,
          row.payable_event,
          row.pay_out,
          row.adv_total_no,
          row.pub_Apno,
          row.vertical,
          row.billing_month,
        ],
      );

      verifiedCount++;
    }

    return res.json({
      success: true,
      verified: verifiedCount,
      skipped_hold: skippedHold,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
};
exports.getPublisherMonthRevenue = async (req, res) => {
  try {
    const { month, user_id, assign_id = [] } = req.body;

    if (!month || !user_id) {
      return res.status(400).json({
        success: false,
        message: "month and user_id are required",
      });
    }

    // include current user also
    const allowedUsers = [Number(user_id), ...assign_id.map(Number)];

    const placeholders = allowedUsers.map(() => "?").join(",");

    const query = `
  SELECT
    p.pub_name,
    p.pub_id,
    l.username AS POC,
    COUNT(*) AS total_entries,
    ROUND(
      SUM(
        COALESCE(CAST(v.pub_Apno AS DECIMAL(18,4)),0) *
        COALESCE(CAST(v.pay_out AS DECIMAL(18,4)),0)
      ),
      2
    ) AS total_revenue
  FROM pub_data_verified v

  INNER JOIN publids p
    ON p.pub_id = CAST(v.pub_id AS UNSIGNED)

  LEFT JOIN login l
    ON l.id = p.user_id

WHERE v.billing_month = ?
  AND v.is_verified IN (1,2)
  AND p.user_id IN (${placeholders})

  GROUP BY
    p.pub_id,
    p.pub_name,
    l.username

  HAVING total_revenue > 0

  ORDER BY total_revenue DESC
`;

    const [rows] = await pool.query(query, [month, ...allowedUsers]);

    return res.status(200).json({
      success: true,
      month,
      total_publishers: rows.length,
      data: rows,
    });
  } catch (error) {
    console.error("getPublisherMonthRevenue:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

exports.holdPublisherPid = async (req, res) => {
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
    carry_from,
  } = req.body;

  console.log("Hold Publisher PID request:", {
    adv_data_id,
    billing_month,
  });

  try {
    // if adv_data_id exists, check whether already saved
    let existing = [];

    if (adv_data_id) {
      const [rows] = await pool.query(
        `
        SELECT id
        FROM pub_data_verified
        WHERE adv_data_id = ?
        AND billing_month = ?
        LIMIT 1
        `,
        [adv_data_id, billing_month],
      );

      existing = rows;
    }

    // already exists -> move to hold
    if (existing.length) {
      await pool.query(
        `
        UPDATE pub_data_verified
        SET is_verified = 3
        WHERE adv_data_id = ?
        AND billing_month = ?
        `,
        [adv_data_id, billing_month],
      );
    } else {
      // first time -> insert as hold
      await pool.query(
        `
        INSERT INTO pub_data_verified
        (
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
          carry_from,
          is_verified,
          verified_at,
          billing_month
        )
        VALUES
        (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 3, NOW(), ?
        )
        `,
        [
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
          carry_from || null,
          billing_month,
        ],
      );
    }

    return res.json({
      success: true,
      message: "PID moved to hold",
    });
  } catch (err) {
    console.error(err);

    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
};
/* =====================================================
   old data show
===================================================== */

/* =====================================================
   FETCH PUBLISHER BILLING (SNAPSHOT / LIVE)
===================================================== */
exports.getoldPublisherBillingData = async (req, res) => {
  let { id: pub_id, month } = req.body;
  month = month.trim();
  try {
    /* 0️⃣ snapshot exists? */
    const [[exists]] = await pool.query(
      `SELECT id FROM publisher_billing WHERE pub_id=? AND month=? LIMIT 1`,
      [pub_id, month],
    );

    let data = [];

    /* 1️⃣ FROM SNAPSHOT */
    if (exists) {
      const [rows] = await pool.query(
        `
        SELECT
          b.id AS billing_id,
           b.status, 
          b.campaign_name,
          b.vertical,
          b.geo,
          b.os,
          b.payable_event,
          b.pub_payout,

          b.adv_total_number,
          b.pub_apno,
         ROUND((b.pub_apno * b.pub_payout), 2) AS payout_amount,
          p.id AS pid_id,
          p.pid,
          p.os AS pid_os,
          p.adv_total_number AS pid_total,
          p.pub_apno AS pid_apno
        FROM publisher_billing b
        LEFT JOIN publisher_billing_pid p
          ON p.billing_id = b.id
        WHERE b.pub_id=? AND b.month=?
        ORDER BY b.created_at DESC, b.campaign_name, p.pid
        `,
        [pub_id, month],
      );

      const map = {};
      for (const r of rows) {
        if (!map[r.billing_id]) {
          map[r.billing_id] = {
            billing_id: r.billing_id,
            status: r.status,
            campaign_name: r.campaign_name,
            vertical: r.vertical,
            geo: r.geo,
            os: r.os,
            payable_event: r.payable_event,
            pub_payout: r.pub_payout,
            adv_total_number: r.adv_total_number,
            pub_apno: r.pub_apno,
            payout_amount: r.payout_amount,
            pid_data: [],
          };
        }

        if (r.pid) {
          map[r.billing_id].pid_data.push({
            id: r.pid_id,
            pid: r.pid,
            os: r.pid_os,
            adv_total_number: r.pid_total,
            pub_apno: r.pid_apno,
            payout_amount: Number(
              (Number(r.pid_apno || 0) * Number(r.pub_payout || 0)).toFixed(2),
            ),
          });
        }
      }

      data = Object.values(map);
    }

    /* 2️⃣ LIVE (from adv_data only if no snapshot) */
    if (!exists) {
      const [summary] = await pool.query(
        `
        SELECT
          campaign_name,
          geo,
          vertical,
          GROUP_CONCAT(DISTINCT os) AS os,
          payable_event,
          CAST(pay_out AS DECIMAL(10,2)) AS pub_payout,
          SUM(CAST(adv_total_no AS DECIMAL(12,2))) AS adv_total_number,
          SUM(CAST(pub_Apno AS DECIMAL(12,2))) AS pub_apno
        FROM adv_data
        WHERE pub_id=? AND shared_date LIKE CONCAT(?, '%')
        GROUP BY campaign_name, geo,vertical, payable_event, CAST(pay_out AS DECIMAL(10,2))
        `,
        [pub_id, month],
      );

      const [pidRows] = await pool.query(
        ` 
        SELECT      
        campaign_name, geo, os, payable_event, pid,
        vertical,
        CAST(pay_out AS DECIMAL(10,2)) AS pub_payout,
        SUM(CAST(adv_total_no AS DECIMAL(12,2))) AS adv_total_number,
        SUM(CAST(pub_Apno AS DECIMAL(12,2))) AS pub_apno
        FROM adv_data
        WHERE pub_id=? AND shared_date LIKE CONCAT(?, '%')
        GROUP BY campaign_name, geo, os,vertical, payable_event, CAST(pay_out AS DECIMAL(10,2)), pid
        `,
        [pub_id, month],
      );

      data = summary.map((s) => ({
        ...s,
        payout_amount:
          s.pub_apno === null
            ? null
            : Number(
                (Number(s.pub_apno || 0) * Number(s.pub_payout || 0)).toFixed(
                  2,
                ),
              ),
        pid_data: pidRows
          .filter(
            (p) =>
              p.campaign_name === s.campaign_name &&
              p.geo === s.geo &&
              p.vertical === s.vertical &&
              p.payable_event === s.payable_event &&
              p.os &&
              s.os.includes(p.os) &&
              Number(p.pub_payout) === Number(s.pub_payout),
          )
          .map((p) => ({
            pid: p.pid,
            os: p.os,
            adv_total_number: p.adv_total_number,
            pub_apno: p.pub_apno,
            payout_amount:
              p.pub_apno === null
                ? null
                : Number(
                    (
                      Number(p.pub_apno || 0) * Number(p.pub_payout || 0)
                    ).toFixed(2),
                  ),
          })),
      }));
    }

    /* 3️⃣ TOTALS */
    const totals = data.reduce(
      (a, r) => {
        a.adv_total_number += Number(r.adv_total_number || 0);
        a.pub_apno += Number(r.pub_apno || 0);
        a.payout += Number(r.payout_amount || 0);
        return a;
      },
      { adv_total_number: 0, pub_apno: 0, payout: 0 },
    );

    // round AFTER reduce (very important)
    totals.adv_total_number = Number(totals.adv_total_number.toFixed(2));
    totals.pub_apno = Number(totals.pub_apno.toFixed(2));
    totals.payout = Number(totals.payout.toFixed(2));

    res.json({
      source: exists ? "snapshot" : "live",
      data,
      totals,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: "Server error" });
  }
};
