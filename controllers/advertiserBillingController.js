const pool = require("../config/db");

/* =====================================================
   FETCH ADVERTISER BILLING (SNAPSHOT / LIVE)
===================================================== */
exports.getAdvertiserBillingData = async (req, res) => {
  let { id: adv_id, month } = req.body;

  // normalize month (VERY IMPORTANT)
  month = month.trim(); // "2025-01"

  try {
    /* =========================
       0️⃣ CHECK SNAPSHOT
    ========================= */
    const [[exists]] = await pool.query(
      `
      SELECT id
      FROM advertiser_billing
      WHERE adv_id = ? AND month = ?
      LIMIT 1
      `,
      [adv_id, month],
    );

    let data = [];

    /* =========================
       1️⃣ FETCH FROM SNAPSHOT
    ========================= */
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
          b.adv_payout,

          b.total_no,
          b.deductions,
          b.approved_no,

          (b.approved_no * b.adv_payout) AS payout_amount,

          p.id AS pid_id,
          p.pid,
          p.os AS pid_os,
          p.total_no AS pid_total_no,
          p.deductions AS pid_deductions,
          p.approved_no AS pid_approved_no

        FROM advertiser_billing b
        LEFT JOIN advertiser_billing_pid p
          ON p.billing_id = b.id

        WHERE b.adv_id = ? AND b.month = ?
        ORDER BY b.created_at DESC,b.campaign_name, b.adv_payout, p.pid
        `,
        [adv_id, month],
      );

      const map = {};
      for (const r of rows) {
        if (!map[r.billing_id]) {
          map[r.billing_id] = {
            billing_id: r.billing_id,
            campaign_name: r.campaign_name,
            vertical: r.vertical,
            status: r.status,
            geo: r.geo,
            os: r.os,
            payable_event: r.payable_event,
            adv_payout: r.adv_payout,

            total_no: r.total_no,
            deductions: r.deductions,
            approved_no: r.approved_no,
            payout_amount: r.payout_amount,

            pid_data: [],
          };
        }

        if (r.pid) {
          map[r.billing_id].pid_data.push({
            id: r.pid_id,
            pid: r.pid,
            os: r.pid_os,
            total_no: r.pid_total_no,
            deductions: r.pid_deductions,
            approved_no: r.pid_approved_no,
            payout_amount: Number(
              (
                Number(r.pid_approved_no || 0) * Number(r.adv_payout || 0)
              ).toFixed(2),
            ),
          });
        }
      }

      data = Object.values(map);
    }

    /* =========================
       2️⃣ LIVE CALCULATION (FROM adv_data)
    ========================= */
    if (!exists) {
      const [summary] = await pool.query(
        `
        SELECT
          TRIM(campaign_name) AS campaign_name,
          TRIM(geo) AS geo,
          TRIM(vertical) AS vertical,
          GROUP_CONCAT(DISTINCT TRIM(os)) AS os,
          payable_event,
          CAST(adv_payout AS DECIMAL(10,2)) AS adv_payout,
          SUM(CAST(adv_total_no AS DECIMAL(12,2))) AS total_no,
          SUM(CAST(adv_deductions AS DECIMAL(12,2))) AS deductions,
          SUM(CAST(adv_approved_no AS DECIMAL(12,2))) AS approved_no

        FROM adv_data
        WHERE adv_id = ?
          AND shared_date LIKE CONCAT(?, '%')

        GROUP BY campaign_name, geo, vertical, payable_event, adv_payout
        `,
        [adv_id, month],
      );

      const [pidRows] = await pool.query(
        `
        SELECT
          TRIM(campaign_name) AS campaign_name,
          TRIM(geo) AS geo,
          TRIM(os) AS os,
          payable_event,
          pid,
          TRIM(vertical) AS vertical,
          CAST(adv_payout AS DECIMAL(10,2)) AS adv_payout,

           SUM(CAST(adv_total_no AS DECIMAL(12,2))) AS total_no,
           SUM(CAST(adv_deductions AS DECIMAL(12,2))) AS deductions,
           SUM(CAST(adv_approved_no AS DECIMAL(12,2))) AS approved_no

        FROM adv_data
        WHERE adv_id = ?
          AND shared_date LIKE CONCAT(?, '%')

        GROUP BY campaign_name, geo, vertical, os, payable_event, adv_payout, pid
        `,
        [adv_id, month],
      );

      data = summary.map((s) => ({
        ...s,
        payout_amount: Number(
          (Number(s.approved_no || 0) * Number(s.adv_payout || 0)).toFixed(2),
        ),
        pid_data: pidRows
          .filter(
            (p) =>
              p.campaign_name === s.campaign_name &&
              p.geo === s.geo &&
              p.payable_event === s.payable_event &&
              s.os.split(",").includes(p.os) &&
              Number(p.adv_payout) === Number(s.adv_payout),
          )
          .map((p) => ({
            pid: p.pid,
            os: p.os,
            total_no: p.total_no,
            deductions: p.deductions,
            approved_no: p.approved_no,
            payout_amount: Number(
              (Number(p.approved_no || 0) * Number(p.adv_payout || 0)).toFixed(
                2,
              ),
            ),
          })),
      }));
    }

    /* =========================
       3️⃣ GRAND TOTALS
    ========================= */
    const totals = data.reduce(
      (acc, r) => {
        acc.total_no += Number(r.total_no || 0);
        acc.deductions += Number(r.deductions || 0);
        acc.approved_no += Number(r.approved_no || 0);
        acc.payout += Number(r.payout_amount || 0);
        return acc;
      },
      { total_no: 0, deductions: 0, approved_no: 0, payout: 0 },
    );

    // round AFTER reduce (safe)
    totals.total_no = Number(totals.total_no.toFixed(2));
    totals.deductions = Number(totals.deductions.toFixed(2));
    totals.approved_no = Number(totals.approved_no.toFixed(2));
    totals.payout = Number(totals.payout.toFixed(2));

    res.json({
      source: exists ? "snapshot" : "live",
      data,
      totals,
    });
  } catch (err) {
    console.error("Advertiser fetch error:", err);
    res.status(500).json({ message: "Server error" });
  }
};

/* =====================================================
   SAVE BILLING
===================================================== */
// exports.saveAdvertiserBilling = async (req, res) => {
//   const { adv_id, month, data } = req.body;
//   const conn = await pool.getConnection();
//   console.log("Saving advertiser billing:", adv_id, month);
//   console.log("Data:", data);
//   try {
//     await conn.beginTransaction();

//     const billingIdMap = [];
//     const updatedRows = [];

//     for (const row of data) {
//       /* =========================
//      0️⃣ NORMALIZE PAYOUT
//   ========================= */
//       const adv_payout =
//         row.adv_payout !== undefined &&
//         row.adv_payout !== null &&
//         row.adv_payout !== ""
//           ? Number(row.adv_payout)
//           : 0;
//       /* =========================
//          1️⃣ CALCULATE TOTALS
//       ========================= */
//       let total_no = null;
//       let deductions = null;
//       let approved_no = null;

//       for (const p of row.pid_data || []) {
//         if (p.total_no != null) {
//           total_no = (total_no ?? 0) + Number(p.total_no);
//         }
//         if (p.deductions != null) {
//           deductions = (deductions ?? 0) + Number(p.deductions);
//         }
//         if (p.approved_no != null) {
//           approved_no = (approved_no ?? 0) + Number(p.approved_no);
//         }
//       }

//       /* =========================
//          2️⃣ UPSERT CAMPAIGN
//       ========================= */
//       let billing_id = row.billing_id || null;

//       if (billing_id) {
//         await conn.query(
//           `
//           UPDATE advertiser_billing
//           SET
//             campaign_name = ?,
//             geo = ?,
//             vertical = ?,
//             os = ?,
//             payable_event = ?,
//             adv_payout = ?,
//             total_no = ?,
//             deductions = ?,
//             approved_no = ?
//           WHERE id = ?
//           `,
//           [
//             row.campaign_name,
//             row.geo,
//             row.vertical,
//             row.os,
//             row.payable_event,
//             adv_payout,
//             total_no,
//             deductions,
//             approved_no,
//             billing_id,
//           ],
//         );
//       } else {
//         const [result] = await conn.query(
//           `
//           INSERT INTO advertiser_billing
//           (
//             adv_id, month, vertical,
//             campaign_name, geo, os,
//             payable_event, adv_payout,
//             total_no, deductions, approved_no
//           )
//           VALUES (?,?,?,?,?,?,?,?,?,?,?)
//           `,
//           [
//             adv_id,
//             month,
//             row.vertical,
//             row.campaign_name,
//             row.geo,
//             row.os,
//             row.payable_event,
//             adv_payout,
//             total_no,
//             deductions,
//             approved_no,
//           ],
//         );

//         billing_id = result.insertId;
//       }

//       billingIdMap.push({
//         tmp_id: row._tmp_id || null,
//         billing_id,
//       });

//       /* =========================
//          3️⃣ UPSERT PID
//       ========================= */
//       for (const p of row.pid_data || []) {
//         if (!p.pid) continue;

//         if (p.id) {
//           // ✅ UPDATE existing row by ID
//           await conn.query(
//             `
//       UPDATE advertiser_billing_pid
//       SET
//         pid = ?,
//         os = ?,
//         total_no = ?,
//         deductions = ?,
//         approved_no = ?
//       WHERE id = ?
//       `,
//             [
//               p.pid,
//               p.os,
//               p.total_no ?? null,
//               p.deductions ?? null,
//               p.approved_no ?? null,
//               p.id,
//             ],
//           );
//         } else {
//           // ✅ INSERT new row
//           await conn.query(
//             `
//       INSERT INTO advertiser_billing_pid
//       (billing_id, pid, os, total_no, deductions, approved_no)
//       VALUES (?,?,?,?,?,?)
//       `,
//             [
//               billing_id,
//               p.pid,
//               p.os,
//               p.total_no ?? null,
//               p.deductions ?? null,
//               p.approved_no ?? null,
//             ],
//           );
//         }
//       }
//       /* =========================
//      4️⃣ FETCH UPDATED PID (IMPORTANT)
//   ========================= */
//       const [pidRows] = await conn.query(
//         `SELECT * FROM advertiser_billing_pid WHERE billing_id = ?`,
//         [billing_id],
//       );

//       /* =========================
//      5️⃣ PUSH FINAL UPDATED ROW
//   ========================= */
//       updatedRows.push({
//         billing_id,
//         campaign_name: row.campaign_name,
//         geo: row.geo,
//         os: row.os,
//         payable_event: row.payable_event,
//         adv_payout,
//         total_no,
//         deductions,
//         approved_no,
//         pid_data: pidRows, // ✅ INCLUDE PID DATA
//       });
//     }

//     await conn.commit();

//     res.json({
//       success: true,
//       billingIdMap,
//       rows: updatedRows, // 👈 send updated data
//     });
//   } catch (err) {
//     await conn.rollback();
//     console.error("Advertiser save error:", err);
//     res.status(500).json({ success: false });
//   } finally {
//     conn.release();
//   }
// };
exports.saveAdvertiserBilling = async (req, res) => {
  const { adv_id, month, data } = req.body;
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    const billingIdMap = [];
    const changedRows = [];

    for (const row of data) {
      /* =========================
         1️⃣ NORMALIZE PAYOUT
      ========================= */
      const adv_payout =
        row.adv_payout !== undefined &&
        row.adv_payout !== null &&
        row.adv_payout !== ""
          ? Number(row.adv_payout)
          : 0;

      /* =========================
         2️⃣ CALCULATE TOTALS
      ========================= */
      let total_no = null;
      let deductions = null;
      let approved_no = null;

      for (const p of row.pid_data || []) {
        if (p.total_no != null) total_no = (total_no ?? 0) + Number(p.total_no);

        if (p.deductions != null)
          deductions = (deductions ?? 0) + Number(p.deductions);

        if (p.approved_no != null)
          approved_no = (approved_no ?? 0) + Number(p.approved_no);
      }

      let billing_id = row.billing_id || null;

      /* =========================
         3️⃣ UPSERT WITH MERGE LOGIC
      ========================= */
      if (billing_id) {
        const [[existing]] = await conn.query(
          `SELECT id, total_no, deductions, approved_no
           FROM advertiser_billing
           WHERE adv_id=? 
           AND month=? 
           AND campaign_name=? 
           AND geo=? 
           AND os=? 
           AND payable_event=? 
           AND adv_payout=? 
           AND id <> ?
           LIMIT 1`,
          [
            adv_id,
            month,
            row.campaign_name,
            row.geo,
            row.os,
            row.payable_event,
            adv_payout,
            billing_id,
          ],
        );

        if (existing) {
          // 🔥 MERGE
          await conn.query(
            `UPDATE advertiser_billing
             SET 
               total_no = IFNULL(total_no,0) + ?,
               deductions = IFNULL(deductions,0) + ?,
               approved_no = IFNULL(approved_no,0) + ?
             WHERE id=?`,
            [total_no || 0, deductions || 0, approved_no || 0, existing.id],
          );

          // move pid rows
          await conn.query(
            `UPDATE advertiser_billing_pid
             SET billing_id=?
             WHERE billing_id=?`,
            [existing.id, billing_id],
          );

          // delete duplicate
          await conn.query(`DELETE FROM advertiser_billing WHERE id=?`, [
            billing_id,
          ]);

          billing_id = existing.id;
        } else {
          // normal update
          await conn.query(
            `UPDATE advertiser_billing
             SET campaign_name=?, geo=?, vertical=?, os=?, payable_event=?,
                 adv_payout=?, total_no=?, deductions=?, approved_no=?
             WHERE id=?`,
            [
              row.campaign_name,
              row.geo,
              row.vertical,
              row.os,
              row.payable_event,
              adv_payout,
              total_no,
              deductions,
              approved_no,
              billing_id,
            ],
          );
        }
      } else {
        const [[existing]] = await conn.query(
          `SELECT id, total_no, deductions, approved_no
           FROM advertiser_billing
           WHERE adv_id=? 
           AND month=? 
           AND campaign_name=? 
           AND geo=? 
           AND os=? 
           AND payable_event=? 
           AND adv_payout=? 
           LIMIT 1`,
          [
            adv_id,
            month,
            row.campaign_name,
            row.geo,
            row.os,
            row.payable_event,
            adv_payout,
          ],
        );

        if (existing) {
          // 🔥 MERGE INTO EXISTING
          await conn.query(
            `UPDATE advertiser_billing
             SET 
               total_no = IFNULL(total_no,0) + ?,
               deductions = IFNULL(deductions,0) + ?,
               approved_no = IFNULL(approved_no,0) + ?
             WHERE id=?`,
            [total_no || 0, deductions || 0, approved_no || 0, existing.id],
          );

          billing_id = existing.id;
        } else {
          // INSERT
          const [result] = await conn.query(
            `INSERT INTO advertiser_billing
             (
               adv_id, month, vertical,
               campaign_name, geo, os,
               payable_event, adv_payout,
               total_no, deductions, approved_no
             )
             VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
            [
              adv_id,
              month,
              row.vertical,
              row.campaign_name,
              row.geo,
              row.os,
              row.payable_event,
              adv_payout,
              total_no,
              deductions,
              approved_no,
            ],
          );

          billing_id = result.insertId;
        }
      }

      billingIdMap.push({ tmp_id: row._tmp_id || null, billing_id });

      if (!changedRows.includes(billing_id)) {
        changedRows.push(billing_id);
      }

      /* =========================
         4️⃣ UPSERT PID
      ========================= */
      for (const p of row.pid_data || []) {
        if (!p.pid) continue;

        if (p.id) {
          await conn.query(
            `UPDATE advertiser_billing_pid
             SET pid=?, os=?, total_no=?, deductions=?, approved_no=?
             WHERE id=?`,
            [
              p.pid,
              p.os,
              p.total_no ?? null,
              p.deductions ?? null,
              p.approved_no ?? null,
              p.id,
            ],
          );
        } else {
          await conn.query(
            `INSERT INTO advertiser_billing_pid
             (billing_id, pid, os, total_no, deductions, approved_no)
             VALUES (?,?,?,?,?,?)`,
            [
              billing_id,
              p.pid,
              p.os,
              p.total_no ?? null,
              p.deductions ?? null,
              p.approved_no ?? null,
            ],
          );
        }
      }
    }

    await conn.commit();

    /* =========================
       5️⃣ FETCH UPDATED DATA (LIKE PUBLISHER)
    ========================= */
    if (!changedRows.length) {
      return res.json({ success: true, billingIdMap, data: [] });
    }

    const [rows] = await conn.query(
      `
      SELECT
        b.id AS billing_id,
        b.status,
        b.campaign_name,
        b.vertical,
        b.geo,
        b.os,
        b.payable_event,
        b.adv_payout,
        b.total_no,
        b.deductions,
        b.approved_no,
        ROUND((b.approved_no * b.adv_payout), 2) AS revenue,

        p.id AS pid_id,
        p.pid,
        p.os AS pid_os,
        p.total_no AS pid_total,
        p.deductions AS pid_deductions,
        p.approved_no AS pid_approved

      FROM advertiser_billing b
      LEFT JOIN advertiser_billing_pid p
        ON p.billing_id = b.id
      WHERE b.id IN (?)
      ORDER BY b.created_at DESC, b.campaign_name, p.pid
      `,
      [changedRows],
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
          adv_payout: r.adv_payout,
          total_no: r.total_no,
          deductions: r.deductions,
          approved_no: r.approved_no,
          revenue: r.revenue,
          pid_data: [],
        };
      }

      if (r.pid) {
        map[r.billing_id].pid_data.push({
          id: r.pid_id,
          pid: r.pid,
          os: r.pid_os,
          total_no: r.pid_total,
          deductions: r.pid_deductions,
          approved_no: r.pid_approved,
          revenue: Number(
            (Number(r.pid_approved || 0) * Number(r.adv_payout || 0)).toFixed(
              2,
            ),
          ),
        });
      }
    }

    const updatedData = Object.values(map);

    res.json({
      success: true,
      billingIdMap,
      data: updatedData,
    });
  } catch (err) {
    console.error("Advertiser save error:", err);
    await conn.rollback();
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
};
/* =====================================================
   LOCK BILLING
===================================================== */
exports.lockAdvertiserBilling = async (req, res) => {
  const { adv_id, month } = req.body;
  const conn = await pool.getConnection();

  try {
    await conn.query(
      `
      UPDATE advertiser_billing
      SET status = 'locked'
      WHERE adv_id = ? AND month = ?
      `,
      [adv_id, month],
    );

    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false });
  } finally {
    conn.release();
  }
};

/* =====================================================
   LIST LOCKED BILLINGS
===================================================== */
exports.listAdvertiserBilling = async (req, res) => {
  const { user_id, role = [], assigned_subadmins = [], month } = req.body;

  console.log("Advertiser billing request:", {
    user_id,
    role,
    assigned_subadmins,
    month,
  });

  try {
    let rows;
    let monthFilter = "";
    let params = [];

    if (month) {
      monthFilter = " AND ab.month = ? ";
      params.push(month);
    }

    // ✅ ADMIN → ALL LOCKED DATA (FILTERED BY MONTH)
    if (role.includes("admin")) {
      let query = `
        SELECT *
        FROM advertiser_billing ab
        WHERE ab.status = 'locked'
        ${month ? "AND ab.month = ?" : ""}
        ORDER BY ab.month DESC
      `;

      const [result] = await pool.query(query, month ? [month] : []);
      rows = result;
    }

    // ✅ ADVERTISER MANAGER → OWN + SUBADMINS
    else if (role.includes("advertiser_manager")) {
      const allowedUsers = [user_id, ...assigned_subadmins];

      let query = `
        SELECT ab.*
        FROM advertiser_billing ab
        JOIN advids a ON ab.adv_id = a.adv_id
        WHERE ab.status = 'locked'
          AND a.user_id IN (?)
          ${month ? "AND ab.month = ?" : ""}
        ORDER BY ab.month DESC
      `;

      const queryParams = month ? [allowedUsers, month] : [allowedUsers];

      const [result] = await pool.query(query, queryParams);
      rows = result;
    } else {
      return res.status(403).json({ message: "Unauthorized" });
    }

    res.json({
      success: true,
      data: rows,
    });
  } catch (err) {
    console.error("Advertiser billing error:", err);
    res.status(500).json({ success: false, message: "Server error" });
  }
};
