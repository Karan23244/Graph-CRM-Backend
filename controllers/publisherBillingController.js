const pool = require("../config/db");

/* =====================================================
   FETCH PUBLISHER BILLING (SNAPSHOT / LIVE)
===================================================== */
exports.getPublisherBillingData = async (req, res) => {
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

          p.pid,
          p.os AS pid_os,
          p.adv_total_number AS pid_total,
          p.pub_apno AS pid_apno
        FROM publisher_billing b
        LEFT JOIN publisher_billing_pid p
          ON p.billing_id = b.id
        WHERE b.pub_id=? AND b.month=?
        ORDER BY b.campaign_name, p.pid
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
        GROUP BY campaign_name, geo,vertical, payable_event, pub_payout
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
        GROUP BY campaign_name, geo, os,vertical, payable_event, pub_payout, pid
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
/* =====================================================
   SAVE PUBLISHER BILLING
===================================================== */
exports.savePublisherBilling = async (req, res) => {
  const { pub_id, month, data } = req.body;
  const conn = await pool.getConnection();
  console.log("Data:", data);
  try {
    await conn.beginTransaction();
    const billingIdMap = [];

    for (const row of data) {
      let adv_total_number = null;
      let pub_apno = null;

      for (const p of row.pid_data || []) {
        if (p.adv_total_number != null)
          adv_total_number =
            (adv_total_number ?? 0) + Number(p.adv_total_number);

        if (p.pub_apno != null) pub_apno = (pub_apno ?? 0) + Number(p.pub_apno);
      }

      let billing_id = row.billing_id || null;

      /* =========================
   2️⃣ UPSERT CAMPAIGN
========================= */
      //   if (billing_id) {
      //     await conn.query(
      //       `
      // UPDATE publisher_billing
      // SET
      //   campaign_name = ?,
      //   geo = ?,
      //   vertical = ?,
      //   os = ?,
      //   payable_event = ?,
      //   pub_payout = ?,
      //   adv_total_number = ?,
      //   pub_apno = ?
      // WHERE id = ?
      // `,
      //       [
      //         row.campaign_name,
      //         row.geo,
      //         row.vertical,
      //         row.os,
      //         row.payable_event,
      //         row.pub_payout,
      //         adv_total_number,
      //         pub_apno,
      //         billing_id,
      //       ],
      //     );
      //   }

      if (billing_id) {
        const [[existing]] = await conn.query(
          `SELECT id, adv_total_number, pub_apno
     FROM publisher_billing
     WHERE pub_id=? 
     AND month=? 
     AND campaign_name=? 
     AND geo=? 
     AND os=? 
     AND payable_event=? 
     AND pub_payout=? 
     AND id <> ? 
     LIMIT 1`,
          [
            pub_id,
            month,
            row.campaign_name,
            row.geo,
            row.os,
            row.payable_event,
            row.pub_payout,
            billing_id,
          ],
        );

        if (existing) {
          // 🔥 MERGE ROWS
          await conn.query(
            `UPDATE publisher_billing
       SET 
         adv_total_number = IFNULL(adv_total_number,0) + ?,
         pub_apno = IFNULL(pub_apno,0) + ?
       WHERE id=?`,
            [adv_total_number || 0, pub_apno || 0, existing.id],
          );

          // move pid rows
          await conn.query(
            `UPDATE publisher_billing_pid
       SET billing_id=?
       WHERE billing_id=?`,
            [existing.id, billing_id],
          );

          // delete duplicate row
          await conn.query(`DELETE FROM publisher_billing WHERE id=?`, [
            billing_id,
          ]);

          billing_id = existing.id;
        } else {
          // normal update
          await conn.query(
            `UPDATE publisher_billing
       SET campaign_name=?, geo=?, vertical=?, os=?, payable_event=?,
           pub_payout=?, adv_total_number=?, pub_apno=?
       WHERE id=?`,
            [
              row.campaign_name,
              row.geo,
              row.vertical,
              row.os,
              row.payable_event,
              row.pub_payout,
              adv_total_number,
              pub_apno,
              billing_id,
            ],
          );
        }
      } else {
        //     const [result] = await conn.query(
        //       `
        // INSERT INTO publisher_billing
        // (
        //   pub_id, month, vertical,
        //   campaign_name, geo, os,
        //   payable_event, pub_payout,
        //   adv_total_number, pub_apno
        // )
        // VALUES (?,?,?,?,?,?,?,?,?,?)
        // `,
        //       [
        //         pub_id,
        //         month,
        //         row.vertical,
        //         row.campaign_name,
        //         row.geo,
        //         row.os,
        //         row.payable_event,
        //         row.pub_payout,
        //         adv_total_number,
        //         pub_apno,
        //       ],
        //     );

        //     billing_id = result.insertId;
        // check if identical campaign row already exists
        const [[existing]] = await conn.query(
          `SELECT id, adv_total_number, pub_apno
   FROM publisher_billing
   WHERE pub_id=? 
   AND month=? 
   AND campaign_name=? 
   AND geo=? 
   AND os=? 
   AND payable_event=? 
   AND pub_payout=? 
   LIMIT 1`,
          [
            pub_id,
            month,
            row.campaign_name,
            row.geo,
            row.os,
            row.payable_event,
            row.pub_payout,
          ],
        );

        if (existing) {
          // 🔥 MERGE INTO EXISTING ROW
          await conn.query(
            `UPDATE publisher_billing
     SET 
       adv_total_number = IFNULL(adv_total_number,0) + ?,
       pub_apno = IFNULL(pub_apno,0) + ?
     WHERE id=?`,
            [adv_total_number || 0, pub_apno || 0, existing.id],
          );

          billing_id = existing.id;
        } else {
          // normal insert
          const [result] = await conn.query(
            `INSERT INTO publisher_billing
     (
       pub_id, month, vertical,
       campaign_name, geo, os,
       payable_event, pub_payout,
       adv_total_number, pub_apno
     )
     VALUES (?,?,?,?,?,?,?,?,?,?)`,
            [
              pub_id,
              month,
              row.vertical,
              row.campaign_name,
              row.geo,
              row.os,
              row.payable_event,
              row.pub_payout,
              adv_total_number,
              pub_apno,
            ],
          );

          billing_id = result.insertId;
        }
      }

      billingIdMap.push({ tmp_id: row._tmp_id || null, billing_id });

      for (const p of row.pid_data || []) {
        await conn.query(
          `
          INSERT INTO publisher_billing_pid
          (billing_id, pid, os, adv_total_number, pub_apno)
          VALUES (?,?,?,?,?)
          ON DUPLICATE KEY UPDATE
            adv_total_number=VALUES(adv_total_number),
            pub_apno=VALUES(pub_apno)
          `,
          [
            billing_id,
            p.pid,
            p.os,
            p.adv_total_number ?? null,
            p.pub_apno ?? null,
          ],
        );
      }
    }

    await conn.commit();
    res.json({ success: true, billingIdMap });
  } catch (e) {
    console.error("PUBLISHER SAVE ERROR:", e);
    await conn.rollback();
    res.status(500).json({ success: false, error: e.message });
  } finally {
    conn.release();
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
      UPDATE publisher_billing
      SET status='locked'
      WHERE pub_id=? AND month=?
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
