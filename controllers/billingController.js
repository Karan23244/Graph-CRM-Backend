const pool = require("../config/db");
const { normalizeRole, getSubAdminIds } = require("../helpers/billinghelpers");
/* =====================================================
   DROPDOWNS
===================================================== */
exports.getBillingDropdowns = async (req, res) => {
  try {
    let { roles, user_id } = req.body;

    // Ensure roles is always an array
    if (!Array.isArray(roles)) {
      roles = [roles];
    }

    roles = roles.map(normalizeRole);

    let publishers = [];
    let advertisers = [];

    const isAdmin = roles.includes("admin");
    const isPublisherManager = roles.includes("publisher_manager");
    const isAdvertiserManager = roles.includes("advertiser_manager");
    const isPublisher = roles.includes("publisher");
    const isAdvertiser = roles.includes("advertiser");

    /**
     * =====================
     * ADMIN → ALL DATA
     * =====================
     */
    if (isAdmin) {
      const [pubs] = await pool.query(
        "SELECT pub_id, pub_name FROM publids ORDER BY pub_name",
      );

      const [advs] = await pool.query(
        "SELECT adv_id, adv_name FROM advids ORDER BY adv_name",
      );

      return res.json({
        publishers: pubs,
        advertisers: advs,
      });
    }

    /**
     * =====================
     * GET SUB ADMINS (ONCE)
     * =====================
     */
    let userIds = [user_id];

    if (isPublisherManager || isAdvertiserManager) {
      const subAdmins = await getSubAdminIds(user_id);
      userIds = [user_id, ...subAdmins];
    }

    /**
     * =====================
     * PUBLISHERS
     * =====================
     */
    if (isPublisherManager) {
      const [rows] = await pool.query(
        `
        SELECT DISTINCT pub_id, pub_name
        FROM publids
        WHERE user_id IN (?)
        ORDER BY pub_name
        `,
        [userIds],
      );
      publishers.push(...rows);
    }

    if (isPublisher) {
      const [rows] = await pool.query(
        `
        SELECT pub_id, pub_name
        FROM publids
        WHERE user_id = ?
        ORDER BY pub_name
        `,
        [user_id],
      );
      publishers.push(...rows);
    }

    /**
     * =====================
     * ADVERTISERS
     * =====================
     */
    if (isAdvertiserManager) {
      const [rows] = await pool.query(
        `
        SELECT DISTINCT adv_id, adv_name
        FROM advids
        WHERE user_id IN (?)
        ORDER BY adv_name
        `,
        [userIds],
      );
      advertisers.push(...rows);
    }

    if (isAdvertiser) {
      const [rows] = await pool.query(
        `
        SELECT adv_id, adv_name
        FROM advids
        WHERE user_id = ?
        ORDER BY adv_name
        `,
        [user_id],
      );
      advertisers.push(...rows);
    }

    /**
     * =====================
     * REMOVE DUPLICATES
     * =====================
     */
    publishers = Object.values(
      publishers.reduce((acc, cur) => {
        acc[cur.pub_id] = cur;
        return acc;
      }, {}),
    );

    advertisers = Object.values(
      advertisers.reduce((acc, cur) => {
        acc[cur.adv_id] = cur;
        return acc;
      }, {}),
    );

    return res.json({
      publishers,
      advertisers,
    });
  } catch (err) {
    console.error("Billing dropdown error:", err);
    return res.status(500).json({ message: "Server error" });
  }
};

/* =====================================================
   BILLING DATA
===================================================== */
exports.getBillingData = async (req, res) => {
  const { type, id, month } = req.body;
  console.log("Billing Request:", type, id, month);

  try {
    let rows = [];

    // ======================================================
    // 🔵 ADVERTISER BILLING
    // ======================================================
    if (type === "advertiser") {
      [rows] = await pool.query(
        `
        SELECT
          a.campaign_name,
          a.adv_payout AS payout_rate,

          SUM(CAST(a.adv_approved_no AS UNSIGNED)) AS approved_no,

          SUM(
            CAST(a.adv_approved_no AS UNSIGNED) *
            CAST(a.adv_payout AS DECIMAL(10,2))
          ) AS payout,

          COALESCE(bv.status,0) AS verified

        FROM adv_data a

        LEFT JOIN billing_verifications bv
          ON bv.adv_id = a.adv_id
          AND bv.campaign_name = a.campaign_name
          AND bv.month = ?
          AND bv.role = 'advertiser'

        WHERE a.adv_id = ?
          AND a.shared_date LIKE CONCAT(?, '%')

        GROUP BY a.campaign_name, a.adv_payout, bv.status
        ORDER BY a.campaign_name
        `,
        [month, id, month],
      );
    }

    // ======================================================
    // 🟢 PUBLISHER BILLING
    // ======================================================
    if (type === "publisher") {
      [rows] = await pool.query(
        `
SELECT
  CONCAT(
    x.campaign_name, ' - ',
    GROUP_CONCAT(DISTINCT x.os ORDER BY x.os SEPARATOR ','),
    ' - ',
    x.geo
  ) AS campaign_key,

  x.campaign_name,
  GROUP_CONCAT(DISTINCT x.os ORDER BY x.os SEPARATOR ',') AS os,
  x.geo,
  x.payable_event,
  x.payout_rate,

  SUM(x.total_no) AS total_no,
  SUM(x.approved_no) AS approved_no,

  SUM(x.approved_no * x.payout_rate) AS payout,

  COALESCE(pv.status,0) AS publisher_verified,
  COALESCE(av.status,0) AS advertiser_verified

FROM (
    /* ===== Data from adv_data (auto data) ===== */
    SELECT
      a.pub_id,
      a.adv_id,
      TRIM(a.campaign_name) AS campaign_name,
      TRIM(a.geo) AS geo,
      TRIM(a.os) AS os,
      a.payable_event,
      CAST(a.pay_out AS DECIMAL(10,2)) AS payout_rate,
      CAST(NULLIF(a.adv_total_no,'') AS DECIMAL(10,2)) AS total_no,
      CAST(NULLIF(a.pub_Apno,'') AS DECIMAL(10,2)) AS approved_no,
      a.shared_date
    FROM adv_data a
    WHERE a.pub_id = ?
      AND a.shared_date LIKE CONCAT(?, '%')

    UNION ALL

    /* ===== Data from publisher_entries (manual data) ===== */
    SELECT
      pe.pub_id,
      NULL AS adv_id,
      TRIM(pe.campaign_name) AS campaign_name,
      TRIM(pe.geo) AS geo,
      TRIM(pe.os) AS os,
      pe.payable_event,
      CAST(pe.payout_rate AS DECIMAL(10,2)) AS payout_rate,
      CAST(pe.total_no AS DECIMAL(10,2)) AS total_no,
      CAST(pe.approved_no AS DECIMAL(10,2)) AS approved_no,
      CONCAT(pe.month, '-01') AS shared_date
    FROM publisher_entries pe
    WHERE pe.pub_id = ?
      AND pe.month = ?
) x

LEFT JOIN billing_verifications pv
  ON pv.pub_id = x.pub_id
  AND pv.campaign_name = x.campaign_name
  AND pv.month = ?
  AND pv.role = 'publisher'

LEFT JOIN billing_verifications av
  ON av.adv_id = x.adv_id
  AND av.campaign_name = x.campaign_name
  AND av.month = ?
  AND av.role = 'advertiser'

GROUP BY
  x.campaign_name,
  x.geo,
  x.payable_event,
  x.payout_rate,
  pv.status,
  av.status

ORDER BY campaign_key;
`,
        [
          id, // adv_data.pub_id
          month, // adv_data.shared_date LIKE 'YYYY-MM%'
          id, // publisher_entries.pub_id
          month, // publisher_entries.month = 'YYYY-MM'
          month, // verification publisher month
          month, // verification advertiser month
        ],
      );
    }

    // ======================================================
    // 🔁 FORMAT ROWS (LOCK UNVERIFIED)
    // ======================================================
    const formattedRows = rows.map((r) => {
      if (type === "publisher" && r.advertiser_verified !== 1) {
        return {
          ...r,
          approved_no: "Not verified yet",
          payout: "Not verified yet",
          locked: true,
        };
      }

      return {
        ...r,
        approved_no: r.approved_no,
        payout: r.payout,
        locked: false,
      };
    });

    // ======================================================
    // 🧮 TOTALS (ONLY VERIFIED)
    // ======================================================
    const totals = rows.reduce(
      (acc, r) => {
        if (type === "publisher" && r.advertiser_verified !== 1) {
          return acc;
        }

        acc.approved_no += Number(r.approved_no || 0);
        acc.payout += Number(r.payout || 0);
        return acc;
      },
      { approved_no: 0, payout: 0 },
    );

    // ======================================================
    // ✅ RESPONSE
    // ======================================================
    res.json({
      data: formattedRows,
      totals,
    });
  } catch (err) {
    console.error("Billing data error:", err);
    res.status(500).json({ message: "Server error" });
  }
};
/* =====================================================
  Publisher External Data
===================================================== */

exports.getPublisherExternalBilling = async (req, res) => {
  const { pubid, month } = req.body;
  console.log("Publisher External Billing Request:", pubid, month);
  console.log("Request Body:", req.body);
  try {
    // /**
    //  * STEP 1: Resolve pub_id using username
    //  */
    // const [[publisher]] = await pool.query(
    //   `
    //   SELECT pub_id
    //   FROM publids
    //   WHERE username = ?
    //   `,
    //   [pubid],
    // );

    // if (!publisher) {
    //   return res.status(404).json({ message: "Publisher not found" });
    // }

    const pub_id = pubid;

    /**
     * STEP 2: Fetch ONLY verified or locked publisher billing snapshots
     * - No adv_data
     * - Only publisher_billing + pid_billing
     */
    const [rows] = await pool.query(
      `
SELECT
  CONCAT(
    pb.campaign_name, ' - ',
    GROUP_CONCAT(DISTINCT pb.os ORDER BY pb.os SEPARATOR ','),
    ' - ',
    pb.geo
  ) AS campaign_key,

  pb.campaign_name,
  GROUP_CONCAT(DISTINCT pb.os ORDER BY pb.os SEPARATOR ',') AS os,
  pb.geo,
  pb.payable_event,
  pb.pub_payout AS payout_rate,

  SUM(pb.adv_total_number) AS total_no,
  SUM(pb.pub_apno) AS approved_no,
  SUM(pb.pub_apno * pb.pub_payout) AS payout,

  pb.status,

  COALESCE(
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'pid', pp.pid,
        'total_no', pp.adv_total_number,
        'approved_no', pp.pub_apno,
        'payout_amount', (pp.pub_apno * pb.pub_payout)
      )
    ),
    JSON_ARRAY()
  ) AS pid_data

FROM publisher_billing pb

LEFT JOIN publisher_billing_pid pp
  ON pp.billing_id = pb.id

WHERE pb.pub_id = ?
  AND pb.month = ?
  AND pb.status IN ('verified', 'locked')

GROUP BY
  pb.campaign_name,
  pb.geo,
  pb.payable_event,
  pb.pub_payout,
  pb.status

ORDER BY campaign_key;
  `,
      [pub_id, month],
    );
    /**
     * STEP 3: Totals (only verified)
     */
    const totals = rows.reduce(
      (acc, r) => {
        if (r.status !== "verified") return acc;
        acc.approved_no += Number(r.pub_approved_no || 0);
        acc.payout += Number(r.payout_amount || 0);
        return acc;
      },
      { approved_no: 0, payout: 0 },
    );

    res.json({
      data: rows.map((r) => ({
        ...r,
        locked: r.status === "locked",
      })),
      totals,
    });
  } catch (err) {
    console.error("Publisher external billing error:", err);
    res.status(500).json({ message: "Server error" });
  }
};

/* =====================================================
   BILLING DATA
===================================================== */
