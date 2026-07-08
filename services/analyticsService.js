// ==========================
// 📁 services/analyticsService.js
// ==========================
const db = require("../config/db");

// ==========================
// 🔹 Get Publisher IDs
// ==========================
async function getPublisherIds(user_id, role, assign_subadmin) {

  // 🔹 ADMIN → no filter
  if (role === "admin") {
    return null;
  }

  // 🔹 combine current user + assigned subadmins
  let users = [user_id];

  if (assign_subadmin && assign_subadmin.length > 0) {
    users = [...users, ...assign_subadmin];
  }

  // 🔹 remove duplicates
  users = [...new Set(users)];


  // 🔹 Get pub_ids from publids table
  const [rows] = await db.query(
    `
      SELECT DISTINCT pub_id
      FROM publids
      WHERE user_id IN (${users.map(() => "?").join(",")})
    `,
    users,
  );

  const pubIds = rows.map((r) => String(r.pub_id));

  return pubIds;
}

// ==========================
// 🔹 Top GEO
// ==========================
async function getTopGeo(pubIds, month) {
  const baseWhere = [`is_verified = 2`];
  const baseValues = [];

  if (month) {
    baseWhere.push(`billing_month = ?`);
    baseValues.push(month);
  }

  // 🔹 Filter for subadmin
  if (pubIds && pubIds.length) {
    baseWhere.push(`pub_id IN (${pubIds.map(() => "?").join(",")})`);

    baseValues.push(...pubIds);
  }

  const where1 = `WHERE ${baseWhere.join(" AND ")}`;

  const query = `
    WITH geo_rev AS (
      SELECT
        geo,

        ROUND(
          SUM(
            CAST(pay_out AS DECIMAL(18,4)) *
            CAST(pub_Apno AS DECIMAL(18,4))
          ),
          2
        ) AS total_revenue

      FROM pub_data_verified

      ${where1}

      GROUP BY geo

      ORDER BY total_revenue DESC

      LIMIT 10
    ),

    geo_pub AS (
      SELECT
        c.geo,

        c.pub_id,

        pl.pub_name,

        l.username,

        ROUND(
          SUM(
            CAST(c.pay_out AS DECIMAL(18,4)) *
            CAST(c.pub_Apno AS DECIMAL(18,4))
          ),
          2
        ) AS revenue,

        ROW_NUMBER() OVER (
          PARTITION BY c.geo
          ORDER BY
            SUM(
              CAST(c.pay_out AS DECIMAL(18,4)) *
              CAST(c.pub_Apno AS DECIMAL(18,4))
            ) DESC
        ) AS rnk

      FROM pub_data_verified c

      LEFT JOIN publids pl
        ON c.pub_id = pl.pub_id

      LEFT JOIN login l
        ON pl.user_id = l.id

      ${where1
        .replace(/is_verified/g, "c.is_verified")
        .replace(/billing_month/g, "c.billing_month")
        .replace(/pub_id/g, "c.pub_id")}

      GROUP BY
        c.geo,
        c.pub_id,
        pl.pub_name,
        l.username
    )

    SELECT
      g.geo,
      g.total_revenue,

      p.pub_id,
      p.pub_name,
      p.username,
      p.revenue,
      p.rnk

    FROM geo_rev g

    JOIN geo_pub p
      ON g.geo = p.geo

    WHERE p.rnk <= 15
  `;
  const finalValues = [...baseValues, ...baseValues];

  const [rows] = await db.query(query, finalValues);

  const result = {};

  rows.forEach((r) => {
    if (!result[r.geo]) {
      result[r.geo] = {
        geo: r.geo || "UNKNOWN",
        total_revenue: Number(r.total_revenue || 0).toFixed(2),
        top_publishers: [],
      };
    }

    result[r.geo].top_publishers.push({
      pub_id: r.pub_id,
      pub_name: r.pub_name,
      username: r.username,
      revenue: Number(r.revenue || 0).toFixed(2),
      rank: r.rnk,
    });
  });

  return Object.values(result);
}

// ==========================
// 🔹 Top Vertical
// ==========================
async function getTopVertical(pubIds, month) {
  const baseWhere = [`is_verified = 2`];
  const baseValues = [];

  if (month) {
    baseWhere.push(`billing_month = ?`);
    baseValues.push(month);
  }

  if (pubIds && pubIds.length) {
    baseWhere.push(`pub_id IN (${pubIds.map(() => "?").join(",")})`);

    baseValues.push(...pubIds);
  }

  const where1 = `WHERE ${baseWhere.join(" AND ")}`;

  const query = `
    WITH vert_rev AS (
      SELECT
        COALESCE(vertical, 'UNKNOWN') AS vertical,

        ROUND(
          SUM(
            CAST(pay_out AS DECIMAL(18,4)) *
            CAST(pub_Apno AS DECIMAL(18,4))
          ),
          2
        ) AS total_revenue

      FROM pub_data_verified

      ${where1}

      GROUP BY vertical

      ORDER BY total_revenue DESC

      LIMIT 10
    ),

    vert_pub AS (
      SELECT
        COALESCE(c.vertical, 'UNKNOWN') AS vertical,

        c.pub_id,

        pl.pub_name,

        l.username,

        ROUND(
          SUM(
            CAST(c.pay_out AS DECIMAL(18,4)) *
            CAST(c.pub_Apno AS DECIMAL(18,4))
          ),
          2
        ) AS revenue,

        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(c.vertical, 'UNKNOWN')
          ORDER BY
            SUM(
              CAST(c.pay_out AS DECIMAL(18,4)) *
              CAST(c.pub_Apno AS DECIMAL(18,4))
            ) DESC
        ) AS rnk

      FROM pub_data_verified c

      LEFT JOIN publids pl
        ON c.pub_id = pl.pub_id

      LEFT JOIN login l
        ON pl.user_id = l.id

      ${where1
        .replace(/is_verified/g, "c.is_verified")
        .replace(/billing_month/g, "c.billing_month")
        .replace(/pub_id/g, "c.pub_id")}

      GROUP BY
        c.vertical,
        c.pub_id,
        pl.pub_name,
        l.username
    )

    SELECT
      v.vertical,
      v.total_revenue,

      p.pub_id,
      p.pub_name,
      p.username,
      p.revenue,
      p.rnk

    FROM vert_rev v

    JOIN vert_pub p
      ON v.vertical = p.vertical

    WHERE p.rnk <= 15
  `;

  const finalValues = [...baseValues, ...baseValues];

  const [rows] = await db.query(query, finalValues);

  const result = {};

  rows.forEach((r) => {
    if (!result[r.vertical]) {
      result[r.vertical] = {
        vertical: r.vertical || "UNKNOWN",
        total_revenue: Number(r.total_revenue || 0).toFixed(2),
        top_publishers: [],
      };
    }

    result[r.vertical].top_publishers.push({
      pub_id: r.pub_id,
      pub_name: r.pub_name,
      username: r.username,
      revenue: Number(r.revenue || 0).toFixed(2),
      rank: r.rnk,
    });
  });

  return Object.values(result);
}

// ==========================
// 🔹 Top OS
// ==========================
async function getTopOS(pubIds, month) {
  const baseWhere = [`is_verified = 2`];
  const values = [];

  if (month) {
    baseWhere.push(`billing_month = ?`);
    values.push(month);
  }

  if (pubIds && pubIds.length) {
    baseWhere.push(`pub_id IN (${pubIds.map(() => "?").join(",")})`);

    values.push(...pubIds);
  }

  const where = `WHERE ${baseWhere.join(" AND ")}`;

  const [rows] = await db.query(
    `
      SELECT
        pub_id,
        os,
        pay_out,
        pub_Apno
      FROM pub_data_verified
      ${where}
    `,
    values,
  );

  const osMap = {};

  function normalizeOS(os) {
    os = os.toLowerCase().trim();

    if (os === "ios") return "iOS";
    if (os === "android") return "Android";

    return os;
  }

  rows.forEach((r) => {
    if (!r.os) return;

    const revenue = Number(r.pay_out || 0) * Number(r.pub_Apno || 0);

    const osList = r.os.split(",").map((o) => normalizeOS(o));

    const splitRevenue = revenue / osList.length;

    osList.forEach((os) => {
      if (!osMap[os]) osMap[os] = {};
      if (!osMap[os][r.pub_id]) {
        osMap[os][r.pub_id] = 0;
      }

      osMap[os][r.pub_id] += splitRevenue;
    });
  });

  const allPubIds = Object.values(osMap).flatMap((p) => Object.keys(p));

  const uniquePubIds = [...new Set(allPubIds)];

  let pubMap = {};

  if (uniquePubIds.length) {
    const [pubRows] = await db.query(
      `
        SELECT
          pl.pub_id,
          pl.pub_name,
          l.username

        FROM publids pl

        LEFT JOIN login l
          ON pl.user_id = l.id

        WHERE pl.pub_id IN (${uniquePubIds.map(() => "?").join(",")})
      `,
      uniquePubIds,
    );

    pubRows.forEach((p) => {
      pubMap[p.pub_id] = {
        pub_name: p.pub_name,
        username: p.username,
      };
    });
  }

  const result = Object.keys(osMap).map((os) => {
    const allPubs = Object.entries(osMap[os])
      .map(([pub_id, revenue]) => ({
        pub_id,
        pub_name: pubMap[pub_id]?.pub_name || null,
        username: pubMap[pub_id]?.username || null,
        revenue: Number(revenue).toFixed(2),
      }))
      .sort((a, b) => b.revenue - a.revenue)
      .map((p, i) => ({
        ...p,
        rank: i + 1,
      }));

    return {
      os,

      total_revenue: Number(
        allPubs.reduce((sum, p) => sum + Number(p.revenue), 0),
      ).toFixed(2),

      top_publishers: allPubs.slice(0, 15),
    };
  });

  return result.sort((a, b) => b.total_revenue - a.total_revenue).slice(0, 10);
}

module.exports = {
  getPublisherIds,
  getTopGeo,
  getTopVertical,
  getTopOS,
};
