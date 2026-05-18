"use strict";

/**
 * Given a numeric value and a color-rules object like:
 * {
 *   "green":  { "range1": { "min": 1, "max": 2 } },
 *   "yellow": { "range1": { "min": 3, "max": 4 }, "range2": { "min": 5, "max": 6 } },
 *   ...
 * }
 * Returns the matching color string, or null if no range matches.
 *
 * Colors are evaluated in priority order: green → yellow → orange → red.
 */
const COLOR_PRIORITY = ["green", "yellow", "orange", "red"];

function getColor(value, colorRules) {
  if (value === null || value === undefined || !colorRules) return null;

  for (const color of COLOR_PRIORITY) {
    const ranges = colorRules[color];
    if (!ranges) continue;

    for (const rangeKey of Object.keys(ranges)) {
      const { min, max } = ranges[rangeKey];
      if (value >= min && value <= max) {
        return color;
      }
    }
  }
  return null; // no match
}

/**
 * Maps internal KPI key → rule1/rule2 param key.
 * rule1 = CR / conversion KPIs
 * rule2 = Fraud / install KPIs
 */
const KPI_TO_RULE1_KEY = {
  c2i: "CTI",
  // Dynamic: cr_E1 → 'ITE1', cr_E2 → 'ITE2' etc.
};

const KPI_TO_RULE2_KEY = {
  rt_install: "RI",
  pa_install: "PI",
  install_fraud: "Total Install Fraud",
  // Dynamic: pa_E2 → 'PA E2'
};

/**
 * Given the full flat KPI object for a window, attach colors.
 * Returns an object where colored KPIs become { value, color } pairs.
 *
 * @param {object}   kpis       - flat kpi values from computeKPIs
 * @param {object}   rule1      - parsed rule1_params JSON
 * @param {object}   rule2      - parsed rule2_params JSON
 * @param {string[]} eventKeys  - ['E1','E2']
 * @returns {object}            - colored KPI map
 */
function applyColors(kpis, rule1, rule2, eventKeys) {
  const result = {};

  // c2i uses rule1 → CTI
  result.c2i = withColor(kpis.c2i, rule1?.["CTI"]);

  // fraud KPIs use rule2
  result.rt_install = withColor(kpis.rt_install, rule2?.["RI"]);
  result.pa_install = withColor(kpis.pa_install, rule2?.["PI"]);
  result.install_fraud = withColor(
    kpis.install_fraud,
    rule2?.["Total Install Fraud"],
  );

  // raw counts (no color)
  result.clicks = kpis.clicks;
  result.installs = kpis.installs;
  result.rti = kpis.rti;
  result.pi = kpis.pi;
  // Dynamic event KPIs
  eventKeys.forEach((eKey, idx) => {
    const ruleKeyIdx = idx + 1;

    result[`${eKey}_count`] = kpis[`${eKey}_count`];

    result[`cr_${eKey}`] = withColor(
      kpis[`cr_${eKey}`],
      rule1?.[`ITE${ruleKeyIdx}`],
    );

    // percentage color
    result[`pa_${eKey}`] = withColor(kpis[`pa_${eKey}`], rule2?.[`PA ${eKey}`]);

    // pae count
    result[`pae_${eKey}`] = withColor(
      kpis[`pae_${eKey}`],
      rule2?.[`PAE${ruleKeyIdx}`],
    );
  });

  return result;
}

function withColor(value, colorRules) {
  return {
    value: value ?? 0,
    color: getColor(value, colorRules),
  };
}

module.exports = { getColor, applyColors, withColor };
