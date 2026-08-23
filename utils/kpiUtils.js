"use strict";
const PROVIDERS = require("./providerDefinitions");
/**
 * Safe percentage: (numerator / denominator) * 100, rounded to 2dp.
 * Returns 0 if denominator is 0 or null.
 */
function pct(numerator, denominator) {
  if (numerator === null || denominator === null) {
    return "N/A";
  }

  if (denominator === 0) {
    return 0;
  }

  return Number(((numerator / denominator) * 100).toFixed(2));
}
/**
 * Safe integer fallback
 */
function int(val) {
  if (val === null) return null;

  const num = parseInt(val, 10);

  return Number.isNaN(num) ? 0 : num;
}

/**
 * Compute all KPIs for one window's aggregated row.
 *
 * @param {object} agg        - aggregated metrics { clicks, installs, rti, pi, events: { E1: {noe, pe}, ... } }
 * @param {string[]} eventKeys - ['E1','E2', ...]  (already mapped, empty strings removed)
 * @returns {object}          - flat KPI object
 */
function computeKPIs(agg, eventKeys, provider = "appsflyer") {
  const clicks = int(agg.clicks);
  const installs = int(agg.installs);
  const rti = int(agg.rti);
  const pi = int(agg.pi);
  const impressions = int(agg.impressions);

  const c2iDenominator =
    clicks > 0 ? clicks : impressions > 0 ? impressions : 0;

  const kpis = {
    clicks,
    installs,
    rti,
    pi,
    c2i: pct(installs, c2iDenominator),
  };

  if (PROVIDERS[provider]?.supportsFraud) {
    kpis.rt_install =
      installs === null || rti === null ? "N/A" : pct(rti, installs);

    kpis.pa_install =
      installs === null || pi === null ? "N/A" : pct(pi, installs);

    kpis.install_fraud =
      installs === null || rti === null || pi === null
        ? "N/A"
        : pct(rti + pi, installs);
  }
  // Dynamic event KPIs
  for (const eKey of eventKeys) {
    const eData = agg.events?.[eKey] || { noe: 0, pe: 0 };

    const noeVal = int(eData.noe);
    const peVal = int(eData.pe);

    kpis[`${eKey}_count`] = noeVal;

    kpis[`cr_${eKey}`] =
      installs === null || noeVal === null ? "N/A" : pct(noeVal, installs);

    if (PROVIDERS[provider]?.supportsPaidEvents) {
      kpis[`pa_${eKey}`] =
        noeVal === null || peVal === null ? "N/A" : pct(peVal, noeVal);

      kpis[`pae_${eKey}`] = peVal;
    }
  }

  return kpis;
}

module.exports = { pct, int, computeKPIs };
