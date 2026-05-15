"use strict";

/**
 * Safe percentage: (numerator / denominator) * 100, rounded to 2dp.
 * Returns 0 if denominator is 0 or null.
 */
function pct(numerator, denominator) {
  if (!denominator || denominator === 0) return 0;
  return parseFloat(((numerator / denominator) * 100).toFixed(2));
}

/**
 * Safe integer fallback
 */
function int(val) {
  return parseInt(val) || 0;
}

/**
 * Compute all KPIs for one window's aggregated row.
 *
 * @param {object} agg        - aggregated metrics { clicks, installs, rti, pi, events: { E1: {noe, pe}, ... } }
 * @param {string[]} eventKeys - ['E1','E2', ...]  (already mapped, empty strings removed)
 * @returns {object}          - flat KPI object
 */
function computeKPIs(agg, eventKeys) {
  const clicks = int(agg.clicks);
  const installs = int(agg.installs); // noi
  const rti = int(agg.rti);
  const pi = int(agg.pi);

  const kpis = {
    clicks,
    installs,
    rti,
    pi,
    c2i: pct(installs, clicks), // Clicks-to-Install %
    rt_install: pct(rti, installs), // RT Install %
    pa_install: pct(pi, installs), // PA Install %
    install_fraud: pct(rti + pi, installs), // Total Install Fraud %
  };

  // Dynamic event KPIs
  for (const eKey of eventKeys) {
    const eData = agg.events?.[eKey] || { noe: 0, pe: 0 };

    const noeVal = int(eData.noe); // E1 / E2
    const peVal = int(eData.pe); // PAE1 / PAE2

    const total = noeVal + peVal;

    // existing logic (KEEP)
    kpis[`${eKey}_count`] = total;
    kpis[`cr_${eKey}`] = pct(total, installs);

    // change this formula
    kpis[`pa_${eKey}`] = pct(peVal, noeVal);

    // NEW → raw paid event count
    kpis[`pae_${eKey}`] = peVal;
  }

  return kpis;
}

module.exports = { pct, int, computeKPIs };
