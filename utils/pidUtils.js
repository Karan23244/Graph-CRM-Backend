// "use strict";

// /**
//  * Classify a PID row into a color based on traffic and pause state.
//  *
//  * Rules (in priority order):
//  *  Black  → pubam IS NULL OR pubid IS NULL
//  *  Gold   → traffic exists AND is_paused = 0
//  *  Blue   → no traffic  AND is_paused = 0
//  *  Purple → traffic exists AND is_paused = 1
//  *  Grey   → no traffic AND is_paused = 1 (catch-all)
//  *
//  * Traffic exists when:
//  *   mtd_clicks >= (clicks_per_day × mtd_days)  AND
//  *   mtd_installs >= (installs_per_day × mtd_days)
//  *
//  * @param {object} row             - PID row with pubam, pubid, is_paused, mtd_clicks, mtd_installs
//  * @param {number} clicksPerDay    - from campaign_configs
//  * @param {number} installsPerDay  - from campaign_configs
//  * @param {number} totalDays       - MTD total days
//  * @returns {string}               - 'black' | 'gold' | 'blue' | 'purple' | 'grey'
//  */
// function classifyPID(row, clicksPerDay, installsPerDay, totalDays) {
//   const { pubam, pubid, is_paused } = row;

//   // Black: missing attribution info
//   const invalidValues = [null, undefined, "", "N/A"];
//   const isMissing =
//     invalidValues.includes(pubam) || invalidValues.includes(pubid);

//   if (isMissing) {
//     return "black";
//   }
//   const requiredClicks = clicksPerDay * totalDays;
//   const requiredInstalls = installsPerDay * totalDays;

//   const mtdClicks = parseInt(row.mtd_clicks) || 0;
//   const mtdInstalls = parseInt(row.mtd_installs) || 0;

//   const hasTraffic =
//     mtdClicks >= requiredClicks && mtdInstalls >= requiredInstalls;
//   const paused = parseInt(is_paused) === 1;

//   if (hasTraffic && !paused) return "gold";
//   if (!hasTraffic && !paused) return "blue";
//   if (hasTraffic && paused) return "purple";
//   return "grey";
// }

// module.exports = { classifyPID };

"use strict";

/**
 * PID Color Classification Rules
 *
 * A. Gold
 *    → Traffic Exists AND CRM Active
 *
 * B. Blue
 *    → No Traffic AND CRM Active
 *
 * C. Purple
 *    → Traffic Exists AND CRM Paused
 *
 * D. Black
 *    → pubam IS NULL OR pubid IS NULL
 *
 * Traffic Exists when:
 *   total_clicks > clicks_per_day
 *   OR
 *   total_installs > installs_per_day
 */

function classifyPID(row, clicksPerDay, installsPerDay) {
  const { pubam, pubid, is_paused } = row;

  // Missing Data → Black
  const invalidValues = [null, undefined, "", "N/A"];

  const isMissing =
    invalidValues.includes(pubam) ||
    invalidValues.includes(pubid);

  if (isMissing) {
    return "black";
  }

  // Current totals
  const totalClicks = parseInt(row.mtd_clicks) || 0;
  const totalInstalls = parseInt(row.mtd_installs) || 0;

  // Traffic Exists
  const hasTraffic =
    totalClicks > clicksPerDay ||
    totalInstalls > installsPerDay;

  // CRM Status
  const isPaused = parseInt(is_paused) === 1;

  // Gold → Traffic + Active
  if (hasTraffic && !isPaused) {
    return "gold";
  }

  // Blue → No Traffic + Active
  if (!hasTraffic && !isPaused) {
    return "blue";
  }

  // Purple → Traffic + Paused
  if (hasTraffic && isPaused) {
    return "purple";
  }

  // Remaining case:
  // No Traffic + Paused
  // Returning black because only 4 colors requested
  return "black";
}

module.exports = { classifyPID };
