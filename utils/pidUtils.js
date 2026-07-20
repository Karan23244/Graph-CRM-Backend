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
    invalidValues.includes(pubam) || invalidValues.includes(pubid);

  if (isMissing) {
    return "violet";
  }

  // Current totals
  const totalClicks = parseInt(row.mtd_clicks) || 0;
  const totalInstalls = parseInt(row.mtd_installs) || 0;

  // Traffic Exists
  const hasTraffic =
    totalClicks > clicksPerDay || totalInstalls > installsPerDay;

  // CRM Status
  const isPaused = parseInt(is_paused) === 1;

  // Gold → Traffic + Active
  if (hasTraffic && !isPaused) {
    return "green";
  }

  // Blue → No Traffic + Active
  if (!hasTraffic && !isPaused) {
    return "orange";
  }
  // Blue → No Traffic + Active
  if (!hasTraffic && isPaused) {
    return "red";
  }
  // Purple → Traffic + Paused
  if (hasTraffic && isPaused) {
    return "yellow";
  }

  // Remaining case:
  // No Traffic + Paused
  // Returning black because only 4 colors requested
  return "red";
}

module.exports = { classifyPID };
