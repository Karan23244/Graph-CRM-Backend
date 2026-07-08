'use strict';

/**
 * Build date boundaries for MTD, primary (7D), and secondary (3D) windows.
 * All windows are relative to end_date.
 *
 * @param {string} startDate  - "YYYY-MM-DD"
 * @param {string} endDate    - "YYYY-MM-DD"
 * @param {number} primary    - default 7
 * @param {number} secondary  - default 3
 * @returns {{ mtd, primary, secondary }}
 */
function buildDateWindows(startDate, endDate, primary = 7, secondary = 3) {
  const end = new Date(endDate);
  const subtractDays = (base, days) => {
    const d = new Date(base);
    d.setDate(d.getDate() - (days - 1)); // inclusive: "last N days" includes end_date
    return formatDate(d);
  };

  return {
    mtd: {
      label: 'mtd',
      start: startDate,
      end:   endDate,
      days:  daysBetween(startDate, endDate),
    },
    primary: {
      label: `${primary}d`,
      start: subtractDays(end, primary),
      end:   endDate,
      days:  primary,
    },
    secondary: {
      label: `${secondary}d`,
      start: subtractDays(end, secondary),
      end:   endDate,
      days:  secondary,
    },
  };
}

function formatDate(d) {
  return d.toISOString().split('T')[0];
}

function daysBetween(start, end) {
  const s = new Date(start);
  const e = new Date(end);
  return Math.max(1, Math.round((e - s) / 86400000) + 1);
}

module.exports = { buildDateWindows, formatDate, daysBetween };