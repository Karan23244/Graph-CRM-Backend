/**
 * decisionEngine.logic.js
 * Pure functions — no DB calls, no side effects.
 * All decision, grading, eligibility logic lives here.
 */

"use strict";
const { getDecisionMatrix } = require("./decisionMatrixStore");
const METRIC_MAP = require("./decisionEngine.metricMap");
// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
console.log(METRIC_MAP);
const COLOR_TO_GRADE = { green: "A", yellow: "B", orange: "C", red: "D" };

// Grading source map — which param block + key each metric uses
const GRADING_SOURCE = {
  c2i: { block: "rule1", key: "CTI" },
  i2e2: { block: "rule1", key: "ITE2" },
  fraud: { block: "rule2", key: "Total Install Fraud" },
  pa_e2: { block: "rule2", key: "PA E2" },
};

// ─────────────────────────────────────────────
// GRADING
// ─────────────────────────────────────────────

/**
 * Returns grade letter (A/B/C/D) for a given numeric value against
 * a param config block.
 *
 * Config shape:
 * {
 *   green:  { range1: { min, max }, range2: { min, max } },
 *   yellow: { ... },
 *   orange: { ... },
 *   red:    { ... }
 * }
 *
 * Evaluation is a pure range lookup — no directional assumption.
 * Falls back to 'D' when value does not match any defined range.
 */
function getGrade(value, paramConfig) {
  if (!paramConfig || typeof value !== "number") return "D";

  for (const [color, grade] of Object.entries(COLOR_TO_GRADE)) {
    const ranges = paramConfig[color];
    if (!ranges) continue;
    for (const range of Object.values(ranges)) {
      if (range && value >= range.min && value <= range.max) {
        return grade;
      }
    }
  }
  return "D";
}

/**
 * Grades all metrics defined for the current provider.
 * Metrics in ignoreList are excluded from decision logic but still
 * returned in the response with ignored:true and grade:null.
 *
 * @param {object} metricValues  { c2i, fraud, i2e2, pa_e2 }
 * @param {object} rule1Params   Parsed rule1_params JSON
 * @param {object} rule2Params   Parsed rule2_params JSON
 * @param {string[]} ignoreList  e.g. ['fraud']
 * @returns {object} { c2i: { value, grade, ignored }, ... }
 */
function gradeMetrics(
  metricValues,
  rule1Params,
  rule2Params,
  ignoreList,
  providerName,
) {
  const provider = METRIC_MAP[providerName];

  return Object.fromEntries(
    provider.metrics.map(({ metric, block, key }) => {
      const ignored = ignoreList.includes(metric);

      const value = metricValues[metric];

      const config = block === "rule1" ? rule1Params[key] : rule2Params[key];

      return [
        metric,
        {
          value,
          grade: ignored ? null : getGrade(value, config),
          ignored,
        },
      ];
    }),
  );
}

// ─────────────────────────────────────────────
// METRICS COMPUTATION
// ─────────────────────────────────────────────

/**
 * Computes raw metric percentages from aggregated DB values.
 * Safe division: returns 0 when denominator is 0.
 */
function computeMetrics(
  { clicks, installs, rti, pi, e1Total, e2Total, peE1Total, peE2Total },
  providerName,
) {
  const provider = METRIC_MAP[providerName];

  clicks = Number(clicks) || 0;
  installs = Number(installs) || 0;

  rti = Number(rti) || 0;
  pi = Number(pi) || 0;

  e1Total = Number(e1Total) || 0;
  e2Total = Number(e2Total) || 0;

  peE1Total = Number(peE1Total) || 0;
  peE2Total = Number(peE2Total) || 0;

  const pct = (a, b) => (b > 0 ? Number(((a / b) * 100).toFixed(2)) : 0);

  const metrics = {
    c2i: pct(installs, clicks),
    i2e1: pct(e1Total, installs),
    i2e2: pct(e2Total, installs),
  };

  if (provider.hasFraud) {
    metrics.fraud = pct(rti + pi, installs);
  }

  if (provider.hasPaidEvents) {
    metrics.pa_e1 = pct(peE1Total, e1Total);
    metrics.pa_e2 = pct(peE2Total, e2Total);
  }

  return metrics;
}

// ─────────────────────────────────────────────
// ELIGIBILITY
// ─────────────────────────────────────────────

/**
 * Returns true if ANY eligibility condition is met (using last-5-day window).
 *
 * Conditions:
 * 1. Click Cap:    clicks_5d  >= clicks_per_day  × 5
 * 2. Install Cap:  installs_5d >= installs_per_day × 5
 * 3. Link Active:  shared_date + 5 days <= selected_date
 */
function checkEligibility({
  clicks5d,
  installs5d,
  sharedDate,
  selectedDate,
  clicksPerDay,
  installsPerDay,
}) {
  const clickCap = clicks5d >= clicksPerDay;
  const installCap = installs5d >= installsPerDay;

  let minSharedDaysReached = false;

  if (sharedDate) {
    const threshold = new Date(sharedDate);
    threshold.setDate(threshold.getDate() + 5);

    minSharedDaysReached = new Date(selectedDate) >= threshold;
  }
  return {
    clickCap,
    installCap,
    minSharedDaysReached,
    eligible: clickCap || installCap || minSharedDaysReached,
  };
}

// ─────────────────────────────────────────────
// DECISION ENGINE — STEP 1 + STEP 2
// ─────────────────────────────────────────────

/**
 * STEP 1 — Fraud severity gate.
 * If Install Fraud grade is D (red) and it is not ignored,
 * status is immediately forced to "Pause" without proceeding
 * to the combination matrix.
 *
 * Returns null if fraud does not trigger an early exit,
 * or 'Pause' if it does.
 */
function applyFraudRule(gradedMetrics, providerName) {
  const provider = METRIC_MAP[providerName];

  if (!provider.hasFraud) return null;

  const fraud = gradedMetrics.fraud;

  if (!fraud) return null;

  if (!fraud.ignored && fraud.grade === "D") {
    return "Pause";
  }

  return null;
}
/**
 * STEP 2 — Combination matrix lookup.
 * Builds the decision key by sorting active (non-ignored) metrics
 * by their grade ascending (A→D).
 * Ties broken by METRIC_PRIORITY order for determinism.
 *
 * Falls back to 'Stable' for any unmatched combination.
 */
function applyDecisionMatrix(gradedMetrics, provider) {
    const providerConfig = METRIC_MAP[provider];

    const key = providerConfig.metrics
        .map(({ metric }) => gradedMetrics[metric].grade)
        .join("|");

    console.log("Generated Key:", key);

    const matrix = getDecisionMatrix(provider);

    console.log("Decision:", matrix[key]);

    return matrix[key] ?? "Stable";
}

/**
 * Master evaluation: runs Step 1 then Step 2.
 * Only called when the row is eligible.
 */
function evaluateStatus(gradedMetrics, provider) {
  // Stable if all active metrics are Grade A
  const activeMetrics = Object.values(gradedMetrics).filter(
    (m) => !m.ignored && m.grade !== null,
  );

  const allGradeA =
    activeMetrics.length > 0 && activeMetrics.every((m) => m.grade === "A");

  if (allGradeA) {
    return "Stable";
  }

  return (
    applyFraudRule(gradedMetrics, provider) ??
    applyDecisionMatrix(gradedMetrics, provider)
  );
}

// ─────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────

module.exports = {
  computeMetrics,
  gradeMetrics,
  checkEligibility,
  evaluateStatus,
  getGrade, // exported for unit tests
  applyFraudRule, // exported for unit tests
  applyDecisionMatrix, // exported for unit tests
};
