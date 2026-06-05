/**
 * decisionEngine.logic.js
 * Pure functions — no DB calls, no side effects.
 * All decision, grading, eligibility logic lives here.
 */

"use strict";
const { getDecisionMatrix } = require("./decisionMatrixStore");
// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────

const COLOR_TO_GRADE = { green: "A", yellow: "B", orange: "C", red: "D" };
const GRADE_ORDER = { A: 0, B: 1, C: 2, D: 3 };

// Tiebreak order when two metrics share the same grade
const METRIC_PRIORITY = ["c2i", "fraud", "i2e2", "pa_e2"];

// Grading source map — which param block + key each metric uses
const GRADING_SOURCE = {
  c2i: { block: "rule1", key: "CTI" },
  i2e2: { block: "rule1", key: "ITE2" },
  fraud: { block: "rule2", key: "Total Install Fraud" },
  pa_e2: { block: "rule2", key: "PA E2" },
};

// ─────────────────────────────────────────────
// DECISION MATRIX
// Key format: "{metricWithA}|{metricWithB}|{metricWithC}|{metricWithD}"
// ─────────────────────────────────────────────

const DECISION_MATRIX = {
  // ── A = c2i ──────────────────────────────────────
  "c2i|fraud|i2e2|pa_e2": "Optimise",
  "c2i|fraud|pa_e2|i2e2": "Optimise",
  "c2i|i2e2|fraud|pa_e2": "Pause",
  "c2i|i2e2|pa_e2|fraud": "Pause",
  "c2i|pa_e2|fraud|i2e2": "Optimise",
  "c2i|pa_e2|i2e2|fraud": "Pause",

  // ── A = fraud ─────────────────────────────────────
  "fraud|c2i|i2e2|pa_e2": "Optimise",
  "fraud|c2i|pa_e2|i2e2": "Optimise",
  "fraud|i2e2|c2i|pa_e2": "Optimise",
  "fraud|i2e2|pa_e2|c2i": "Optimise",
  "fraud|pa_e2|c2i|i2e2": "Optimise",
  "fraud|pa_e2|i2e2|c2i": "Optimise",

  // ── A = i2e2 ─────────────────────────────────────
  "i2e2|c2i|fraud|pa_e2": "Optimise",
  "i2e2|c2i|pa_e2|fraud": "Pause",
  "i2e2|fraud|c2i|pa_e2": "Optimise",
  "i2e2|fraud|pa_e2|c2i": "Optimise",
  "i2e2|pa_e2|c2i|fraud": "Pause",
  "i2e2|pa_e2|fraud|c2i": "Optimise",

  // ── A = pa_e2 ────────────────────────────────────
  "pa_e2|c2i|fraud|i2e2": "Optimise",
  "pa_e2|c2i|i2e2|fraud": "Pause",
  "pa_e2|fraud|c2i|i2e2": "Optimise",
  "pa_e2|fraud|i2e2|c2i": "Optimise",
  "pa_e2|i2e2|c2i|fraud": "Pause",
  // pa_e2|i2e2|fraud|c2i — not defined in spec, defaults to Stable
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
 * Grades all four metrics.
 * Metrics in ignoreList are excluded from decision logic but still
 * returned in the response with ignored:true and grade:null.
 *
 * @param {object} metricValues  { c2i, fraud, i2e2, pa_e2 }
 * @param {object} rule1Params   Parsed rule1_params JSON
 * @param {object} rule2Params   Parsed rule2_params JSON
 * @param {string[]} ignoreList  e.g. ['fraud']
 * @returns {object} { c2i: { value, grade, ignored }, ... }
 */
function gradeMetrics(metricValues, rule1Params, rule2Params, ignoreList) {
  const paramBlocks = { rule1: rule1Params, rule2: rule2Params };

  return Object.fromEntries(
    Object.entries(GRADING_SOURCE).map(([metric, { block, key }]) => {
      const ignored = ignoreList.includes(metric);
      const value = metricValues[metric];
      const config = paramBlocks[block]?.[key] ?? null;
      const grade = ignored ? null : getGrade(value, config);
      return [metric, { value, grade, ignored }];
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
function computeMetrics({ clicks, installs, rti, pi, e2Total, peE2Total }) {
  clicks = Number(clicks) || 0;
  installs = Number(installs) || 0;
  rti = Number(rti) || 0;
  pi = Number(pi) || 0;
  e2Total = Number(e2Total) || 0;
  peE2Total = Number(peE2Total) || 0;

  const pct = (num, den) =>
    den > 0 ? parseFloat(((num / den) * 100).toFixed(2)) : 0;

  return {
    c2i: pct(installs, clicks),
    fraud: pct(rti + pi, installs),
    i2e2: pct(e2Total, installs),
    pa_e2: pct(peE2Total, e2Total),
  };
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
  const clickCap = clicks5d >= clicksPerDay * 5;
  const installCap = installs5d >= installsPerDay * 5;

  let linkActive = false;
  if (sharedDate) {
    const threshold = new Date(sharedDate);
    threshold.setDate(threshold.getDate() + 5);
    linkActive = threshold <= new Date(selectedDate);
  }

  return {
    clickCap,
    installCap,
    linkActive,
    eligible: clickCap || installCap || linkActive,
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
function applyFraudRule(gradedMetrics) {
  const { fraud } = gradedMetrics;
  if (!fraud.ignored && fraud.grade === "D") return "Pause";
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
function applyDecisionMatrix(gradedMetrics) {
  const active = Object.entries(gradedMetrics)
    .filter(([, { ignored, grade }]) => !ignored && grade !== null)
    .sort(([ma, a], [mb, b]) => {
      const gradeDiff = GRADE_ORDER[a.grade] - GRADE_ORDER[b.grade];
      if (gradeDiff !== 0) return gradeDiff;
      return METRIC_PRIORITY.indexOf(ma) - METRIC_PRIORITY.indexOf(mb);
    });

  const key = [
    gradedMetrics.c2i.grade,
    gradedMetrics.fraud.grade,
    gradedMetrics.i2e2.grade,
    gradedMetrics.pa_e2.grade,
  ].join("|");
  console.log(`Constructed decision key: ${key}`);
  const matrix = getDecisionMatrix();
  console.log(`Decision key: ${key}`);
  console.log(`Matrix lookup result: ${matrix[key]}`);
  return matrix[key] ?? "Stable";
}

/**
 * Master evaluation: runs Step 1 then Step 2.
 * Only called when the row is eligible.
 */
function evaluateStatus(gradedMetrics) {
  // Stable if all active metrics are Grade A
  const activeMetrics = Object.values(gradedMetrics).filter(
    (m) => !m.ignored && m.grade !== null,
  );

  const allGradeA =
    activeMetrics.length > 0 && activeMetrics.every((m) => m.grade === "A");

  if (allGradeA) {
    return "Stable";
  }

  return applyFraudRule(gradedMetrics) ?? applyDecisionMatrix(gradedMetrics);
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
