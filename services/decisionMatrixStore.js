"use strict";

const { loadDecisionMatrix } = require("./decisionMatrixLoader");

const DECISION_MATRICES = {};

async function initializeDecisionMatrix() {
  for (const provider of ["appsflyer", "adjust", "singular"]) {
    DECISION_MATRICES[provider] = await loadDecisionMatrix(provider);
  }
}
function getDecisionMatrix(provider, key = null) {

  const matrix = DECISION_MATRICES[provider];

  if (!matrix) {
    return {};
  }

  return matrix;
}

module.exports = {
  initializeDecisionMatrix,
  getDecisionMatrix,
};
