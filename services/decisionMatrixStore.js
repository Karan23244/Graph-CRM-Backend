"use strict";

const { loadDecisionMatrix } = require("./decisionMatrixLoader");

const DECISION_MATRICES = {};

async function initializeDecisionMatrix() {
  for (const provider of ["appsflyer", "adjust", "singular"]) {
    DECISION_MATRICES[provider] = await loadDecisionMatrix(provider);

    console.log(
      `Loaded ${provider}: ${
        Object.keys(DECISION_MATRICES[provider]).length
      } combinations`,
    );
  }
}
function getDecisionMatrix(provider, key = null) {
  console.log("Requested Provider:", JSON.stringify(provider));
  console.log("Available Providers:", Object.keys(DECISION_MATRICES));

  const matrix = DECISION_MATRICES[provider];

  console.log(
    "Found Matrix:",
    !!matrix,
    matrix ? Object.keys(matrix).length : 0,
  );

  if (!matrix) {
    return {};
  }

  if (key) {
    console.log("Lookup Key:", key);
    console.log("Decision:", matrix[key]);
  }

  return matrix;
}

module.exports = {
  initializeDecisionMatrix,
  getDecisionMatrix,
};
