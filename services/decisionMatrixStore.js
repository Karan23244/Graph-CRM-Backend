const { loadDecisionMatrix } = require("./decisionMatrixLoader");

let DECISION_MATRIX = {};

async function initializeDecisionMatrix() {
  DECISION_MATRIX = await loadDecisionMatrix();
  console.log(
    `Loaded ${Object.keys(DECISION_MATRIX).length} decision combinations`,
  );
}

module.exports = {
  initializeDecisionMatrix,
  getDecisionMatrix: () => DECISION_MATRIX,
};