"use strict";

const ExcelJS = require("exceljs");
const path = require("path");
const PROVIDERS = require("./decisionEngine.providers");

async function loadDecisionMatrix(provider) {
  const providerConfig = PROVIDERS[provider];
  if (!providerConfig) {
    throw new Error(`Unsupported provider: ${provider}`);
  }

  const filePath = path.join(__dirname, "../data", providerConfig.matrixFile);

  const workbook = new ExcelJS.Workbook();

  await workbook.xlsx.readFile(filePath);

  const worksheet = workbook.worksheets[0];

  if (!worksheet) {
    throw new Error(`No worksheet found in ${providerConfig.matrixFile}`);
  }

  const headers = {};

  worksheet.getRow(1).eachCell((cell, col) => {
    headers[String(cell.value).trim().toLowerCase()] = col;
  });

  const metricColumns = providerConfig.metrics.map(({ column }) => {
    const col = headers[column.toLowerCase()];
    if (!col) {
      throw new Error(
        `Missing "${column}" column in ${provider} decision matrix`,
      );
    }

    return col;
  });

  const decisionCol =
    headers["suggested comment"] || headers["decision"] || headers["status"];

  if (!decisionCol) {
    throw new Error("Decision column not found");
  }

  const matrix = {};

  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;

    const grades = metricColumns.map((column) =>
      String(row.getCell(column).value || "")
        .trim()
        .toUpperCase(),
    );

    if (grades.some((g) => !g)) return;

    const decision = String(row.getCell(decisionCol).value || "Stable").trim();

    matrix[grades.join("|")] = decision;
  });

  return matrix;
}

module.exports = {
  loadDecisionMatrix,
};
