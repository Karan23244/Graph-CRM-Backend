"use strict";

const ExcelJS = require("exceljs");
const path = require("path");

async function loadDecisionMatrix(
  filePath = path.join(
    __dirname,
    "../data/c2i_pi_cre_pae_256_possibilitiess.xlsx",
  ),
) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);

  const worksheet = workbook.worksheets[0];

  const headers = {};
  worksheet.getRow(1).eachCell((cell, col) => {
    headers[String(cell.value).trim().toLowerCase()] = col;
  });
  console.log("Identified headers:", headers); // Debugging line
  const c2iCol = headers["c2i"];
  const piCol = headers["pi"];
  const cre2Col = headers["cre2"];
  const pae2Col = headers["pae2"];

  const decisionCol =
    headers["suggested comment"] || headers["decision"] || headers["status"];

  if (!c2iCol || !piCol || !cre2Col || !pae2Col || !decisionCol) {
    throw new Error(
      "Required columns not found in decision matrix spreadsheet",
    );
  }

  const matrix = {};

  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;

    const c2i = String(row.getCell(c2iCol).value || "")
      .trim()
      .toUpperCase();

    const pi = String(row.getCell(piCol).value || "")
      .trim()
      .toUpperCase();

    const cre2 = String(row.getCell(cre2Col).value || "")
      .trim()
      .toUpperCase();

    const pae2 = String(row.getCell(pae2Col).value || "")
      .trim()
      .toUpperCase();

    const decision = String(row.getCell(decisionCol).value || "Stable").trim();

    if (!c2i || !pi || !cre2 || !pae2) return;

    const key = `${c2i}|${pi}|${cre2}|${pae2}`;
    matrix[key] = decision;
  });

  return matrix;
}

module.exports = { loadDecisionMatrix };
