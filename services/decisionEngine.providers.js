"use strict";

module.exports = {
  appsflyer: {
    hasFraud: true,
    hasPaidEvents: true,

    matrixFile: "appsflyer.xlsx",

    metrics: [
      { metric: "c2i", column: "c2i" },
      { metric: "fraud", column: "pi" },
      { metric: "i2e2", column: "cre2" },
      { metric: "pa_e2", column: "pae2" },
    ],
  },

  adjust: {
    hasFraud: false,
    hasPaidEvents: false,

    matrixFile: "adjust.xlsx",

    metrics: [
      { metric: "c2i", column: "c2i" },
      { metric: "i2e1", column: "cre1" },
      { metric: "i2e2", column: "cre2" },
    ],
  },

  singular: {
    hasFraud: false,
    hasPaidEvents: false,

    matrixFile: "adjust.xlsx",

    metrics: [
      { metric: "c2i", column: "c2i" },
      { metric: "i2e1", column: "cre1" },
      { metric: "i2e2", column: "cre2" },
    ],
  },
};
