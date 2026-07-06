module.exports = {
  appsflyer: {
    hasFraud: true,
    hasPaidEvents: true,
    matrixFile: "appsflyer.xlsx",

    metrics: [
      {
        metric: "c2i",
        block: "rule1",
        key: "CTI",
        column: "c2i",
      },
      {
        metric: "fraud",
        block: "rule2",
        key: "Total Install Fraud",
        column: "pi",
      },
      {
        metric: "i2e2",
        block: "rule1",
        key: "ITE2",
        column: "cre2",
      },
      {
        metric: "pa_e2",
        block: "rule2",
        key: "PA E2",
        column: "pae2",
      },
    ],
  },

  adjust: {
    hasFraud: false,
    hasPaidEvents: false,
    matrixFile: "adjust.xlsx",

    metrics: [
      {
        metric: "c2i",
        block: "rule1",
        key: "CTI",
        column: "c2i",
      },
      {
        metric: "i2e1",
        block: "rule1",
        key: "ITE1",
        column: "cre1",
      },
      {
        metric: "i2e2",
        block: "rule1",
        key: "ITE2",
        column: "cre2",
      },
    ],
  },

  singular: {
    hasFraud: false,
    hasPaidEvents: false,
    matrixFile: "singular.xlsx",

    metrics: [
      {
        metric: "c2i",
        block: "rule1",
        key: "CTI",
        column: "c2i",
      },
      {
        metric: "i2e1",
        block: "rule1",
        key: "ITE1",
        column: "cre1",
      },
      {
        metric: "i2e2",
        block: "rule1",
        key: "ITE2",
        column: "cre2",
      },
    ],
  },
};
