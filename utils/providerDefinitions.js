// utils/providerDefinitions.js

module.exports = {
  appsflyer: {
    supportsFraud: true,
    supportsPaidEvents: true,
  },

  adjust: {
    supportsFraud: false,
    supportsPaidEvents: false,
  },

  singular: {
    supportsFraud: false,
    supportsPaidEvents: false,
  },
};