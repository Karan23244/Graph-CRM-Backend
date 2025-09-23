function calculateCTI(clicks, noi) {
  return clicks ? (noi / clicks) * 100 : 0;
}

function calculateITE(noe, noi) {
  return noi ? (noe / noi) * 100 : 0;
}

function calculateETC(nocrm, noe) {
  return noe ? (nocrm / noe) * 100 : 0;
}

function calculateFraudScore(rti, pi, installs) {
  const realtimePercent = (rti / installs) * 100;
  const p360Percent = (pi / installs) * 100;
  return Math.max(realtimePercent, p360Percent);
}

function getZoneDynamic(fraud, cti, ite, etc, conditions = []) {
  if (!conditions || conditions.length === 0) return "Red";
  for (const cond of conditions) {
    const fraudOk = cond.fraud_ignore
      ? true
      : fraud >= Number(cond.fraud_min) && fraud <= Number(cond.fraud_max);
    const ctiOk = cond.cti_ignore
      ? true
      : cti >= Number(cond.cti_min) && cti <= Number(cond.cti_max);
    const iteOk = cond.ite_ignore
      ? true
      : ite >= Number(cond.ite_min) && ite <= Number(cond.ite_max);
    const etcOk = cond.etc_ignore
      ? true
      : etc >= Number(cond.etc_min) && etc <= Number(cond.etc_max);

    if (fraudOk && ctiOk && iteOk && etcOk) {
      return cond.zone_color;
    }
  }

  return "Red"; // fallback
}

module.exports = {
  calculateCTI,
  calculateITE,
  calculateETC,
  calculateFraudScore,
  getZoneDynamic,
};
