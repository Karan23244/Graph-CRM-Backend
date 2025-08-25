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
    const fMin = Number(cond.fraud_min || 0);
    const fMax = Number(cond.fraud_max || 999999);
    const cMin = Number(cond.cti_min || 0);
    const cMax = Number(cond.cti_max || 999999);
    const iMin = Number(cond.ite_min || 0);
    const iMax = Number(cond.ite_max || 999999);
    const eMin = Number(cond.etc_min || 0);
    const eMax = Number(cond.etc_max || 999999);

    // ✅ Apply ignore flags
    const fraudOk = cond.fraud_ignore ? true : fraud >= fMin && fraud <= fMax;
    const ctiOk = cond.cti_ignore ? true : cti >= cMin && cti <= cMax;
    const iteOk = cond.ite_ignore ? true : ite >= iMin && ite <= iMax;
    const etcOk = cond.etc_ignore ? true : etc >= eMin && etc <= eMax;

    if (fraudOk && ctiOk && iteOk && etcOk) {
      return cond.zone_color;
    }
  }

  return "Red"; // default fallback
}

module.exports = {
  calculateCTI,
  calculateITE,
  calculateETC,
  calculateFraudScore,
  getZoneDynamic,
};
