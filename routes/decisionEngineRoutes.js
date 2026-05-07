// ─────────────────────────────────────────────────────────────────────────────


/**
 * decisionEngine.route.js
 *
 * Mount in app.js:
 *   const decisionEngineRoute = require('./modules/decisionEngine/decisionEngine.route');
 *   app.use('/api', decisionEngineRoute);
 *
 * Endpoint: POST /api/decision
 */

'use strict';

const express    = require('express');
const router     = express.Router();
const { getDecision } = require('../controllers/DecisionengineController.js');

router.post('/decision', getDecision);

module.exports = router;