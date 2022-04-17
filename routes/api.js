const express = require('express');
const router = express.Router();
const uploadController = require('../controllers/api/upload.controller.js');
const reportController = require('../controllers/api/report.controller.js');
/* GET users listing. */
router.get('/', function(req, res, next) {
  res.send('here is api');
});

router.get('/report', reportController);
router.post('/upload', uploadController);  

module.exports = router;
