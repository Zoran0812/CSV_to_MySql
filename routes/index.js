var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { finished : false, reported : false });
});

router.get('/test1', function(req, res, next) {
  res.render('convert', { finished : false });
});
router.get('/test', function(req, res, next) {
  let timestring = new Date().toLocaleString();
  res.render('report', { jockey : true, startTime : timestring, endTime : timestring });
});

module.exports = router;
