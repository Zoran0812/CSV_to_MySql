const res = require("express/lib/response");
const mysql = require("mysql");
const util = require("util");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const report = function(req, res) {
  const selectDate = req.query.select_date;
  const month = req.query.select_month;
  const param = req.query.select_param;
  const connection = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: process.env.database || "csvdb"
  });
  const query = util.promisify(connection.query).bind(connection);
  // let _jockeys = [], _trainers = [], _dams = [], _damsires = [], _sires = [], _horses = [];
  switch(param) {
    case 'all':
      calcTrainer(req, res, query, selectDate, month, false);
      break;
    case 'horse':
      calcHorse(req, res, query, selectDate, month, false);
      break;
    case 'jockey':
      calcJockey(req, res, query, selectDate, month, false);
      break;
    case 'trainer':
      calcTrainer(req, res, query, selectDate, month, false);
      break;
    case 'dam':
      calcDam(req, res, query, selectDate, month, false);
      break;
    case 'sire':
      calcSire(req, res, query, selectDate, month, false);
      break;
    case 'damsire':
      calcDamSire(req, res, query, selectDate, month, false);
      break;
  }
}

//after Jockey
async function calcTrainer(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _trainer FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "Trainer_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "trainer", title: "trainer" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let name = item._trainer;
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _trainer = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _trainer = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _trainer = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _trainer = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _trainer = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _trainer = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        // console.log("res = ", index, name, data[0].calc, data[1].calc, data[2].calc, data[3].calc);
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"trainer":"'+ name +'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        // IVs.push(data[0].iv);
        if(index == cnt) {
          if(ongoing){
            calcJockey(req, res, query, selectDate, month);
          }else{
            let start = now.toLocaleString();
            let end = new Date().toLocaleString();
            sendJsonData(req, res, {status : 'TRAINER', startTime : start, endTime : end});          
          }
        }
      });
    }); 
  });
}
//after Horse
async function calcJockey(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _jockey FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "Jockey_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "jockey", title: "jockey" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let jockey = item._jockey
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _jockey = '"+ jockey +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _jockey = '"+ jockey +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _jockey = '"+ jockey +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _jockey = '"+ jockey +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _jockey = '"+ jockey +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _jockey = '"+ jockey +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        // console.log("res = ", index, jockey, data[0].calc, data[1].calc, data[2].calc, data[3].calc);
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"jockey":"'+jockey+'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        if(index == cnt) {
          if(ongoing){
            calcHorse(req, res, query, selectDate, month, false);
          }else{
            let start = now.toLocaleString();
            let end = new Date().toLocaleString();
            sendJsonData(req, res, {status : 'JOCKEY', startTime : start, endTime : end});          
          }
        }
      });
    }); 
  });
}
//after sire
async function calcHorse(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _horse FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "Horse_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "horse", title: "horse" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let name = item._horse;
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _horse = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _horse = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _horse = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _horse = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _horse = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _horse = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"horse":"'+ name +'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        // IVs.push(data[0].iv);
        if(index == cnt) {
          if(ongoing){
            calcSire(req, res, query, selectDate, month);
          }else{
            let start = now.toLocaleString();
            let end = new Date().toLocaleString();
            sendJsonData(req, res, {status : 'SIRE', startTime : start, endTime : end});          
          }
        }
      });
    }); 
  });
}
//after dam
async function calcSire(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _sire FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "Sire_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "sire", title: "sire" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let name = item._sire;
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _sire = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _sire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _sire = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _sire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _sire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _sire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        // console.log("res = ", index, name, data[0].calc, data[1].calc, data[2].calc, data[3].calc);
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"sire":"'+ name +'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        // IVs.push(data[0].iv);
        if(index == cnt) {
          if(ongoing){
            calcDam(req, res, query, selectDate, month);
          }else{
            let start = now.toLocaleString();
            let end = new Date().toLocaleString();
            sendJsonData(req, res, {jockey : true, startTime : start, endTime : end});          
          }
        }
      });
    }); 
  });
}
//after damsire
async function calcDam(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _dam FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "Dam_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "dam", title: "dam" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let name = item._dam;
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _dam = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _dam = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _dam = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _dam = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _dam = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _dam = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        // console.log("res = ", index, name, data[0].calc, data[1].calc, data[2].calc, data[3].calc);
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"dam":"'+ name +'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        // IVs.push(data[0].iv);
        if(index == cnt) {
          if(ongoing){
            calcDamsire(req, res, query, selectDate, month);
          }else{
            let start = now.toLocaleString();
            let end = new Date().toLocaleString();
            sendJsonData(req, res, {jockey : true, startTime : start, endTime : end});          
          }
        }
      });
    }); 
  });
}
//after finish
async function calcDamSire(req, res, query, selectDate, month, ongoing = true) {
  let sQuery = "SELECT DISTINCT _damsire FROM tbl_horse_racing WHERE _date > '" + selectDate + "' - INTERVAL " + month + " month;";
  // let IVs = [];
  _query(query, sQuery)
  .then(data => {
    let index = 0;
    const cnt = data.length;
    const now = new Date();
    const csvWriter = createCsvWriter({
      path: __basedir + "/reports/" + "DamSire_" + selectDate + "_" + month + "mon(" + now.toDateString() + "," + now.getHours() + "h_" + now.getMinutes() + "m_" + now.getSeconds() + "s).csv",
      header: [
        { id: "damsire", title: "damsire" },
        { id: "iv", title: "IV" },
        { id: "ae", title: "A/E" },
        { id: "prb", title: "PRB" },
        { id: "prb2", title: "PRB2" }
      ]
    });
    data.forEach(item => {
      let name = item._damsire;
      sQuery = "SELECT ROUND(jwr.cnt / awr.cnt, 3) as calc " +
      "FROM (" +
            "SELECT (r.cnt / o.cnt) as cnt " +
            "FROM (" +
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _damsire = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month " +
                  ") r cross join " +
                  "("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing " +
                  "WHERE _damsire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
                ") o"+
          ") jwr cross join " +
          "("+
            "SELECT (r.cnt / o.cnt) as cnt "+
            "FROM ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                  ") r cross join ("+
                  "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
                  "WHERE _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
                ") o"+
          ") awr " +
          "UNION ALL "+
          "SELECT ROUND(r.cnt / o.cnt, 3) "+
          "FROM ("+
			          "SELECT COUNT(_jockey) as cnt FROM tbl_horse_racing "+
			          "WHERE _damsire = '"+ name +"' AND _pos = '1' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month" +
			          ") r cross join "+
			          "("+
			          "SELECT SUM(AN) as cnt FROM tbl_horse_racing "+
			          "WHERE _damsire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month"+
			          ") o "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AQ), 3) FROM tbl_horse_racing " +
          "WHERE _damsire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month "+
          "UNION ALL "+
          "SELECT ROUND(AVG(AR), 3) FROM tbl_horse_racing "+
          "WHERE _damsire = '"+ name +"' AND _date > '"+ selectDate +"' - INTERVAL " + month + " month;";
      _query(query, sQuery)
      .then(data => {
        index++;
        // console.log("res = ", index, name, data[0].calc, data[1].calc, data[2].calc, data[3].calc);
        const jsonData1 = JSON.parse(JSON.stringify(data));
        let jsonString = '[';
        jsonString += '{"damsire":"'+ name +'",';
        jsonString += '"iv":'+jsonData1[0].calc+',';
        jsonString += '"ae":'+jsonData1[1].calc+',';
        jsonString += '"prb":'+jsonData1[2].calc+',';
        jsonString += '"prb2":'+jsonData1[3].calc+'}]';
        const jsonData = JSON.parse(jsonString);
        csvWriter
          .writeRecords(jsonData)
          .then(() =>
            console.log(">>ROW ", index)
          );    
    
        // IVs.push(data[0].iv);
        if(index == cnt) {
          let start = now.toLocaleString();
          let end = new Date().toLocaleString();
          sendJsonData(req, res, {jockey : true, startTime : start, endTime : end});          
        }
      });
    }); 
  });
}

async function _query(query, squery) {
  try {
    return await query(squery);
    // const rows = await query(squery);
    // console.log("CNT = ", rows.length);
    // return rows;
  } catch(e) {
    throw(e);
  }
};

const sendJsonData = function (req, res, json) {
  res.render('report', json);
};

module.exports = report;