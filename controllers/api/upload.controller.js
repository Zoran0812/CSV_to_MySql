const fs = require("fs");
const mysql = require("mysql");
const fastcsv = require("fast-csv");

const upload = function(req, res){ 
  let uploadFile;
  let uploadPath;

  if (!req.files || Object.keys(req.files).length === 0) {
    return res.status(400).send('No files were uploaded.');
  }

  uploadFile = req.files.upload_file;
  targetPath = '/uploads/' + uploadFile.name;
  uploadPath = __basedir + targetPath;
  
  console.log("uploadPath : " , uploadPath);
  
  // Use the mv() method to place the file somewhere on your server
  uploadFile.mv(uploadPath, function(err) {
    if (err)
      return res.status(500).send(err);

    // res.send('File uploaded!'); 
    // sendJsonData(req, res, { UploadStatus: 'success' });
    // res.json({ finished: false });
  });
  // sendJsonData(req, res, { converting: true });
  // let stream = fs.createReadStream(__basedir + '/uploads/' + "bezkoder.csv");
  let stream = fs.createReadStream(uploadPath);
  let csvData = [];
  let csvStream = fastcsv
    .parse()
    .on("data", function(data) {
      csvData.push(data);
    })
    .on("end", function() {
      // remove the first line: header
      csvData.shift();
      console.log("++++++++++++",csvData.length);
      // create a new connection to the database
      const connection = mysql.createConnection({
        host: "localhost",
        user: "root",
        password: "",
        database: process.env.database || "csvdb"
      });

      // open the connection
      connection.connect(error => {
        if (error) {
          console.error(error);
        } else {
          // let data1 = [];
          // data1.push(csvData[0]);
          // data1.push(csvData[1]);
          // console.log("DATA = ", data1);
          // let query =
          //   "INSERT INTO category (id, name, description, created_at) VALUES ?";
          // connection.query(query, [data1], (error, response) => {
          //   console.log(error || response);
          // });
          let writing = true;
          let once = 20;
          const total = csvData.length;
          let counter = 0;
          // let query = "INSERT INTO tbl_horse_racing (date,region,course,off,race_name,type) VALUES ?";
          let query = "INSERT INTO tbl_horse_racing (_date,_region,_course,_off,_race_name,_type,_class,_pattern,_rating_band,_age_band,_sex_rest,_dist,_dist_f,_dist_m,_going,_ran,_num,_pos,_draw,_ovr_btn,_btn,_horse,_age,_sex,_lbs,_hg,_time,_secs,_dec,_jockey,_trainer,_prize,_or,_rpr,_sire,_dam,_damsire,_owner,_comment,AN,AO,AP,AQ,AR) VALUES ?";
          while (writing) {
            let onceData = [];
            if(csvData.length <= once) {
              once = csvData.length;
              writing = false;
            }
            for (let i = 0; i < once; i++) {
              let segment = csvData[0];
              let date = getDate(segment[0]);
              segment.shift();
              segment.unshift(date);
              // console.log("segment = ",segment);
              onceData.push(segment);
              csvData.shift();
            }

            connection.query(query, [onceData], (error, response) => {
              console.log(error || response);
              counter += response.affectedRows;
              if(counter == total){
                console.log(">>>>", total);
                sendJsonData(req, res, { finished : true });
              }
            });
          }
        }
      });
    });
  stream.pipe(csvStream);
  // sendJsonData(req, res, { converting: false });
}

function getDate(sDate) {
  let date = sDate.split('/');
  return date[2] + '-' + date[1] + '-' + date[0];
}

const sendJsonData = function (req, res, json) {
  res.render('convert', json);
}

module.exports = upload;