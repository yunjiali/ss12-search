 //FIRSTLY replace all double quote in sentences.csv

var _=require('underscore');
var async = require('async');
var S=require('string');
var CONFIG=require('config').Crawler;
var csv = require('csv');
var monk = require('monk');


var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
console.log('Connect to mongodb...');

console.log("Start reading sentences...");

var collection = db.get("sentences");
csv().from.path('sentences.csv', {delimiter:'	'}).on('record', function(row, index) {
  var entry = {};
  entry.id = row[0];
  entry.lang = row[1];
  entry.text = row[2];
  entry.sameases = [];
  entry.tag = null;
  //sentences.push(entry);
  collection.insert(entry);
  //console.log('#'+index+' '+JSON.stringify(row));
}).on('end', function(count) {
  console.log(count+" sentences added.");
});





