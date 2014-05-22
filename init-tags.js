//make sure elastic search has started at localhost:9200
//delete the old index 
//replace all double quote in sentences.csv

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

console.log("Start reading tags...");
var collection = db.get("sentences");

var count=0;
csv().from.path('tags.csv', {delimiter:'	'}).on('record', function(row, index) {
  var id = row[0];
  var tag = row[1];
  collection.findAndModify({id:id},{$set:{tag:tag}});
  count++;
  if(count%10000 === 0)
  	console.log(count+" tags processed.")
  //console.log('#'+index+' '+JSON.stringify(row));
}).on('end', function(count) {
  console.log(count+" tags added.");
  callback(null);
});





