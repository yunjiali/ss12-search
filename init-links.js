//make sure elastic search has started at localhost:9200
//delete the old index 
//replace all double quote in sentences.csv

var _=require('underscore');
var async = require('async');
var S=require('string');
var CONFIG=require('config').Crawler;
var csv = require('csv');
var monk = require('monk');
var argv = require('optimist').argv


if(!argv.no){
	console.error("please specify --no");
	return;
	process.exit(0);
}

var no = argv.no;

var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
console.log('Connect to mongodb...');

var id;
var sameases = [];
console.log("Start reading links...");
var count = 0
var collection = db.get("sentences");
csv().from.path('links'+no+'.csv', {delimiter:'	'}).on('record', function(row, index) {
  if(!id || id !== row[0]){
  	//write current sameases to sentences objects
  	var sentenceId = id;
  	var sameasesCopy = _.clone(sameases);
  	collection.findAndModify({id:sentenceId},{$set:{sameases:sameasesCopy}});
    
  	id = row[0];
  	sameases = [];
  }
  sameases.push(row[1]);
  count++;
  if(count%10000 === 0)
  	console.log(count+" links processed.");
  //console.log('#'+index+' '+JSON.stringify(row));
}).on('end', function(count) {
  console.log(count+" links added.");
});



