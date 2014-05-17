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

var ElasticSearchClient = require('elasticsearchclient');

var serverOptions = {
    host: CONFIG.elasticSearchHost,
    port: CONFIG.elasticSearchPort,
    secure: false
}


var elasticSearchClient = new ElasticSearchClient(serverOptions);
console.log("connect to elasticsearchclient at "+CONFIG.elasticSearchHost+":"+CONFIG.elasticSearchPort);
//Delete use curl first:
//curl -XDELETE http://localhost:9200/ss12search/

//commands.push({ "index" : { "_index" :'debates', "_type" : "debate","_id":(i+1)+""}});
//delete debates[i]._id; //remove _id which is confilict with bulk update
//commands.push(debates[i]);
//var sentences =[];
//var tags = [];
//var links = [];
async.waterfall([
	function(callback){ //read sentences.csv
		console.log("Start reading sentences...");
		//callback(null);
		//return;
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
		  callback(null);
		});
	},
	function(callback){ //read links.csv
		var id;
		var sameases = [];
		console.log("Start reading links...");
		var count = 0
		
		csv().from.path('links.csv', {delimiter:'	'}).on('record', function(row, index) {
		  if(!id || id !== row[0]){
		  	//write current sameases to sentences objects
		  	var sentenceId = id;
		  	var sameasesCopy = _.clone(sameases);
		  	var collection = db.get("sentences");
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
		  callback(null);
		});
	},
	function(callback){ //read tags.csv
		console.log("Start reading tags...");
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
	}

],function(err, result){
	var commands = [];

	db.get("sentences").find({},function(err, sentences){
		for(var i=0;i<sentences.length;i++){
			commands.push({ "index" : { "_index" :'ss12search', "_type" : "sentence","_id":(i+1)+""}});
			delete sentences[i]._id;
			commands.push(sentences[i]);
		}
		
		console.log("Start to create index...");
		console.log(commands.length+" commands...");
		elasticSearchClient.bulk(commands,{})
			.on('data', function(data) {
				//console.log(data)
			})
            .on('done', function(done){
            	console.log("Finished.");
            	process.exit(0);
            })
            .on('error', function(error){
            	console.log("Error:"+error);
            	process.exit(1);
            	return;
            })
            .exec();
	});
});




