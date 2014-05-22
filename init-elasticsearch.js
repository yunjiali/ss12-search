//make sure elastic search has started at localhost:9200
//delete the old index 
//replace all double quote in sentences.csv

var _=require('underscore');
var async = require('async');
var S=require('string');
var CONFIG=require('config').Crawler;
var csv = require('csv');
var monk = require('monk');
var argv=require('optimist').argv;

if(!argv.offset){
	console.log("--offset Please provide the offset. 1, 2, 3, 5 indicate 100thousands, etc.")
}

var LIMIT = 100000



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

var commands = [];

db.get("sentences").find({},{skip:parseInt(argv.offset*100000),limit:LIMIT},function(err, sentences){
	var commandArr = []
	for(var i=0;i<sentences.length;i++){
		commandArr.push({ "index" : { "_index" :'ss12search', "_type" : "sentence","_id":(parseInt(argv.offset*100000)+i+1)+""}});
		delete sentences[i]._id;
		commandArr.push(sentences[i]);
		if(i!==0 && i%10000 === 0){
			commands.push(commandArr);
			commandArr = [];
		}
	}

	commands.push(commandArr);

	console.log("Start to create index...");
	console.log(commands.length+" commands...");
	async.eachSeries(commands,function(commandsArr, commandsCallback){
		elasticSearchClient.bulk(commandsArr,{})
		.on('data', function(data) {
			//console.log(data)
		})
        .on('done', function(done){
        	console.log("Finished indexing one commandArr.");
        	commandsCallback(null);
        })
        .on('error', function(error){
        	console.log("Error:"+error);
        	process.exit(1);
        	return;
        })
        .exec();
	},function(commandsErr, commandsResults){
		console.log("Finished.");
	});
	
});






