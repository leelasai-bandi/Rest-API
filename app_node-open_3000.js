const Promise = require('bluebird');
//const http = require('http');
const opts = {
        errorEventName:'error',
        logDirectory:'/usr/nodejs-log/', // NOTE: folder must exist and be writable...
        fileNamePattern:'open-data-api-<DATE>.log',
        dateFormat:'YYYY.MM.DD-HH'
};
const log = require('simple-node-logger').createRollingFileLogger(opts);
const config = require('./config.js');
const DB = require('./db.js');
const datasets = require('./datasets.js');
var app = require('express')();
app.disable('x-powered-by');
var shape = require('shape-json');
//var apigee = require('apigee-access');
var moment = require('moment');
Date.prototype.toJSON = function() { return moment(this).format('YYYY-MM-DD HH:mm:ss'); }

app.get('/:org/:dataset', function(req, res, next) {
    var query = '';
    var org = req.params.org.toLowerCase();
        var datasetName = req.params.dataset.toLowerCase();
        var _column = req.query.column;
        var _limit  = req.query.limit;
        var _filter = req.query.filter;
        var skip = req.query.offset || 0;
    var order = req.query.order_by || '';
    var dev_usrname = req.header('usr');
    var count_query;
    var dTime=new Date();
    var cdKey=moment(dTime).format("YYYYMMDD-HHmmss-")+Math.floor(Math.random() * 100000)+9999;
    log.info("cdKey :"+cdKey);

    if(req.query.column !== undefined)
    {
     _column=_column.split(" ").join("");
         log.info("_column :"+"'"+_column+"'");}
    //log.info('recieved request: org=' + org + ' | dataset=' + datasetName + ' | column=' + _column + ' | filter=' + _filter + ' | limit=' + _limit + ' | skip=' + skip);
    //log.info('recieved request: org=' + org + ' | dataset=' + datasetName + ' | column=' + _column + ' | filter=' + _filter + ' | limit=' + _limit + ' | skip=' + skip);
    log.info('recieved request: org=' + org + ' | dataset=' + datasetName + ' | column=' + _column + ' | filter=' + _filter + ' | limit=' + _limit + ' | skip=' + skip +' | userName =' +dev_usrname+'order by='+order);
        var csv = (req.get('Accept') === 'text/csv' || req.query.csv === 'true') ? true : false;
    var pool;
   var pgLimit=1000;
//var pgLimit=apigee.getVariable(req,'limit')

 //Username retrival from "Set Developer Policy"
  /*  var dev_usrname = apigee.getVariable(req,'apigee.username');
    var dev_fname=apigee.getVariable(req,'apigee.fname');
    var dev_lname=apigee.getVariable(req,'apigee.lname');
    var dev_email=apigee.getVariable(req,'apigee.developer.email');
    var QCount=apigee.getVariable(req,'apigee.quotaCount');
    var apiProd=apigee.getVariable(req,'apigee.apiproduct.name');
    var usr_ip=apigee.getVariable(req,'request.header.X-Forwarded-For');

    //Retriving Eid from username
    var str_dusr =  String(dev_usrname);
    str_dusr = str_dusr.substr(8,15);
    var dTime=new Date();

    const timeEpoch = Math.round(dTime.getTime() / 1000);
    log.info('|usrip="'+usr_ip+'"|Request_URL="'+req.url+'"|userDevApp="'+dev_usrname+'"|userFirstName="'+dev_fname+'"|userLastName="'+dev_lname+'"|userEmail="'+dev_email+'"|eId="'+str_dusr +'"|productName="'+apiProd+'"|availableQuota="'+QCount+'"|entityId="'+org+'"|datasetName="'+datasetName+'"|dateTime="'+dTime+'"|epochTime="'+timeEpoch+'"');*/
var newJsn = [];
var dataset_classify_type;
var json_output;

var scheme={};
var rowCount = 0;
      if (dev_usrname!== undefined)
      {
      if(parseInt(_limit) < parseInt(pgLimit) || _limit === undefined)
      {
    config.getPgPool().then(function(pgPool) {
        pool = pgPool;
        return datasets.lookup(org, datasetName,pool);
    })
                .then(function(tableDef) {
                        dataset_classify_type=tableDef.dtype;
                        json_output=tableDef.output;
                        scheme=tableDef.schema;
                        count_query = 'Select count(1) from ' + tableDef.pgTable + ';';
                        log.info('Dataset_Classify & JSON Output :'+tableDef.dtype+' '+tableDef.output)
                //Handler response type
                if (csv) {
                    res.setHeader('Content-Type', 'text/csv');
                    res.setHeader('Content-Disposition', 'attachment; filename=' + tableDef.csvFileName);
                } else {
                            res.setHeader('Content-Type', 'application/json');
                    }
                    return DB.buildQuery(_column, _filter, _limit, tableDef, skip, order);
                })
      .then(function(q) {
        query = q;
        if (csv) query.rowMode = 'array';
        return  DB.pgClient(pool);
      })
      .then(function(client) {
        if(dataset_classify_type === 'open')
                    {
                     if(json_output === 'nested-json')
        {

        DB.queryStream(query, client.client, function(row) {
                        //rowCount++;
                        newJsn.push(row);
           }, function() {
                var str_newJsn=JSON.stringify(newJsn[0]);
                var prs_newJsn=JSON.parse(str_newJsn)
                 var prsd_schma=JSON.parse(scheme);
                var nstdjsn=shape.parse(prs_newJsn, prsd_schma);
                log.info("Row Count :"+rowCount)
                log.info('Nested data End');
                        client.release();
                                 res.setHeader('Content-Type', 'application/json');
                                 res.send(JSON.stringify(nstdjsn, null, 3));
                        }, function(e) {
                    log.info('query failed (queryStream).', e);
                        var err = config.Error(config.errors._003);
                        return next(err);
                });
        }
        else if(json_output === 'json')
        {
        //var resHeader = (csv) ? query.columns + '\n' : '{"results": [';
        var resHeader = (csv) ? query.columns + '\n' : '{"results": ';
        var prefix = ''; suffix = (csv) ? '\n' : '';
        DB.queryStream(query, client.client, function(row) {
                        if (resHeader) res.write(resHeader); resHeader = false;
	                var data = (!csv) ? JSON.stringify(row) : row.toString();
			console.log('write resHeader')
                        res.write(prefix + data + suffix);
                        prefix = (!csv) ? ',' : '';
                        //rowCount++;
           }, function() {
                        //var count_query = 'Select count(1) from ' + tableDef.pgTable + ';';
                        //log.info("count_query",count_query)
                        client.release();
			if (resHeader) res.write(resHeader);
			if (!csv) { //JSON end response
                                //res.write(']}');
                                res.write('}');
                        }
			res.end();
			/*DB.queryStream(count_query, client.client, function(row) {
                            rowCount = row.count
                        }, function() {
              		if (resHeader) res.write(resHeader);
                        client.release();
                        if (!csv) { //JSON end response
                                res.write('], "count": ' + rowCount + '}');
                        }
                        res.end();
                        }, function(e) {
                    log.info('query failed (queryStream).', e);
                        var err = config.Error(config.errors._003);
			client.release();
                        return next(err);
                });*/
        }, function(e) {
                    log.info('query failed (queryStream).', e);
                        var err = config.Error(config.errors._003);
			client.release();
                        return next(err);
                });
                    }
                    }
                    else
                    {
                         var err = config.Error(config.errors._005);
                        return next(err);
                    }
        })
    .catch(function(err) {
        return next(err);
    });

      }
      else
    {

                var err = {name: 'Limit exceeded, please use limit less than '+pgLimit,status: 412,errorString: 'Limit exceeded.'}
                                return next(err);
    }
    }
	 else 
	 {
	  var err = {name: 'User is undefined, unauthorized error',status: 412,errorString: 'User undefined'}
                                return next(err);
	 }
});

app.use(function(err, req, res, next) {
        console.error('Error', err);
        err = (err) ? err : config.Error(config.errors._002);
        res.status(err.status).send({
        error: err.name,
        errorString: err.errorString
    });
});

app.use(function (req, res, next) {
        var err = (err) ? err : config.Error(config.errors._004);
    res.status(err.status).send({
                error: err.name,
                errorString: err.errorString
    });
});

app.listen(3000, function() {
    console.log('App listening on port 3000 /home/appadmin/open-data-api-nv0.1/app.js');
});
