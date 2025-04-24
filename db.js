//const dbConfig = require('./db-config.js');
const config = require('./config.js');
const pg = require('pg');
const async = require('async')
const Promise = require('bluebird');
const opts = {
        errorEventName:'error',
        logDirectory:'/usr/nodejs-log/', // NOTE: folder must exist and be writable...
        fileNamePattern:'open-data-api-<DATE>.log',
        dateFormat:'YYYY.MM.DD-HH'
};
const log = require('simple-node-logger').createRollingFileLogger(opts);

//timezone fix snippet
var types = pg.types;
types.setTypeParser(1114, function(stringValue) {
return stringValue;
});


const DB = {
        buildQuery: function(column, filter, limit, tableDef, offset, order) {
                var $this = this;
                var _query = 'SELECT ';
                log.info('filter:' + filter);
                var _from = ' FROM ' + tableDef.pgTable;
                var _columns = tableDef.pgColumns.toString();
                var _columnsArr = (column) ? column.split(',') : [];
                return new Promise(function (resolve, reject) {
                        //columns
                        $this.validateColumns(_columnsArr, tableDef)
                                .then(function(c_columns) {
                                        if (c_columns) _columns = c_columns;
                                        return Promise.resolve();
                                })
                                //filter
                                .then(function() {
                                        if (filter) filter = ' WHERE ' + filter;
                                        else filter = '';
                                        return Promise.resolve();
                                })
                                //limit
                                .then(function() {
                                        limit = (limit) ? limit : config.pgDefaultLimit;
                                        if (limit) limit = ' LIMIT ' + limit;
                                        else limit = '';
                                        return Promise.resolve();
                                })
				.then(function() {
					if (offset) offset = ' OFFSET ' + offset;
					else offset = '';
					return Promise.resolve();
				})
				.then(function() {
					if (order) order = ' ORDER BY ' + order;
					else order = '';
					return Promise.resolve();
				})
                                //finalize                                
				.then(function() {
					_query += _columns + _from + filter + order + limit + offset;
					//_query = 'Select ' + _columns + ' from (' + _query + ') DATA;'
					log.info('final query: ' + _query);
					resolve({text: _query, columns: _columns});
				})
                	        .catch(function(err) {
                        	        reject(err);
                        	});
                });
        },
        validateColumns: function(columnsArr, tableDef) {
                var $this = this;
                //log.info('checking column array: ' + columnsArr);
                return new Promise(function (resolve, reject) {
                        async.map(columnsArr, function(column, next) {
                                //log.info('checking column: ' + column);
                                if (tableDef.pgColumns.includes(column)) next(false, column);
                                else next(true); //throw error
                        }, function(err, c_columns) {
                                if (err) reject(config.Error(config.errors._001));
                                else {
                                        log.info('clean columns: ' + c_columns);
                                        resolve(c_columns.toString());
                                }
                        });
                });
        },
       /* client: function() {
                log.info("Entered DB client function call..");
                var hrstart = process.hrtime();
                var $this = this;
                return new Promise(function (resolve, reject) {
                        $this.pool.connect(function (err, client, release) {
                                var hrend = process.hrtime(hrstart);
                                log.info("client PG connection elapsed time ", hrend[1]/1000000);
                                if (err) {
                                        reject(config.Error(config.errors._002));
                                } else {
                                        resolve({client: client, release: release});
                                }
                        });
                });
        },*/
        pgClient: function(pool) {
                log.info("Entered DB pgClient function call..");
		var hrstart = process.hrtime();
                var $this = this;
                return new Promise(function (resolve, reject) {
                        pool.connect(function (err, client, release) {
                                var hrend = process.hrtime(hrstart);
                                log.info("pgClient PG connection elapsed time ", hrend[1]/1000000);
                                if (err) {
                                    log.info(err);
                                        reject(config.Error(config.errors._002));
                                } else {
                                        resolve({client: client, release: release});
                                }
                        });
                });
        },
        /*queryStream: function(query, client, onRow, onEnd, onError) {
                var pgQuery = client.query(query);
                pgQuery.on('row', onRow);
                pgQuery.on('end', onEnd);
                pgQuery.on('error', onError);
        }*/
        queryStream: function(query, client, onRow, onEnd, onError) {
        console.log("querystreamcall")
        // var pgQuery = client.query(query);
        client.query(query, (err, res) => {
            //console.log("res",res.rows)
            if (err) {
              onError(err)
            } else {
              onRow(res.rows)
              onEnd(res.rows.length)
            }
          })
        // pgQuery.on('row', onRow);
        // pgQuery.on('end', onEnd);
        // pgQuery.on('error', onError);
    }
};

/*pg.on('error', function(e) {
        throw config.Error(config.errors._002);
});*/

if(!Array.prototype.includes){Object.defineProperty(Array.prototype,'includes',{value:function(searchElement,fromIndex){if(this==null){throw new TypeError('"this" is null or not defined');}var o=Object(this);var len=o.length>>>0;if(len===0){return false}var n=fromIndex|0;var k=Math.max(n>=0?n:len-Math.abs(n),0);function sameValueZero(x,y){return x===y||(typeof x==='number'&&typeof y==='number'&&isNaN(x)&&isNaN(y))}while(k<len){if(sameValueZero(o[k],searchElement)){return true}k++}return false}})}

module.exports = DB;

