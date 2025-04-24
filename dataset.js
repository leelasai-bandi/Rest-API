const config = require('./config.js');
//const apigee = require('apigee-access');
const Promise = require('bluebird');
const DB = require('./db.js');
const nodeCache = require( "node-cache" );
var dataset_cache = new nodeCache();
var zlib = require('zlib');
const opts = {
        errorEventName:'error',
        logDirectory:'/usr/nodejs-log/', // NOTE: folder must exist and be writable...
        fileNamePattern:'open-data-api-<DATE>.log',
        dateFormat:'YYYY.MM.DD-HH'
};
const log = require('simple-node-logger').createRollingFileLogger(opts);
const datasets = {
        lookup: function(org,name ,pool) {
             var org1=org.toLowerCase()
            var name1=name.toLowerCase()
             var definitions= {}
         var viewdf={}
         var sname="NA";
         var cname;
         var vname;
                 var schema;
                 var output;
                 var dtype;
         var res1;
         var pkey;
            var $this = this;
                return new Promise(function (resolve, reject) {
            var data = dataset_cache.get('cache_viewDF')
            if(data === undefined){  //can be uncommented to feed the cache
            //if(data !== undefined){
            log.info("ENTERED DATA "+data);
            /*log.info("ENTERED ERR"+error)*/
            log.info("ENTERED Promise of datasets.js ");
                config.getPgPool().then(function(pgPool) {
        pool = pgPool;
                return  DB.pgClient(pool);
                })
                .then(function(client) {
                log.info("Client function Entered")
                var query1="select entity,dataset_name,apigee_view_name,apigee_schema_name,apigee_view_columns,apigee_json_schema,apigee_output_type,dataset_classification_type,primary_key from apigee.vw_apigee_dataset_view_map"
                //var query1="select entity,dataset_name,apigee_view_name,apigee_schema_name,apigee_view_columns,apigee_json_schema,apigee_output_type,dataset_classification_type from apigee.vw_apigee_dataset_view_map"
                pool.query(query1, function (err, res) {
                        //dataset_cache.set('cache_viewDF',res,7200);
                        //log.info('Cache captured :'+dataset_cache.get('cache_viewDF'));
                        zlib.gzip(JSON.stringify(res), function (error, result) {if (error) throw error; dataset_cache.set('cache_viewDF',result,7200)});
                        //log.info('res :'+res)
                        res1=res;
                         for (var items in res1['rows'])
          {
          if((res1['rows'][items]['entity']==org1) && (res1['rows'][items]['dataset_name']==name1)){
             vname=res1['rows'][items]['apigee_view_name']
             sname=res1['rows'][items]['apigee_schema_name']
             cname=res1['rows'][items]['apigee_view_columns']
             schema=res1['rows'][items]['apigee_json_schema']
                         output=res1['rows'][items]['apigee_output_type']
                         dtype=res1['rows'][items]['dataset_classification_type']
                         pkey = res1['rows'][items]['primary_key']
             log.info('Non Cache Data in Dataset'+vname+' '+sname+' '+cname+' '+schema+' '+output+' '+dtype+' '+pkey);
             }

          }
           if(sname == "NA")
              {
                  reject(config.Error(config.errors._004));
              }
              else
              {
                //log.info("res1 from data === undefined");
                ss=sname+"."+vname
                viewdf.pgTable=ss
                viewdf.pgColumns=cname.split(",")
                viewdf.csvFileName=name1+".txt"
                viewdf.output=output
                viewdf.dtype=dtype
                viewdf.schema=schema
                viewdf.pk=pkey
                definitions.viewdf=viewdf
                log.info(definitions)
                resolve(definitions.viewdf);
              }

                })

                })

            }
                else
                {
                data = dataset_cache.get('cache_viewDF')
                    log.info("ENTERED else DATA");
                    if(data === undefined){reject(config.Error(config.errors._004))}
                    else
                    {
                     zlib.unzip(data, function (error, result) {if (error) reject(config.Error(config.errors._004));
                    {
                    res1=JSON.parse(result);
                     for (var items in res1['rows'])
          {
          if((res1['rows'][items]['entity']==org1) && (res1['rows'][items]['dataset_name']==name1)){
             vname=res1['rows'][items]['apigee_view_name']
             sname=res1['rows'][items]['apigee_schema_name']
             cname=res1['rows'][items]['apigee_view_columns']
                         schema=res1['rows'][items]['apigee_json_schema']
                         output=res1['rows'][items]['apigee_output_type']
                         dtype=res1['rows'][items]['dataset_classification_type']
                         pkey = res1['rows'][items]['primary_key']
                         log.info('Cache Data in Dataset'+vname+' '+sname+' '+cname+' '+schema+' '+output+' '+dtype+' '+pkey);
          }
          }
          if(sname == "NA")
              {
                  reject(config.Error(config.errors._004));
              }
              else
              {
                log.info("res1 from data");
                ss=sname+"."+vname
                viewdf.pgTable=ss
                viewdf.pgColumns=cname.split(",")
                viewdf.csvFileName=name1+".txt"
                viewdf.output=output
                viewdf.dtype=dtype
                viewdf.schema=schema
                viewdf.pk=pkey
                definitions.viewdf=viewdf
                log.info(definitions);
                resolve(definitions.viewdf);
              }
                    }
                 });
                    }



                }

        })
        }
        };

module.exports = datasets;

