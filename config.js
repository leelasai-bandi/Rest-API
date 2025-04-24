//const apigee = require('apigee-access');
const Promise = require('bluebird');
const opts = {
        errorEventName:'error',
        logDirectory:'/usr/nodejs-log/', // NOTE: folder must exist and be writable...
        fileNamePattern:'open-data-api-<DATE>.log',
        dateFormat:'YYYY.MM.DD-HH'
};
const log = require('simple-node-logger').createRollingFileLogger(opts);

const pg = require('pg');
const config = {
        errors: {
        _001: {
            name: 'Bad Request',
            httpStatusCode: 400,
            errorString: 'Bad column name(s), please check your query.'
        },
        _002: {
            name: 'Internal Error',
            httpStatusCode: 500,
            errorString: 'Internal error, please contact DubaiPulse service desk.'
        },
        _003: {
            name: 'Bad Request',
            httpStatusCode: 400,
            errorString: 'Seems like there is something wrong with requested filter. Make sure you\'re using correct column names and valid expressions.'
        },
        _004: {
            name: 'Not Found',
            httpStatusCode: 404,
            errorString: 'Requested resource cannot be found.'
        },
        _005: {
            name: 'Open AM Call Failed',
            httpStatusCode: 404,
            errorString: 'Did not get a respnse from OpenAM.'
        },
        _006: {
            name: 'User Not Found',
            httpStatusCode: 404,
            errorString: 'User Not Found in OpenAM.'
        },
        _007: {
            name: 'User does not have permission to access the resource',
            httpStatusCode: 404,
            errorString: 'Permission Denied.'
        }
    },
      Error: function(code) {
            log.info('entered error logging')
                var cErr = new Error(code.errorString);
                cErr.name = code.name;
                cErr.errorString = code.errorString;
                cErr.status = code.httpStatusCode;
                return cErr;
        },
        readKvmKey: function(kvm, key) {
            //log.info('KVM: getting key=' + key);
        return new Promise(function (resolve, reject) {
            kvm.get(key, function(err, key_value) {
                if (err) log.info('kvm read err: ' + err);
                if (err) reject(err);
                else resolve(key_value);
            });
        });
        },
        pgDefaultLimit: 1000,
        getPgPool: function() {
            var $this = this;
            if ($this.pgPool) {
                log.info('return pool');
                return Promise.resolve($this.pgPool);
            } else {
                /*log.info('reading pool config from kvm');
                    //kvm = apigee.getKeyValueMap('UAT_APIGEE_USER', 'environment');   //Staging ENV
                    kvm = apigee.getKeyValueMap('PROD_APIGEE_USER', 'environment');    //Production ENV
                        var readKeys = [];
                        readKeys.push($this.readKvmKey(kvm, 'host'));
                        readKeys.push($this.readKvmKey(kvm, 'port'));
                        readKeys.push($this.readKvmKey(kvm, 'db'));
                        readKeys.push($this.readKvmKey(kvm, 'ssl'));
                        readKeys.push($this.readKvmKey(kvm, 'user'));
                        readKeys.push($this.readKvmKey(kvm, 'secret'));
                        readKeys.push($this.readKvmKey(kvm, 'defaultLimit'));*/
                        //return Promise.all(readKeys).then(function(values) {
                            //log.info('host :'+values[0]+' port :'+values[1]+'user :'+values[4]+'pss :'+values[5]);
                                $this.pgPool = new pg.Pool({
                                        host: '172.28.44.132',
                                        port: 54321,
                                        database: 'SDP_PROD',
                                        //ssl: (values[3] == 'false') ? false : true,
                                        ssl: false,
                                        user: 'sdp_prod_apigee_user',
                                        password: 'EY87v5gVo#a=mbm6C*l&N8$DZf2824',
					idleTimeoutMillis: 10000,
					allowExitOnIdle: true,
					max: 125
                                });
                                //$this.pgDefaultLimit = values[6];
                        return Promise.resolve($this.pgPool);
                   // });
                }
        }
};

module.exports = config;

