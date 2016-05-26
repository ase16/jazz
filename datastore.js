'use strict';
const gcloud = require('gcloud');
const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

// Authenticating on a per-API-basis.
let datastore;

// this is a pseudo-check to see if we have a connection
// if the object is not undefined, we assume it has been initialized/set
const isConnected = () => typeof datastore !== "undefined";


// get a list of VMs that have tweets assigned
function getAssignedButDeadVMs(vms, callback) {
    if (!isConnected()) {
        callback("Datastore is not connected", null);
        return;
    }
    var deadVMs = [];
    var query = datastore
        .createQuery('Tweet')
        .select(['vm'])
        .groupBy(['vm']);
    datastore.runQuery(query, function(err, res) {
        if (err || !Array.isArray(res)) {
            return onTweetCallback(err, null);
        }
        // filter 'dead' vms
        res.forEach(function(v) {
            if (vms.indexOf(v.data.vm) == -1) {
                deadVMs.push(v.data.vm);
            }
        });
        callback(null, deadVMs);
    });
}

const db = {
    connect: function(conf, callback) {
        // do we already have a connection?
        if (isConnected()) return;
        datastore = gcloud.datastore(conf);
        callback();
    },

    // @param callback fn(err, res)
    // --> res is an array containing terms e.g. ['jay-z', 'google', 'solarpower']
    getAllTerms: function(callback) {
        if (!isConnected()) {
            return callback("Datastore is not connected", []);
        }

        var keywords = [];
        var query = datastore.createQuery('Term');				// --> https://googlecloudplatform.github.io/gcloud-node/#/docs/v0.32.0/datastore?method=createQuery
        datastore.runQuery(query, function(err, entities) {		// --> https://googlecloudplatform.github.io/gcloud-node/#/docs/v0.32.0/datastore?method=runQuery
            if (err) {
                log.error("datastore.readTerms error: Error = ", err);
                return callback(err, null);
            }

            entities.forEach(function(e) {
                keywords.push(e.key.name);
            });
            callback(null, keywords);
        });
    },

    // Add a new tweet.
    // @param tweet JSON as received from the Twitter API
    // @param callback fn(err, res)
    insertTweet: function(tweet, vm, callback) {
        if (!isConnected()) {
            callback("Datastore is not connected", null);
            return;
        }
        var tweetKey = datastore.key(['Tweet', tweet['id_str']]);
        var time = Date.parse(tweet['created_at']);
        if (isNaN(time)) {
            time = Date.now();
        }
        datastore.save({
                key: tweetKey,
                data: {
                    created_at: time,
                    vm: vm,
                    tweet: tweet['text']
                }
        }, function(err) {
            if (err) {
                callback(err);
                return;
            }
            callback(null, tweetKey);
        });
    },

    updateTweet: function(tweet, callback) {
        if (!isConnected()) {
            callback("Datastore is not connected", null);
            return;
        }

        datastore.upsert(tweet, callback);
    },

    getLostTweets: function(vms, onTweetCallback) {
        if (!isConnected()) {
            onTweetCallback("Datastore is not connected", null);
            return;
        }

        getAssignedButDeadVMs(vms, function(err, deadVMs) {
            deadVMs.forEach(function(v) {
                log.info("Reassign tweets of: " + v);
                var deadTweetsQuery = datastore
                    .createQuery('Tweet')
                    .autoPaginate(true)
                    .filter('vm', '=', v);
                datastore.runQuery(deadTweetsQuery)
                    .on('error', log.error)
                    .on('data', onTweetCallback)
                    .on('end', function() {
                        log.info("Finished reassigning tweets of: " + v);
                    });
            });
        });
    },

    storeStat: function(newStat, callback) {
        if (!isConnected()) {
            callback("Datastore is not connected", null);
            return;
        }

        var key = datastore.key(['JazzStat']);

        datastore.save({
            key: key,
            data: {
                created: newStat.created,
                tweetsPerSec: newStat.tweetsPerSec
            }
        }, function(err) {
            if (err) {
                callback(err);
                return;
            }
            callback(null, key);
        });
    }
};

module.exports = db;