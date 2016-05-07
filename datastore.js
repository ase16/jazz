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
            return callback("Not connected", []);
        }

        var keywords = [];
        var query = datastore.createQuery('Term');				// --> https://googlecloudplatform.github.io/gcloud-node/#/docs/v0.32.0/datastore?method=createQuery
        datastore.runQuery(query, function(err, entities) {		// --> https://googlecloudplatform.github.io/gcloud-node/#/docs/v0.32.0/datastore?method=runQuery
            if (err) {
                log.error("datastore.readTerms error: Entities = ", err);
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
    insertTweet: function(tweet, callback) {
        if (!isConnected()) {
            callback("Not connected", null);
            return;
        }

        var tweetKey = datastore.key('Tweet');
        datastore.save({
                key: tweetKey,
                data: {
                    id_str: tweet['id_str'],
                    created_at: tweet['created_at'],
                    inProgress: false,
                    tweet: tweet['text']
                }
        }, function(err) {
            if (err) {
                callback(err);
                return;
            }
            callback(null, tweetKey);
        });
    }
};

module.exports = db;