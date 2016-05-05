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
        if (isConnected()) {
            // TODO: implement query!

            // fetching tweets given keywords in a file
            var keywords = []
            var fs = require('fs');
            var keywordsFile = 'keywords.txt';
            try {
                fs.accessSync(keywordsFile, fs.R_OK);
                keywords = fs.readFileSync(keywordsFile).toString().split('\n');
            } catch (e) {
                log.error("Cannot access keywords file: " + keywordsFile);
                keywords = [];
            }

            callback(null, keywords);

        } else {
            callback("Not connected", []);
        }
    },

    getTerm: function(term, callback) {
        // TODO: return entity for given term
    },

    insertTerm: function(term, cid, callback) {
        // TODO: get entity for given term, add cid (if not present yet), call update
    },

    updateTerm: function(term, callback) {
        // TODO: updates a term entity (save it).
    },

    removeTerm: function(term, cid, callback) {
        // TODO. get entity for given term, remove cid (if present), call update or if no cid left -> delete term entity,
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