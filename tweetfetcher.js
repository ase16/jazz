'use strict'

const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

let db;

// Twitter API module
// https://github.com/ttezel/twit
const twit = require('twit');

// Useful links for the Twitter API:
// Tweet JSON: https://dev.twitter.com/overview/api/tweets
// Stream API: https://dev.twitter.com/streaming/overview

// #tweets and ts when last updated.
var stats = {
    timestamp: new Date().getTime(),
    tweetsCount: 0
};

// @parma callback fn(arr) - get array of keywords
function getKeywords(callback) {
    db.getAllTerms(function(err, words) {
        var keywords = [];
        if (err) {
            log.error("Could not retrieve terms from database.", err);
        } else {
            keywords = words;
        }

        // fetching tweets given keywords in a file
        // useful for debugging

        var fs = require('fs');
        var keywordsFile = 'keywords.txt';
        try {
            fs.accessSync(keywordsFile, fs.R_OK);
            keywords = fs.readFileSync(keywordsFile).toString().split('\n');
        } catch (e) {
            log.error("Cannot access keywords file: " + keywordsFile);
            keywords = [];
        }


        // some basic cleansing
        keywords = keywords.map(function(k) {
            return k.toLowerCase().trim();
        });
        keywords = keywords.filter(function(k) {
            return k.length > 0;
        });

        callback(keywords);
    });
}

// processes incoming tweet. The tweet is inserted without
// additional processing into the tweets collection.
function onNewTweet(tweet) {
    log.debug('New Tweet:\t[id: %s, text: %s]', tweet['id_str'], tweet['text']);
    //console.log(tweet);

    db.insertTweet(tweet, function(err, result){
        if (err) {
            log.error('Could not insert tweet:', err);
        } else {
            log.debug('Inserted tweet into the database.');
            stats.tweetsCount += 1;
        }
    });
}

// set up twitter stream and subscribe to keywords
// @param onNewTweet function to call when new tweet is received
// @param callback fn(stream)
function subscribeToTweets(callback) {
    getKeywords(function(keywords) {
        var stream = twitter.stream('statuses/filter', {
            track: keywords,
            language: 'en'
        });
        stream.on('tweet', onNewTweet);

        // set up some logging
        // the messages are described here:
        // https://dev.twitter.com/streaming/overview/messages-types
        stream.on('connect', function (request) {
            log.info('Twitter - Connect');
        });
        stream.on('connected', function (response) {
            log.info('Twitter - Connected');
        });
        stream.on('disconnect', function (disconnectMessage) {
            log.warn('Twitter - Disconnect');
        });
        stream.on('reconnect', function (request, response, connectInterval) {
            log.info('Twitter - Reconnect in %s ms', connectInterval);
        });
        stream.on('limit', function (limitMessage) {
            log.warn('Twitter - Limit: ', limitMessage);
        });
        stream.on('warning', function (warning) {
            log.warn('Twitter - Warning: ', warning);
        });
        stream.on('error', function (error) {
            log.warn('Twitter - Error: ', error)
        });

        log.info('Set up connectio n to twitter stream API.');
        log.info('Stream keywords (%s): %s', keywords.length, keywords);

        callback(stream);
    });
}

// prints some statistics about the tweets in the DB and the received tweets.
function logStats() {
    var now = new Date().getTime();
    var timeSpan = now-stats['timestamp'];
    var newTweets = stats['tweetsCount'];
    var tweetsPerSec = (newTweets/timeSpan*1000).toFixed(1);
    if (isNaN(tweetsPerSec)) { tweetsPerSec=0.0 }
    stats.timestamp = now;
    stats.tweetsCount = 0;
    
    log.info('Fetching ' + tweetsPerSec + ' tweets/second');
}

var twitterCredentials = config.get('twitter');
var twitter = new twit(twitterCredentials);
var twitterStream;

const tweetfetcher = {

    init: function(dbModule, callback) {

        db = dbModule;

        // set up logging of stats
        setInterval(logStats, 10*1000);

        // connect to twitter
        subscribeToTweets(function(stream) {
            twitterStream = stream;
            callback();
            log.info("Tweetfetcher initialized");
        });
    },
};

module.exports = tweetfetcher;