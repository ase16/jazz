'use strict'

const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

const bucketSize = 1000;
var currentBucket = 0;
var currentCountInBucket = 0;

let db;

// Twitter API module
// https://github.com/ttezel/twit
const twit = require('twit');

// Useful links for the Twitter API:
// Tweet JSON: https://dev.twitter.com/overview/api/tweets
// Stream API: https://dev.twitter.com/streaming/overview

// tweets in db and ts when queried.
var stats = {
    timestamp: 0,
    numberOfTweets: 0
};

// @parma callback fn(arr) - get array of keywords
function getKeywords(callback) {
    // TODO: allow adding new keywords on the fly, (e.g. onNewKeywordAdded -> need to set up stream again for that)

    db.getAllTerms(function(err, words) {
        var keywords = [];
        if (err) {
            log.error("Could not retrieve terms from database.", err);
        } else {
            keywords = words;
        }

        // fetching tweets given keywords in a file
        /*
        var fs = require('fs');
        var keywordsFile = 'keywords.txt';
        try {
            fs.accessSync(keywordsFile, fs.R_OK);
            keywords = fs.readFileSync(keywordsFile).toString().split('\n');
        } catch (e) {
            log.error("Cannot access keywords file: " + keywordsFile);
            keywords = [];
        }
        */

        keywords = keywords.filter(function(k) {
            return k.length > 1;
        });
        keywords = keywords.map(function(k) {
            return k.toLowerCase();
        });
        callback(keywords);
    });
}

// processes incoming tweet. The tweet is inserted without
// additional processing into the tweets collection.
function onNewTweet(tweet) {
    log.debug('New Tweet:\t[id: %s, text: %s]', tweet['id_str'], tweet['text']);
    //console.log(tweet);

    db.insertTweetIntoBucket(tweet, currentBucket, function (err, result) {
        if (err) {
            log.error('Could not insert tweet:', err);
        } else {
            currentCountInBucket = result.value.count;
            log.debug('Inserted tweet into the database. Current bucket: %s, count: %s',
                                                    currentBucket, currentCountInBucket);
            if (currentCountInBucket >= bucketSize) {
                currentBucket += 1;
            }
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
            log.info('Twitter - Connect.');
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
function logStats(db) {
    tweetfetcher.getStats((data) => log.info(data))
}

// sets up the tweets collection and logging stats
function setupTweetsCollection() {
    db.createTweetsCollection(function(err) {
        // log number of tweets in the db from time to time.
        db.countTweets(function(err, count) {
            stats = {
                timestamp: new Date().getTime(),
                numberOfTweets: count
            };
            logStats(db);
        });
        setInterval(logStats, 10*1000, db);
    });
}

var twitterCredentials = config.get('twitter');
var twitter = new twit(twitterCredentials);
var twitterStream;

const tweetfetcher = {

    init: function(dbModule, callback) {

        db = dbModule;

        setupTweetsCollection();

        db.getCurrentTweetBucket(function(err, res) {
            if (err) {
                log.warn('Error retrieving current tweet bucket.', err);
            } else {
                if (res) {
                    currentBucket = res['bucket'];
                    currentCountInBucket = res['count'];
                } else {
                    currentBucket = 0;
                    currentCountInBucket = 0;
                }

                log.info('Current tweet bucket: %s, count: %s', currentBucket, currentCountInBucket);

                subscribeToTweets(function(stream) {
                    twitterStream = stream;
                    callback()
                    log.info("Tweetfetcher initialized")
                });
            }
        });
    },

    getStats : function(callback) {
        db.countTweets(function(err, count) {
            if (!err) {
                var now = new Date().getTime();
                var newTweets = count-stats['numberOfTweets'];
                var timeSpan = now-stats['timestamp'];
                var tweetspersec = (newTweets/timeSpan*1000).toFixed(1);
                if (isNaN(tweetspersec)) { tweetspersec=0 }
                stats = {
                    'timestamp': now,
                    'numberOfTweets': count
                };

                // TODO: move this somewhere else - this is just for the demonstration!
                // TODO: make a aggregation query that fetches all info in 1 query ("group by")

                /**
                 Query (make sure that the indexes are used! otherwise the query is very slow and expensive)
                 db.tweets.aggregate(
                 {
                     $group: {
                         _id: {
                            inProgress: "$inProgress",
                            analyzed : "$analyzed"
                            },
                         count: { $sum: 1 }
                     }
                 }
                 )
                 */
                db.countTweetsAnalyzed(function(err, countAnalyzed) {
                   db.countTweetsInProgress(function(err, countInProgress) {
                       db.countTweetsPending(function(err, countPending) {
                           callback(
                               count + ' tweets in database ('
                               + countAnalyzed + ' analyzed, '
                               + countInProgress + ' in progress, '
                               + countPending + ' pending), currently fetching '
                               + tweetspersec + ' tweets/second.');
                       })
                   })
                });


            }
        });
    }
}

module.exports = tweetfetcher;