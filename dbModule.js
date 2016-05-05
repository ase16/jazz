"use strict";

const config = require('config');
const Log = require('winston');

Log.level = config.get('log.level');

const MongoClient = require('mongodb').MongoClient;

let mongodb;

// returns the mongodb connection string
const mongodbUrl = function() {
    var user = '';
    if (config.get('mongodb.user').length > 0 && config.get('mongodb.password').length > 0) {
        user = config.get('mongodb.user') + 
         ':' + config.get('mongodb.password') + 
         '@';
    }
    return 'mongodb://'
            + user +
              config.get('mongodb.host') +
        ':' + config.get('mongodb.port') +
        '/' + config.get('mongodb.dbname');
};

// this is a pseudo-check to see if we have a connection
// if the object is not undefined, we assume it has been initialized/set
const isConnected = () => typeof mongodb !== "undefined";

const db = {

    // this should only be called at startup
	connect: function(callback) {

        // do we already have a connection?
        if (isConnected()) return;

        const url = mongodbUrl();

        MongoClient.connect(url, function(err, database) {
            if (err) {
                Log.error('DB: Could not connect to mongodb server: %s', url, err);
                process.exit(1);
            } else {
                Log.info('DB: Connected correctly to mongodb server: %s', url);
                mongodb = database;
                callback();
            }
        })
	},

    // @param term e.g. 'clinton'
    // @param cid customer-id aka the email address
    // @param callback fn(err, res)
	insertTerm: function(term, cid, callback) {

        if (isConnected()) {

            mongodb.collection('terms').updateOne(

                // query
                { 'term': term },
                {
                    // with each update we overwrite the term with the exact same val
                    // this is not pretty, but this way we can keep this fn short
                    $set: { 'term': term },
                    $currentDate: { 'updated_at': true },
                    // the referenced customer-ids array is added to keep track of
                    // how many companies dependent on this term
                    $addToSet: { 'ref_cids': cid }
                },
                // if query doesn't match, create new doc
                { upsert: true },
                callback
            )
        }
	},

    // @param term e.g. 'clinton'
    // @param cid customer-id aka the email address
    // @param callback fn(err, result)
	removeTerm: function(term, cid, callback) {

        if (isConnected()) {

            mongodb.collection('terms').updateOne(

                // query
                { term: term },
                { $pull: { 'ref_cids': cid }},
                { upsert: false },
                function(err) {

                    if (err) {
                        return callback(err);
                    }
                    else {
                        mongodb.collection('terms').remove(
                            // query
                            {
                                term: term,
                                // make sure the referenced cid array is empty
                                // to make sure no other company requires the term
                                ref_cids: { $exists: true, $size: 0 }
                            }, callback
                        )
                    }
                }
            )
        }
	},

    // @param callback fn(err, res)
    // --> res is an array containing terms e.g. ['jay-z', 'google', 'solarpower']
    getAllTerms: function(callback) {

        if (isConnected()) {

            // 2nd arg is the projection (which fields should be 'returned')
            mongodb.collection('terms').find({ }, { term: true })
                .toArray( function(err, docs){
                    callback(null, docs.map((d) => d.term));
                })
        } else {
            callback("Not connected", []);
        }
    },

    // @param callback fn(err)
    createTweetsCollection: function(callback) {
        if (isConnected()) {
            // create collection for tweets and indexes
            mongodb.createCollection('tweets', function (err, tweets) {
                // on index created callback
                var indexCreated = function (err, indexName) {
                    if (!err) {
                        Log.info('Index created: %s', indexName);
                    } else {
                        Log.error('Could not create index: %s', err);
                    }
                };

                // 'id_str' is the unique key of twitter ('id' does not work correctly due to JS limitations).
                tweets.createIndex({'id_str': 1}, {unique: true, sparse: true}, indexCreated);

                // add indexes for finding tweets not yet analyzed/processed
                tweets.createIndex({'inProgress': 1, 'analyzed': 1}, {'sparse': true}, indexCreated);
                tweets.createIndex({'analyzed': 1}, {sparse: true}, indexCreated);
                tweets.createIndex({'inProgress': 1}, {sparse: true}, indexCreated);

                // text index on the tweets (word lookup)
                tweets.createIndex({'text': 'text'}, {}, indexCreated);

                callback(err);
            });
        } else {
            callback("Not connected");
        }
    },

    // @param callback fn(err, count)
    countTweets: function(callback) {
        if (isConnected()) {
            mongodb.collection('tweets').count(callback);
        } else {
            callback("Not connected", 0);
        }
    },

    // analyzed tweets are "completed"
    countTweetsAnalyzed: function(callback) {
        if (isConnected()) {
            mongodb.collection('tweets').count({analyzed: true}, callback);
        } else {
            callback("Not connected", 0);
        }
    },

    // tweets in progress are currently being analyzed.
    countTweetsInProgress: function(callback) {
        if (isConnected()) {
            mongodb.collection('tweets').count({analyzed: false, inProgress: true}, callback);
        } else {
            callback("Not connected.", 0);
        }
    },

    // pending tweets are tweets that are not analyzed yet and not currently in progress
    countTweetsPending: function(callback) {
        if (isConnected()) {
            mongodb.collection('tweets').count({analyzed: false, inProgress: false}, callback);
        } else {
            callback("Not connected.", 0);
        }
    },

    // Add a new tweet to the tweets collection
    // @param tweet JSON as received from the Twitter API
    // @param callback fn(err, res)
    insertTweet: function(tweet, callback) {
        if (isConnected()) {
            tweet['analyzed'] = false;      // set to true if this tweet was analyzed successfully.
            tweet['inProgress'] = false;    // set to true if this tweet is being processed right now.
            mongodb.collection('tweets').insertOne(tweet, callback);
            /**
             * analyzed / inProgress booleans lead to 4 possible states
             *  A   iP
             * --------
             *  0 | 0   -> New tweet, ready to be analyzed
             *  0 | 1   -> Tweet is currently analyzed by a worker. If too long iP, we could reset it to (0,0). Could happen if worker dies
             *  1 | 0   -> Tweet was analyzed
             *  1 | 1   -> This case should not happen (inProgress flag not updated).
             * --------
             */
        } else {
            callback("Not connected", null);
        }
    },

    // Uses findOneAndUpdate to find a tweet that is not processed yet and marks it as inProgress.
    // @param callback fn(err, dbresult)
    findSingleNewTweet: function(callback) {
        if (isConnected()) {
            mongodb.collection('tweets').findOneAndUpdate(
                {inProgress: false, analyzed: false},
                {$set: {inProgress: true}},
                {projection: {
                    id_str: 1,
                    text: 1}
                },
                callback);
        } else {
            callback("Not connected.", 0);
        }
    },

    // @param dbId: the database _id field, can be accessed by res.value['_id']
    // @param words: an array with the cleaned words of the tweet
    // @param sentiment: the sentiment analysis result
    updateSingleTweetWithAnalysis: function(dbId, words, sentiment, callback) {
        mongodb.collection('tweets').updateOne(
            {_id: dbId},
            {$set: {
                inProgress: false,
                analyzed: true,
                words: words,
                sentiment: sentiment
                }
            }, callback);
    },

    /**
     Inserts tweet in buckets of the form:
     {
      _id: ObjectId(...),
      bucket: 1,              // id of the bucket, increasing
      count: 42,              // number of tweets in bucket, increasing
      tweets: [ {             // array if tweets
          id_str: '...',
          text: ''
          created_at: ... },
      ... ]
     }

     * @param tweet
     * @param callback
     */
    insertTweetIntoBucket: function(tweet, bucket, callback) {
        mongodb.collection('tweet_buckets').findOneAndUpdate(
            { 'bucket': bucket },
            {
                '$inc': { 'count': 1 },
                '$push': { 'tweets': {
                    'id_str': tweet['id_str'],
                    'text': tweet['text'],
                    'created_at': tweet['created_at']
                } }
            },
            {
                'upsert': true,
                'returnOriginal': false,
                'projection': {'count': 1}
            },
            callback
        );
    },

    getCurrentTweetBucket: function(callback) {
        mongodb.collection('tweet_buckets')
            .find({},
                {   /* projection */
                    'bucket': 1,
                    'count': 1
                })
            .sort({_id:-1})
            .limit(1)
            .next(callback);
    },
};

module.exports = db;
