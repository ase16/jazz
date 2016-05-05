'use strict';
const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

const db = require('./dbModule.js');
const tweetfetcher = require('./tweetfetcher.js');

log.info('Tweetfetcher initializing...');
db.connect(function() {
    tweetfetcher.init(db, function(err, res) {
        log.info('Tweetfetcher is ready.');
    });
});