'use strict';

var log = require('winston');
log.level = require('config').get('log.level');
var async = require('async');

// Google API
// API Explorer: https://developers.google.com/apis-explorer/
// nodejs client: https://github.com/google/google-api-nodejs-client/
// REST / Parameters and responses: https://cloud.google.com/compute/docs/reference/latest/
var google = require('googleapis');

var Cloud = function (config, callback) {
    if (!(this instanceof Cloud)) {
        return new Cloud(config, callback);
    }
    this.config = config;
    // authenticate and configure compute engine API endpoint
    async.series([
        this._auth.bind(this),
        this._initCompute.bind(this)
    ], function(err, res) {
        log.debug("Cloud init completed.");
        callback(err);
    });
};

/**
 * API authentication. It uses the "application default" method as described
 * here: https://developers.google.com/identity/protocols/application-default-credentials#whentouse
 * In the google cloud: authentication is automatically done. Outside of the gcloud, the credentials
 * are read from the key file set by the environment variable GOOGLE_APPLICATION_CREDENTIALS.
 *
 * @param callback
 * @private
 */
Cloud.prototype._auth = function(callback) {
    var self = this;
    google.auth.getApplicationDefault(function(err, authClient) {

        if (err) {
            log.debug("Cloud._auth: error. ", err);
            return callback(err, authClient);
        }

        // The createScopedRequired method returns true when running on GAE or a local developer
        // machine. In that case, the desired scopes must be passed in manually. When the code is
        // running in GCE or a Managed VM, the scopes are pulled from the GCE metadata server.
        // See https://cloud.google.com/compute/docs/authentication for more information.
        if (authClient.createScopedRequired && authClient.createScopedRequired()) {
            // Scopes can be specified either as an array or as a single, space-delimited string.
            authClient = authClient.createScoped(['https://www.googleapis.com/auth/compute']);
        }
        self.authClient = authClient;
        log.debug("Cloud._auth: succeeded.");
        return callback(null, authClient);
    });
};

/**
 * Setup Compute Engine endpoint (select API Version, authentication)
 *
 * @param callback
 * @private
 */
Cloud.prototype._initCompute = function(callback) {
    var self = this;
    this.compute = google.compute({
        version: 'v1',
        auth: self.authClient,
        params: {
            // use of a fixed project. It would also be possible to add this parameter to each request.
            project: self.config.projectId
        }
    });
    log.debug("Cloud._initCompute succeeded.");
    callback(null);
};

/**
 * Lists the instances of the given instance group from a certain zone.
 * @param zone
 * @param instanceGroup
 * @param callback
 */
Cloud.prototype.listVMsOfInstanceGroup = function(zone, instanceGroup, callback) {
    var params = {
        zone: zone,
        instanceGroupManager: instanceGroup
    };

    this.compute.instanceGroupManagers.listManagedInstances(params, function(err, res) {
        log.debug("Cloud.listVMsOfInstanceGroup: ", err, res);
        if (!err) {
            if ( typeof res.managedInstances != 'undefined' && res.managedInstances instanceof Array ) {
                // Last segment of the instance URL is the instance name. We add it here such that it can be used in getInstance calls
                res.managedInstances.forEach(function(instance) {
                    instance['name'] = instance.instance.split('/').pop();
                });
            }
        }
        callback(err, res);
    });
};

module.exports = Cloud;