'use strict';

var log = require('winston');
log.level = require('config').get('log.level');
var async = require('async');

// Google API
// API Explorer: https://developers.google.com/apis-explorer/
// nodejs client: https://github.com/google/google-api-nodejs-client/
// REST / Parameters and responses: https://cloud.google.com/compute/docs/reference/latest/
var google = require('googleapis');

var WORKER_INSTANCE_GROUP = "will-nodes";
var WORKER_INSTANCE_TEMPLATE = "will-template-2";

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
 * Get the configuration
 * @returns {*}
 */
Cloud.prototype.getConfig = function () {
    return this.config;
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
 * List all instances (in the default zone).
 * @param callback
 */
Cloud.prototype.listInstances = function(callback) {
    var self = this;
    this.compute.instances.list({
        zone: self.config.zone
    }, callback);
};

/**
 * Initializes the instance templates: create templates if not exist yet.
 */
Cloud.prototype.initInstanceTemplates = function(callback) {
    var self = this;
    this.compute.instanceTemplates.list({
        filter: "name eq " + WORKER_INSTANCE_TEMPLATE
    }, function(err, res) {
        if (err) {
            return callback(err, res);
        }

        if(typeof res.items != 'undefined' && res.items instanceof Array && res.items.length > 0) {
            log.info("Instance template '%s' does already exist.", WORKER_INSTANCE_TEMPLATE);
            callback(null);
        } else {
            log.info("Instance template '%s' does not exist yet. Create template...", WORKER_INSTANCE_TEMPLATE);
            self._createWorkerTemplate(callback);
        }
    });
};

/**
 * Creates the worker instance template.
 * @param callback
 * @private
 */
Cloud.prototype._createWorkerTemplate = function(callback) {
    var self = this;
    var workerTemplate = {
        "name": WORKER_INSTANCE_TEMPLATE,
        "description": "instance template for a worker node",
        "properties": {
            /* 1st number is the core, 2nd number is memory */
            "machineType": "custom-1-2048",
            "metadata": {
                "items": [
                    {
                        /* startup script is fetched from github and executed (additional scripts are cloned from the repository) */
                        "key": "startup-script",
                        "value": "curl https://raw.githubusercontent.com/ase16/setup-scripts/master/gcloud-startup-script.sh | bash -"
                    }
                ]
            },
            "tags": {
                "items": []
            },
            "disks": [
                {
                    "type": "PERSISTENT",
                    "boot": true,
                    "mode": "READ_WRITE",
                    "autoDelete": true,
                    "deviceName": "worker",
                    "initializeParams": {
                        "sourceImage": "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-8-jessie-v20160329",
                        "diskType": "pd-ssd",
                        "diskSizeGb": "10"
                    }
                }
            ],
            "canIpForward": false,
            "networkInterfaces": [
                {
                    "network": "projects/ase16-1255/global/networks/default",
                    "accessConfigs": [
                        {
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT"
                        }
                    ]
                }
            ],
            "scheduling": {
                "preemptible": false,
                "onHostMaintenance": "MIGRATE",
                "automaticRestart": true
            },
            "serviceAccounts": [
                {
                    "email": "default",
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.read_only",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring.write",
                        "https://www.googleapis.com/auth/cloud.useraccounts.readonly"
                    ]
                }
            ]
        }
    };
    var params = {
        resource: workerTemplate
    };
    this.compute.instanceTemplates.insert(params, function(err, res) {
        log.debug("Cloud.initInstanceTemplates: ", err, res);
        callback(err, res);
    });
};

/**
 * Initializes the instance groups: create groups if not exist yet.
 */
Cloud.prototype.initInstanceGroups = function(callback) {
    var self = this;
    this.compute.instanceGroupManagers.list({
        zone: self.config.zone,
        filter: "name eq " + WORKER_INSTANCE_GROUP
    }, function(err, res) {
        if (err) {
            return callback(err, res);
        }

        if(typeof res.items != 'undefined' && res.items instanceof Array && res.items.length > 0) {
            log.info("Instance group '%s' does already exist.", WORKER_INSTANCE_GROUP);
            callback(null);
        } else {
            log.info("Instance group '%s' does not exist yet. Create group...", WORKER_INSTANCE_GROUP);
            // Defer createWorkerGroup call some time because template may still have the status "pending" in the gcloud.
            setTimeout(self._createWorkerGroup.bind(self), 5*1000, callback);
        }
    });
};

/**
 * Creates the worker instance group.
 * @param callback
 * @private
 */
Cloud.prototype._createWorkerGroup = function(callback) {
    var self = this;
    var workerGroup = {
        "name": WORKER_INSTANCE_GROUP,
        "description": "This group contains all worker nodes.",
        "instanceTemplate": "projects/"+self.config.projectId+"/global/instanceTemplates/"+WORKER_INSTANCE_TEMPLATE,
        "baseInstanceName": WORKER_INSTANCE_GROUP,
        "targetSize": "0",
        "autoHealingPolicies": [
            {
                "initialDelaySec": 300
            }
        ]
    };
    var params = {
        zone: self.config.zone,
        resource: workerGroup
    };

    this.compute.instanceGroupManagers.insert(params, function(err, res) {
        log.debug("Cloud.initInstanceGroups: ", err, res);
        callback(err, res);
    });
};

/**
 * Lists the instances in the worker instance group.
 * @param callback
 */
Cloud.prototype.listWorkerInstances = function(callback) {
    var self = this;
    var params = {
        zone: self.config.zone,
        instanceGroupManager: WORKER_INSTANCE_GROUP
    };

    this.compute.instanceGroupManagers.listManagedInstances(params, function(err, res) {
        log.debug("Cloud.listWorkerInstances: ", err, res);
        if (!err) {
            if ( typeof res.managedInstances != 'undefined' && res.managedInstances instanceof Array ) {
                // last segment of the instance URL is the instance name.
                // We add it here such that it can be used in getInstance calls
                res.managedInstances.forEach(function(instance) {
                    instance['name'] = instance.instance.split('/').pop();
                });
            }
        }
        callback(err, res);
    });
};

/**
 * Get instance information.
 * @param name of the instance
 * @param callback
 */
Cloud.prototype.getInstance = function(name, callback) {
    var self = this;
    var params = {
        zone: self.config.zone,
        instance: name
    };
    this.compute.instances.get(params, function(err, res) {
        log.debug("Cloud.getInstance: ", err, res);
        callback(err, res);
    });
};

/**
 * Resize the worker instance group, i.e. set number of instances in the group.
 * @param newSize
 * @param callback
 */
Cloud.prototype.resizeWorkerGroup = function(newSize, callback) {
    var self = this;
    var params = {
        zone: self.config.zone,
        instanceGroupManager: WORKER_INSTANCE_GROUP,
        size: newSize
    };
    this.compute.instanceGroupManagers.resize(params, function(err, res) {      // instanceGroupManagers.resize --> https://cloud.google.com/compute/docs/reference/latest/instanceGroupManagers/resize
        log.debug("Cloud.resizeWorkerGroup: ", err, res);
        callback(err, res);
    });
};

module.exports = Cloud;