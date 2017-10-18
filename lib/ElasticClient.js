/*
 * Created by nassi on 26/06/16.
 */

'use strict';

var elasticsearch = require('elasticsearch');
var lodash = require('lodash');
var EventEmitter = require('events');
const DEFAULT_PING_INTERVAL = 5000;

var DEFAULTS = {
    apiVersion: "5.0",
    sniffOnStart: false,            // Should the client attempt to detect the rest of the cluster when it is first instantiated
    sniffOnConnectionFault: false,  // Should the client immediately sniff for a more current list of nodes when a connection dies
    connectionClass: "http",        // http, browser.
    pingTimeout: 3000,              // pingTimeout
    requestTimeout: 30000,          // Milliseconds before an HTTP request will be aborted and retried. This can also be set per request
    maxRetries: 3,                  // How many times should the client try to connect to other nodes before returning a ConnectionFault error
    keepAlive: true,                // Should the connections to the node be kept open forever
    minSockets: 10,                 // Minimum number of sockets to keep connected to a node, only applies when keepAlive is true
    maxSockets: 11,                 // Maximum number of concurrent requests that can be made to any node.
    log: null                       // ['info', 'trace', 'warning', 'debug']
};

class ElasticClient extends EventEmitter {

    constructor(options) {
        super();

        options = options || {};

        if (!options.host && !options.hosts) {
            throw new Error('Must specify hosts');
        }
        this.isActive = true;
        this.isInit = true;
        this.livenessCheckInterval = Math.max(options.livenessCheckInterval, DEFAULT_PING_INTERVAL);
        this.options = lodash.defaults(options, DEFAULTS);
        this.client = new elasticsearch.Client(this.options);

        if (options.enableLivenessCheck) {
            this._livenessCheckInterval();
        }
        this._livenessCheck();
    }

    /**
     * create a ping request
     * @returns {Promise}
     */
    ping() {
        return new Promise((resolve, reject) => {
            this._ping().then((response) => {
                this._resolvePing();
                return resolve(response);
            }).catch((error) => {
                this._resolvePing(error);
                return reject(error);
            });
        });
    }

    /**
     * Create an index in Elasticsearch
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.body
     * @returns {Promise}
     */
    createIndex(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.create({
                index: options.index,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then(function (response) {
                return resolve({exists: false, response: response});
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Return a boolean indicating whether given index exists
     * @param {Object} options
     * @param {string} options.index
     * @returns {Promise}
     */
    isIndexExists(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.exists({
                index: options.index,
                requestTimeout: options.requestTimeout
            }).then((exists) => {
                return resolve(exists);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Return index
     * @param {Object} options
     * @param {string} options.index
     * @returns {Promise}
     */
    getIndex(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.get({
                index: options.index,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Delete an index in Elasticsearch
     * @param {Object} options
     * @param {string} options.index
     * @returns {Promise}
     */
    deleteIndex(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.delete({
                index: options.index,
                requestTimeout: options.requestTimeout
            }).then(function (response) {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Update specified aliases
     * @param {Object} options
     * @param {string} options.body
     * @returns {Promise}
     */
    updateAliases(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.updateAliases({
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Delete a specific alias
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.name
     * @returns {Promise}
     */
    deleteAlias(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.deleteAlias({
                index: options.index,
                name: options.name,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Register specific mapping definition for a specific type
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.body
     * @returns {Promise}
     */
    putMapping(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.putMapping({
                index: options.index,
                type: options.type,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Create an index template that will automatically be applied to new indices created
     * @param {Object} options
     * @param {bool} options.create
     * @param {string} options.name
     * @param {string} options.body
     * @returns {Promise}
     */
    putTemplate(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.putTemplate({
                create: options.create || false,
                name: options.name,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Delete an index template by its name
     * @param {Object} options
     * @param {string} options.name
     * @returns {Promise}
     */
    deleteTemplate(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.deleteTemplate({
                name: options.name,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Retrieve an index template by its name
     * @param {Object} options
     * @param {string} options.name
     * @returns {Promise}
     */
    getTemplate(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.getTemplate({
                name: options.name,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Check existence of index template by its name
     * @param {Object} options
     * @param {string} options.name
     * @returns {Promise}
     */
    existsTemplate(options) {
        return new Promise((resolve, reject) => {
            this.client.indices.existsTemplate({
                name: options.name,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Close the connection
     */
    close() {
        this.client.close();
    }

    /**
     * Return documents matching a query
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.body
     * @returns {Promise}
     */
    search(options) {

        return new Promise((resolve, reject) => {
            this.client.search({
                index: options.index,
                type: options.type,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(this._formatResponse(response));
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Execute several search requests within the same request.
     * Perform multiple different searches, the body is made up of meta/data pairs.
     * @param {Object} options
     * @param {string} options.body
     * @returns {Promise}
     */
    msearch(options) {

        return new Promise((resolve, reject) => {
            this.client.msearch({
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(this._mapMultiSearch(response.responses));
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Get the number of documents for the cluster, index, type, or a query
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.body
     * @returns {Promise}
     */
    count(options) {
        return new Promise((resolve, reject) => {
            this.client.count({
                index: options.index,
                type: options.type,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(this._formatResponse(response));
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Get a typed JSON document from the index based on its id
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.id
     * @param {string} options.fields
     * @param {bool} options.refresh - Refresh the shard containing the document before performing the operation
     * @param {string} options.routing - Specific routing value
     * @param {string} options.source - True or false to return the _source field or not, or a list of fields to return
     * @returns {Promise}
     */
    getByID(options) {
        return new Promise((resolve, reject) => {
            this.client.get({
                index: options.index,
                type: options.type,
                id: options.id,
                fields: options.fields,
                refresh: options.refresh,
                routing: options.routing,
                _source: options.source,
                requestTimeout: options.requestTimeout
            }).then((response) => {
                return resolve(response._source);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Stores a typed JSON document in an index, making it searchable. When the id param is
     * not set, a unique id will be auto-generated. When you specify an id either a new
     * document will be created, or an existing document will be updated.
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.id
     * @param {string} options.body
     * @returns {Promise}
     */
    index(options) {
        return new Promise((resolve, reject) => {
            this.client.index({
                index: options.index,
                type: options.type,
                id: options.id,
                body: options.body,
                requestTimeout: options.requestTimeout
            }).then(function (response) {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Perform many index/delete operations in a single API call
     * @param {Object} options
     * @param {string} options.body
     * @param {string} options.fields
     * @param {bool} options.refresh - Refresh the shard containing the document before performing the operation
     * @param {string} options.routing - Specific routing value
     * @returns {Promise}
     */
    bulk(options) {
        return new Promise((resolve, reject) => {
            this.client.bulk({
                body: options.body,
                fields: options.fields,
                refresh: options.refresh,
                routing: options.routing,
                requestTimeout: options.requestTimeout
            }).then(function (response) {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    /**
     * Delete a typed JSON document from a specific index based on its id
     * @param {Object} options
     * @param {string} options.index
     * @param {string} options.type
     * @param {string} options.id
     * @returns {Promise}
     */
    deleteByID(options) {
        return new Promise((resolve, reject) => {
            this.client.delete({
                index: options.index,
                type: options.type,
                id: options.id,
                requestTimeout: options.requestTimeout
            }).then(function (response) {
                return resolve(response);
            }, function (error) {
                return reject(error);
            });
        });
    }

    _formatResponse(response) {
        return {
            aggregations: response.aggregations,
            total: response.hits.total,
            took: response.took,
            hits: this._mapAllHits(response.hits.hits)
        }
    }

    _mapMultiSearch(responses) {
        if (!responses) {
            return responses;
        }

        let index = 0;
        let sources = [];
        for (let r of responses) {
            if (r.error) {
                sources.push({error: r.error.type, index: index});
            }
            else if (r.aggregations) {
                sources.push(r.aggregations);
            }
            else if (r.hits && r.hits.hits.length > 0) {
                sources.push(r.hits.hits[0]._source);
            }
            else {
                sources.push({error: new Error('empty response'), index: index});
            }
            index++;
        }
        return sources
    }

    _mapAllHits(hits) {
        if (!hits) {
            return hits;
        }
        return hits.map(function (hit) {
            return hit._source;
        });
    }

    _ping() {
        return this.client.ping();
    }

    _livenessCheck() {
        this._ping().then(() => {
            this._resolvePing();
        }).catch((error) => {
            this._resolvePing(error);
        });
    }

    _resolvePing(error) {
        if (error) {
            if (this.isActive) {
                this.isActive = false;
                this.emit('down', error);
            }
        }
        else {
            if (!this.isActive || this.isInit) {
                this.isInit = false;
                this.isActive = true;
                this.emit('ready', this.client);
            }
        }
    }

    _livenessCheckInterval() {
        setInterval(() => {
            this._livenessCheck();
        }, this.livenessCheckInterval);
    }
}

module.exports = ElasticClient;
