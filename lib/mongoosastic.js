var elasticsearch = require('elasticsearch'),
    Generator = require('./mapping-generator'),
    generator = new Generator(),
    serialize = require('./serialize'),
    events = require('events'),
    nop = require('nop'),
    _ = require('lodash'),
    util = require('util');

var GlobalDuringIndexCreationQueue = {};

module.exports = function Mongoosastic(schema, options) {
    options = options || {};

    var bulkTimeout, bulkBuffer = [], esClient,
        mapping = getMapping(schema),
        indexName = options && options.index,
        typeName = options && options.type,
        alwaysHydrate = options && options.hydrate,
        defaultHydrateOptions = options && options.hydrateOptions,
        bulk = options && options.bulk,
        filter = options && options.filter,
        bulkCallbacks = [],
        identifyingPaths = options.identifyingPaths,
        routePath = options.routePath;

    if (options.esClient) {
        esClient = options.esClient;
    } else {
        esClient = createEsClient(options);
    }

    if (identifyingPaths && identifyingPaths.length) {
        identifyingPaths.push('_id');
        identifyingPaths = _.uniq(identifyingPaths);
    } else {
        identifyingPaths = ['_id'];
    }

    if (typeof routePath === 'string') {
        routePath = routePath;
    } else {
        routePath = null;
    }

    setUpMiddlewareHooks(schema);

    /**
     * ElasticSearch Client
     */
    schema.statics.esClient = esClient;
    schema.statics.esIndexName = indexName;
    schema.statics.esTypeName = typeName;

    /**
     * Create the mapping. Takes an optional settings parameter and a callback that will be called once
     * the mapping is created

     * @param settings Object (optional)
     * @param cb Function
     */
    schema.statics.createMapping = function (settings, cb) {
        if (arguments.length < 2) {
            cb = arguments[0] || nop;
            settings = undefined;
        }

        setIndexNameIfUnset(this.modelName);

        createMappingIfNotPresent({
            client: esClient,
            indexName: indexName,
            typeName: typeName,
            schema: schema,
            settings: settings
        }, cb);
    };

    /**
     * Create the EXPLICIT mapping. Takes an optional indexSettings parameter and a callback that will be called once
     * the mapping is created
     * @param explicitMapping Object
     * @param indexSettings Object (optional)
     * @param cb Function
     */
    schema.statics.createExplicitMapping = function (explicitMapping, indexSettings, cb) {
        if (arguments.length < 3) {
            cb = arguments[1] || nop;
            indexSettings = undefined;
        }
        setIndexNameIfUnset(this.modelName);
        createMappingIfNotPresent({
            client: esClient,
            indexName: indexName,
            typeName: typeName,
            explicitMapping: explicitMapping,
            schema: schema,
            settings: indexSettings
        }, cb);
    };

    /**
     * @param options  Object (optional)
     * @param cb Function
     */
    schema.methods.index = function (options, cb) {
        var _cb = null;
        if (arguments.length < 2) {
            _cb = arguments[0];
            options = {};
        } else {
            _cb = cb;
        }

        if (filter && filter(this) && typeof _cb === 'function') {
            return _cb();
        }

        setIndexNameIfUnset(this.constructor.modelName);

        var index = options.index || indexName,
            type = options.type || typeName,
            routeValue = null,
            finalParam = null;

        if (routePath) {
            routeValue = this.get(routePath);
            if (routeValue === undefined || routeValue === null) {
                routeValue = this.get('_id');
            }
            if (typeof routeValue !== 'string') {
                routeValue = routeValue.toString();
            }
        }
        var serialModel = serialize(this, mapping);
        // Remove the markdown syntax
        var _doc = this;
        _.forIn(schema.virtuals, function(value, key) {
            var regex = /^es_(.{0,})/;
            var result = regex.exec(key);
            //console.log(result);
            if(result) {
                serialModel[result[1]] = _doc[key];
            }
        });
        //console.log('modelName:', this.constructor.modelName);
        //console.log('this:', this);
        //console.log('es_description:', this.es_description);
        //console.log('es_comments:', this.es_comments);
        //console.log('mapping:', mapping);
        //console.log('serialModel:', serialModel);
        //console.log('schema:', schema);
        //console.log('virtuals', schema.virtuals.es_description.getters);
        if (bulk) {
            /**
             * To serialize in bulk it needs the _id
             */
            serialModel._id = this._id;
            finalParam = {
                index: index,
                type: type,
                model: serialModel
            };
            if (routeValue) {
                finalParam['routing'] = routeValue;
            }
            bulkIndex(finalParam, _cb);
        } else {
            finalParam = {
                index: index,
                type: type,
                id: this._id.toString(),
                body: {
                    doc: serialModel,
                    upsert: serialModel
                },
                retryOnConflict: 3
            };
            if (routeValue) {
                finalParam['routing'] = routeValue;
            }
            esClient.update(finalParam, _cb);
        }
    };

    /**
     * Unset elasticsearch index
     * @param options - (optional) options for unIndex
     * @param cb - callback when unIndex is complete
     */
    schema.methods.unIndex = function (options, cb) {
        var _cb = null;
        if (arguments.length < 2) {
            _cb = arguments[0] || nop;
            options = {};
        } else {
            _cb = cb;
        }

        setIndexNameIfUnset(this.constructor.modelName);

        options.index = options.index || indexName;
        options.type = options.type || typeName;
        options.model = this;
        options.client = esClient;
        options.tries = 3;

        if (bulk)
            bulkDelete(options, _cb);
        else
            deleteByMongoId(options, _cb);
    };

    /**
     * Delete all documents from a type/index
     * @param options - (optional) specify index/type
     * @param cb - callback when truncation is complete
     */
    schema.statics.esTruncate = function (options, cb) {
        if (arguments.length < 2) {
            cb = arguments[0] || nop;
            options = {};
        }

        setIndexNameIfUnset(this.modelName);

        var index = options.index || indexName,
            type = options.type || typeName;

        esClient.deleteByQuery({
            index: index,
            type: type,
            body: {
                query: {
                    match_all: {}
                }
            }
        }, cb);
    };

    /**
     * Synchronize an existing collection
     *
     * @param query - query for documents you want to synchronize
     */
    schema.statics.synchronize = function (query) {
        var em = new events.EventEmitter(),
            closeValues = [],
            pending = 0,
            close = function () {
                em.emit.apply(em, ['close'].concat(closeValues));
            };

        //Set indexing to be bulk when synchronizing to make synchronizing faster
        bulk = {
                delay: 1000,
                size: 1000
            };

        query = query || {};

        setIndexNameIfUnset(this.modelName);

        var stream = this.find(query, null, {timeout: false, batchSize: bulk.size}).sort('_id').stream();

        stream.on('data', function (doc) {
            pending++;
            if (pending > 5 * bulk.size && !stream.paused) {
                stream.pause();
            } else if (pending <= bulk.size && stream.paused) {
                stream.resume();
            }
            doc.index(function (err, doc) {
                pending--;
                if (err) {
                    em.emit('error', err);
                } else {
                    em.emit('data', null, doc);
                }
            });
        });

        stream.on('close', function (a, b) {
            closeValues = [a, b];
            var closeInterval = setInterval(function () {
                if (pending === 0 && bulkBuffer.length === 0) {
                    clearInterval(closeInterval);
                    close();
                }
            }, 100);
        });

        stream.on('error', function (err) {
            em.emit('error', err);
        });

        return em;
    };
    /**
     * ElasticSearch search function
     *
     * @param query - query object to perform search with
     * @param options - (optional) special search options, such as hydrate
     * @param cb - callback called with search results
     */
    schema.statics.search = function (query, options, cb) {
        var _cb = null;
        if (arguments.length === 2) {
            _cb = arguments[1];
            options = {};
        } else {
            _cb = cb;
        }

        options.hydrateOptions = options.hydrateOptions || defaultHydrateOptions || {};

        if (query === null)
            query = undefined;

        var _this = this,
            esQuery = {
                body: query,
                index: options.index || indexName,
                type: options.type || typeName
            };
        if (options.highlight) {
            esQuery.body.highlight = options.highlight;
            delete options.highlight;
        }

        Object.keys(options).forEach(function (opt) {
            if (!opt.match(/hydrate/) && options.hasOwnProperty(opt))
                esQuery[opt] = options[opt];
        });

        setIndexNameIfUnset(this.modelName);

        esClient.search(esQuery, function (err, res) {
            if (err) {
                return cb(err);
            }

            if (alwaysHydrate || options.hydrate) {
                hydrate(res, _this, options, cb);
            } else {
                cb(null, res);
            }
        });
    };

    schema.statics.esCount = function (query, cb) {
        var _cb = null;
        setIndexNameIfUnset(this.modelName);

        if (cb == null && typeof query === 'function') {
            _cb = query;
            query = null;
        } else {
            _cb = cb;
        }

        var esQuery = {
            body: {
                query: query
            },
            index: options.index || indexName,
            type: options.type || typeName
        };

        esClient.count(esQuery, _cb);
    };

    /**
     * Create the mapping. Takes an optional settings parameter and a callback that will be called once
     * the mapping is created

     * @param settings Object (optional)
     * @param cb Function
     */
    schema.statics.createMapping = function (settings, cb) {
        if (arguments.length < 2) {
            cb = arguments[0] || nop;
            settings = undefined;
        }

        setIndexNameIfUnset(this.modelName);

        createMappingIfNotPresent({
            client: esClient,
            indexName: indexName,
            typeName: typeName,
            schema: schema,
            settings: settings
        }, cb);
    };

    function bulkDelete(options, cb) {
        bulkAdd({
            delete: {
                _index: options.index || indexName,
                _type: options.type || typeName,
                _id: options.model._id.toString()
            }
        },null, cb);
    }

    function bulkIndex(options, cb) {
        var finalParam = {
            update: {
                _index: options.index || indexName,
                _type: options.type || typeName,
                _id: options.model._id.toString(),
                _retry_on_conflict: 3
            }
        };
        if (options.routing) {
            finalParam.update['_routing'] = options.routing;
        }
        bulkAdd(finalParam, {doc:options.model, upsert: options.model}, cb);
    }

    function clearBulkTimeout() {
        clearTimeout(bulkTimeout);
        bulkTimeout = undefined;
    }

    function bulkAdd(instruction, data, cb) {
        bulkBuffer.push(instruction);
        bulkCallbacks.push(cb);
        if (data) {
            bulkBuffer.push(data);
        }

        if (bulkBuffer.length >= (bulk.size || 1000)) {
            schema.statics.flush();
            clearBulkTimeout();
        } else if (bulkTimeout === undefined) {
            bulkTimeout = setTimeout(
            function () {
                schema.statics.flush();
                clearBulkTimeout();
            }, bulk.delay || 1000);
        }
    }

    schema.statics.flush = function (cb) {
        var _cb = cb || nop;

        var thisBulkBuffer = _.clone(bulkBuffer);
        var thisBulkCallbacks = _.clone(bulkCallbacks);

        bulkBuffer.length = 0;
        bulkCallbacks.length = 0;

        esClient.bulk({
            body: thisBulkBuffer
        }, function (err, results) {
            var _err = null;

            if (!err && results && results.items && results.items.length === thisBulkCallbacks.length) {
                for (var i=results.items.length-1; i >=0; i--) {
                    if (typeof thisBulkCallbacks[i] === 'function') {
                        thisBulkCallbacks[i](null, results.items[i]);
                    }

                }
            } else {

                _err = err || new Error('Bulk result count dismatched query count.');
                for (var j=thisBulkCallbacks.length- 1; j>=0; j--) {
                    if (typeof thisBulkCallbacks[j] === 'function') {
                        thisBulkCallbacks[j](_err);
                    }
                }

            }
            thisBulkBuffer.length = 0;
            thisBulkCallbacks.length = 0;
            return _cb(_err, results);

        });

    };

    schema.statics.refresh = function (options, cb) {
        if (arguments.length < 2) {
            cb = arguments[0] || nop;
            options = {};
        }

        setIndexNameIfUnset(this.modelName);
        esClient.indices.refresh({
            index: options.index || indexName
        }, cb);
    };

    function setIndexNameIfUnset(model) {
        var modelName = model.toLowerCase();
        if (!indexName) {
            indexName = modelName + 's';
        }

        if (!typeName) {
            typeName = modelName;
        }
    }

    /**
     * Use standard Mongoose Middleware hooks
     * to persist to Elasticsearch
     */
    function setUpMiddlewareHooks(schema) {
        schema.post('remove', function (doc) {
            setIndexNameIfUnset(doc.constructor.modelName);

            var options = {
                index: indexName,
                type: typeName,
                tries: 3,
                model: doc,
                client: esClient
            };

            if (bulk) {
                bulkDelete(options, nop);
            } else {
                deleteByMongoId(options, nop);
            }
        });

        /**
         * Save in elasticsearch on save.
         */
        var saveHookFunc = function (doc) {
            doc.index(function (err, res) {
                if (!filter || !filter(doc)) {
                    doc.emit('es-indexed', err, res);
                }
            });
        };

        var queryHookFunc = function (result) {
            if ((typeof this._conditions == 'object') && (Object.getOwnPropertyNames(this._conditions).length > 0)) {
                //console.log('this:', this);
                var theModel = this.model;
                var origCondition = this._conditions;
                var finalCondition = null;
                //console.log('origCondition:', origCondition);
                //console.log('identifyingPaths:', identifyingPaths);
                finalCondition = _.foldl(identifyingPaths, function(final, path){
                    var theVal = _.get(origCondition, path);
                    if (theVal) {
                        final[path] = theVal;
                    } else {
                        if(origCondition.$or && origCondition.$or[0]) {
                            theVal = origCondition.$or[0][path];
                            if(theVal) {
                                final[path] = theVal;
                            }
                        }
                    }
                    return final
                }, {});
                if (_.isEmpty(finalCondition)) {
                    finalCondition = origCondition;
                }
                //console.log('finalCondition:', finalCondition);
                this.model.find(finalCondition).limit(50).exec(function (err, results) {
                    //console.log("results:", results, "result:", result);
                    if (err) {
                        return
                    }
                    if (results && results.length > 0) {
                        results.forEach(
                            function(record) {
                                //console.log('record is', record);
                                record.index();
                            }
                        )
                    }
                })
            }
        };

        schema.post('save', saveHookFunc);
        schema.post('update', queryHookFunc);

        // TODO: Implement hook function for findOneAndRemove and findOneAndUpdate;

    }

};

function createEsClient(options) {

    var esOptions = {};

    if (util.isArray(options.hosts)) {
        esOptions.host = options.hosts;
    } else {
        esOptions.host = {
            host: options && options.host ? options.host : 'localhost',
            port: options && options.port ? options.port : 9200,
            protocol: options && options.protocol ? options.protocol : 'http',
            auth: options && options.auth ? options.auth : null,
            keepAlive: false
        };
    }

    esOptions.log = (options ? options.log : null);

    return new elasticsearch.Client(esOptions);
}

function createMappingIfNotPresent(options, cb) {
    var client = options.client,
        indexName = options.indexName,
        typeName = options.typeName,
        schema = options.schema,
        settings = options.settings,
        explicitMapping = options.explicitMapping;

    var doWithMapping = function (mapping) {
        var completeMapping = {};
        completeMapping[typeName] = mapping;
        client.indices.exists({index: indexName}, function (err, exists) {
            if (err) {
                return cb(err);
            }

            if (exists) {
                client.indices.putMapping({
                    index: indexName,
                    type: typeName,
                    body: completeMapping
                }, cb);

            } else {
                if (GlobalDuringIndexCreationQueue[indexName]) {
                    GlobalDuringIndexCreationQueue[indexName].push(
                        function () {
                            client.indices.putMapping({
                                index: indexName,
                                type: typeName,
                                body: completeMapping
                            }, cb);
                        }
                    );
                } else {
                    GlobalDuringIndexCreationQueue[indexName]= [];
                    client.indices.create({index: indexName, body: settings}, function (err) {
                        if (err) {
                            cb(err);
                            if (GlobalDuringIndexCreationQueue[indexName].length > 0) {
                                throw err;
                            }
                            return
                        }
                        GlobalDuringIndexCreationQueue[indexName].forEach(function (job) { job() });
                        delete GlobalDuringIndexCreationQueue[indexName];
                        client.indices.putMapping({
                                index: indexName,
                                type: typeName,
                                body: completeMapping
                        }, cb);
                    });

                }
            }
        });
    };
    if (explicitMapping) {
        return doWithMapping(explicitMapping);
    } else {
        generator.generateMapping(schema, function (err, mapping) {
            doWithMapping(mapping);
        });
    }
}

function hydrate(res, model, options, cb) {
    if (!res || !res.hits || !Array.isArray(res.hits.hits)) {
        return cb(new Error('Search result is invalid.'));
    }
    var results = res.hits,
        resultsMap = {},
        ids = results.hits.map(function (a, i) {
            resultsMap[a._id] = i;
            return a._id;
        }),

        query = model.find({_id: {$in: ids}}),
        hydrateOptions = options.hydrateOptions;

    if (ids.length <= 0) {
        return cb(null, res, [])
    }
    // Build Mongoose query based on hydrate options
    // Example: {lean: true, sort: '-name', select: 'address name'}
    Object.keys(hydrateOptions).forEach(function (option) {
        query[option](hydrateOptions[option]);
    });

    query.exec(function (err, docs) {
        if (err) {
            return cb(err);
        } else {
            // var hits = [];

            docs.forEach(function (doc) {
                var i = resultsMap[doc._id];

                // Original mongoosastic behavior, it breaks result consistency
                // if (options.highlight) {
                //   doc._highlight = results.hits[i].highlight;
                // }
                //
                // hits[i] = doc;

                results.hits[i]['_source'] = doc;
            });

            // results.hits = hits;
            res.hits = results;
            cb(null, res, _.pluck(results.hits, '_source'));
        }
    });
}

function getMapping(schema) {
    var retMapping = {};
    generator.generateMapping(schema, function (err, mapping) {
        retMapping = mapping;
    });

    return retMapping;
}

function deleteByMongoId(options, cb) {
    var index = options.index,
        type = options.type,
        client = options.client,
        model = options.model,
        tries = options.tries;

    client.delete({
        index: index,
        type: type,
        id: model._id.toString()
    }, function (err, res) {
        if (err && err.message.indexOf('404') > -1) {
            setTimeout(function () {
                if (tries <= 0) {
                    return cb(err);
                } else {
                    options.tries = --tries;
                    deleteByMongoId(options, cb);
                }
            }, 500);
        } else {
            model.emit('es-removed', err, res);
            cb(err);
        }
    });
}
