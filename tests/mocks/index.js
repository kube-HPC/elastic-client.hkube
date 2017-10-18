var options = {
    index: 'elastic-test-2017-01-09',
    type: 'testMapping',
    body: {
        "mappings": {
            "testMapping": {
                "properties": {
                    "recordID": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "timestamp": {
                        "type": "date"
                    },
                    "location": {
                        "type": "geo_shape",
                        "tree": "quadtree"
                    },
                    "centroid": {
                        "type": "geo_point"
                    }
                }
            }
        }
    }
};

module.exports = options;