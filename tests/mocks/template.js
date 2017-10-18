
var options = {
    name: 'elastic-test-template',
    body: {
        "template": "elastic-test-*",
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