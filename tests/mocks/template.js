
var options = {
    name: 'elastic-test-template',
    body: {
        "index_patterns": "elastic-test-*",
        "mappings": {
            "testMapping": {
                "properties": {
                    "recordID": {
                        "type": "string"
                    },
                    "timestamp": {
                        "type": "date"
                    }
                }
            }
        }
    }
};

module.exports = options;