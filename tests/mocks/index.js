var options = {
    index: 'elastic-test-2023-01-03',
    type: 'testMapping',
    body: {
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