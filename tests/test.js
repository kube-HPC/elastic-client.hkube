/*
 * Created by nassi on 11/09/16.
 */

'use strict';

var ElasticClient = require('../lib/ElasticClient.js');
var mockTemplate = require('./mocks/template.js');
var mockIndex = require('./mocks/index.js');
var mockDocument = require('./mocks/document.js');

var client = new ElasticClient({
    node: 'http://localhost:9200',
    //log: ['info', 'trace', 'warning', 'debug'],
    enableLivenessCheck: false,
    livenessCheckInterval: 5000,
    token: 'ssss'
});

client.putTemplate({
    name: mockTemplate.name,
    body: mockTemplate.body
}).then((response) => {
    keepAlive();
});

function keepAlive() {

    let bulk = [];
    for (let i = 0; i < 200; i++) {
        bulk.push({ index: { _index: mockIndex.index, _type: mockIndex.type } });
        bulk.push(mockDocument);
    }
    client.bulk({ body: bulk }).then((response) => {
        setTimeout(() => {
            keepAlive();
        }, 1000)
    }).catch((error) => {
        setTimeout(() => {
            keepAlive();
        }, 1000)
    });
}
