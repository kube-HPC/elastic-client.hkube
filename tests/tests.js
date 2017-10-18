/*
 * Created by nassi on 28/08/16.
 */

'use strict';

var chai = require("chai");
var expect = chai.expect;
var sinon = require('sinon');
var mockery = require('mockery');
var ElasticClient = require('../lib/ElasticClient.js');
var mockTemplate = require('./mocks/template.js');
var mockIndex = require('./mocks/index.js');
var mockDocument = require('./mocks/document.js');
var client;

describe('ElasticClient', function () {

    before(function (done) {

        const url = 'http://localhost:9200';
        client = new ElasticClient({
            host: url,
            enableLivenessCheck: true,
            livenessCheckInterval: -1
        });
        client.deleteIndex({index: 'elastic-test-*'}).then((response) => {
            done();
        });
    });
    after(function (done) {
        done();
    });

    describe('Actions', function () {
        xdescribe('ready', function () {
            it('should emit ready event', function (done) {
                client.on('ready', () => {
                    done();
                });
                client.ping();
            });
        });
        xdescribe('down', function () {
            it('should success to create job', function (done) {
                client.on('down', () => {
                    done();
                });
                done();
                //client.close();
                //client.ping();
            });
        });
        describe('ping', function () {
            it('should success to create job', function (done) {
                client.ping().then((response) => {
                    expect(response).to.equal(true);
                    done();
                }).catch((error) => {
                    done();
                });
            });
        });
        describe('createIndex', function () {
            it('should create an index in elasticsearch', function (done) {
                client.createIndex({
                    index: mockIndex.index,
                    body: mockIndex.body
                }).then((data) => {
                    expect(data.response.acknowledged).to.equal(true);
                    done();
                });
            });
        });
        describe('isIndexExists', function () {
            it('should indicating whether given index exists', function (done) {
                client.isIndexExists({index: mockIndex.index}).then((response) => {
                    expect(response).to.equal(true);
                    done();
                });
            });
        });
        describe('getIndex', function () {
            it('should success to create job', function (done) {
                client.getIndex({index: mockIndex.index}).then((response) => {
                    expect(Object.keys(response)[0]).to.equal(mockIndex.index);
                    done();
                });
            });
        });
        describe('deleteIndex', function () {
            it('should success to create job', function (done) {
                client.deleteIndex({index: mockIndex.index}).then((response) => {
                    expect(response.acknowledged).to.equal(true);
                    done();
                });
            });
        });
        xdescribe('updateAliases', function () {
            it('should success to create job', function (done) {
                client.updateAliases().then((response) => {
                    done();
                });
            });
        });
        describe('putMapping', function () {
            it('should success to create job', function (done) {
                client.createIndex({
                    index: mockIndex.index,
                    body: mockIndex.body
                }).then((data) => {
                    client.putMapping({
                        index: mockIndex.index,
                        type: mockIndex.type,
                        body: mockIndex.body.mappings
                    }).then((response) => {
                        expect(response.acknowledged).to.equal(true);
                        done();
                    });
                });
            });
        });
        describe('putTemplate', function () {
            it('should success to create job', function (done) {
                client.putTemplate({
                    name: mockTemplate.name,
                    body: mockTemplate.body
                }).then((response) => {
                    expect(response.acknowledged).to.equal(true);
                    done();
                });
            });
        });
        describe('deleteTemplate', function () {
            it('should success to create job', function (done) {
                client.putTemplate({
                    name: mockTemplate.name,
                    body: mockTemplate.body
                }).then((response) => {
                    client.deleteTemplate({name: mockTemplate.name}).then((response) => {
                        expect(response.acknowledged).to.equal(true);
                        done();
                    }).catch((error) => {
                        done();
                    });
                });
            });
        });
        describe('getTemplate', function () {
            it('should retrieve an index template by its name', function (done) {
                client.putTemplate({
                    name: mockTemplate.name,
                    body: mockTemplate.body
                }).then((response) => {
                    client.getTemplate({name: mockTemplate.name}).then((response) => {
                        //to.deep.equal
                        expect(Object.keys(response)[0]).to.equal(mockTemplate.name);
                        done();
                    });
                });
            });
        });
        describe('existsTemplate', function () {
            it('should success to create job', function (done) {
                client.putTemplate({
                    name: mockTemplate.name,
                    body: mockTemplate.body
                }).then((response) => {
                    client.existsTemplate({name: mockTemplate.name}).then((response) => {
                        expect(response).to.equal(true);
                        done();
                    });
                });
            });
        });
        describe('index', function () {
            it('should stores a document in an index', function (done) {
                let options = {
                    index: mockIndex.index,
                    type: mockIndex.type,
                    body: mockDocument
                };
                client.index(options).then((response) => {
                    expect(response.created).to.equal(true);
                    done();
                });
            });
        });
        describe('bulk', function () {
            it('should success to create job', function (done) {
                let bulk = [];
                for (let i = 0; i < 20; i++) {
                    bulk.push({index: {_index: mockIndex.index, _type: mockIndex.type}});
                    bulk.push(mockDocument);
                }
                client.bulk({body: bulk}).then((response) => {
                    expect(response.items.length).to.equal(bulk.length / 2);
                    done();
                });
            });
        });
        describe('search', function () {
            it('should return documents matching a query', function (done) {
                this.timeout(5000);
                let options = {
                    index: mockIndex.index,
                    type: mockIndex.type,
                    body: mockDocument
                };
                client.index(options).then((response) => {
                    let body = {
                        size: 1,
                        query: {
                            bool: {filter: [{term: {recordID: mockDocument.recordID}}]}
                        }
                    };
                    setTimeout(() => {
                        client.search({index: mockIndex.index, type: mockIndex.type, body: body}).then((response) => {
                            expect(response.hits[0].recordID).to.equal(mockDocument.recordID);
                            done();
                        });
                    }, 2000);
                });
            });
        });
        describe('msearch', function () {
            it('should execute several search requests within the same request', function (done) {
                this.timeout(5000);
                let body = [];
                let q = {
                    size: 1,
                    query: {
                        bool: {filter: [{term: {recordID: mockDocument.recordID}}]}
                    }
                };
                body.push({index: mockIndex.index, type: mockIndex.type});
                body.push(q);
                setTimeout(() => {
                    client.msearch({body: body}).then((response) => {
                        expect(response[0].recordID).to.equal(mockDocument.recordID);
                        done();
                    });
                }, 2000);
            });
        });
        describe('getByID', function () {
            it('should get a document from the index based on its id', function (done) {
                client.index({index: mockIndex.index, type: mockIndex.type, body: mockDocument}).then((response) => {
                    client.getByID({index: mockIndex.index, type: mockIndex.type, id: response._id}).then((response) => {
                        expect(response.recordID).to.equal(mockDocument.recordID);
                        done();
                    });
                });

            });
        });
        describe('deleteByID', function () {
            it('should delete a document from a specific index based on its id', function (done) {
                client.index({index: mockIndex.index, type: mockIndex.type, body: mockDocument}).then((indexResponse) => {
                    client.deleteByID({index: mockIndex.index, type: mockIndex.type, id: indexResponse._id}).then((deleteResponse) => {
                        expect(indexResponse._id).to.equal(deleteResponse._id);
                        done();
                    });
                });
            });
        });

    });
});

