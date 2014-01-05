/**
 * Test dependencies
 */

var assert = require('assert'),
    Adapter = require('../../'),
    async = require('async'),
    _ = require('underscore'),
    Support = require('../support')(Adapter);


/**
 * Raw waterline-redis `.find()` tests
 */

describe('adapter `.index()`', function() {
  before(function(done) {
    var definition = {
      id: {
        type: 'integer',
        primaryKey: true,
        autoIncrement: true
      },
      name: {
        type: 'string'
      },
      age: {
        type: 'integer',
        index : true
      }
    };

    Support.Setup('indexers', definition, function(err) {
        
        async.each(_.range(20,45),function(item, cb){
            Adapter.create('indexers', { name: 'Dude age '+item, age: item }, cb);
        },done);
    });
    
  });

  after(function(done) {
    Support.Teardown('indexers', done);
  });

  describe('simple find', function() {

    it("should find using age range greater than or equal to", function(done) {
      var criteria = { where: { age : {'>=': 40} } };

      Adapter.find('indexers', criteria, function(err, records) {
        if(err) throw err;
        assert(records);
        assert(records.length === 5);
        done();
      });
      
    });
    
    it("should find using age range less than or equal to", function(done) {
      var criteria = { where: { age : {'<=': 24} } };

      Adapter.find('indexers', criteria, function(err, records) {

        if(err) throw err;
        assert(records);
        assert(records.length === 5);
        done();
      });
      
    });
    
    it("should find using age range within constraint", function(done) {
      var criteria = { where: { age : {'>': 25, "<=": 30} } };

      Adapter.find('indexers', criteria, function(err, records) {
        if(err) throw err;
        assert(records);
        assert(records.length === 5);
        done();
      });
      
    });
    
  });
  
});
