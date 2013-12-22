/**
 * Test dependencies
 */

var assert = require('assert'),
    Adapter = require('../../'),
    Support = require('../support')(Adapter);

//    Transaction = require('../../lib/transaction.js');

/**
 * Raw waterline-redis `.drop()` tests
 */

describe('adapter `.drop()`', function() {
    
  //_transaction = new Transaction();
    
  before(function(done) {
    var definition = {
      id: {
        type: 'integer',
        primaryKey: true,
        autoIncrement: true
      },
      email: {
        type: 'string',
        unique: true
      }
    };

    Support.Setup('drop', definition, function(err) {
      if(err) throw err;

      Adapter.create('drop', { email: 'jabba@hotmail.com' }, function(err) {
        if(err) throw err;
        done();
      });
    });
  });

  it('should create all index sets', function(done) {

    Adapter.exists('drop', function(err, result) {    
           
        assert(!err);    
        assert(result);
        
        done();
    });    
  });

  it('should drop all index sets', function(done) {
      

    Adapter.drop('drop', function(err, result) {
        
        assert(!err);
        Adapter.exists('drop', function(err, result) {

            assert(!err);    
            assert(result);
            
            done();
        });
    });
  });

});
