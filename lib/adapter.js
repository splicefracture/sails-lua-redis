/*---------------------------------------------------------------
  :: sails-boilerplate
  -> adapter
---------------------------------------------------------------*/

var Schema = require('./schema'),
    Utils = require('./utils'),
    async = require('async'),
    _ = require('underscore'),
    redis = require('redis'),
    redisClient = redis.createClient(),
    Scripto = require('./scripto'),
    scriptManager = new Scripto(redisClient);

module.exports = (function() {

  var adapter = {

  _schema : Schema,

  syncable: false,

  defaults: {
    migrate: 'alter',
    port: 6379,
    host: 'localhost',
    password: null,
    options: {
      parser: 'hiredis',
      return_buffers: false,
      detect_buffers: false,
      socket_nodelay: true,
      no_ready_check: false,
      enable_offline_queue: true
    }
  },

      register: function(){
	  
      },

  registerCollection: function(collection, callback) {

    var index = [];
    var unique = [];
    var primary = null;
    var attribute = [];
    var auto = [];

    collection.definition.createdAt = { type : 'date'};
    collection.definition.updatedAt = { type : 'date'};
     
    Schema.register(collection.identity,collection.definition);
     
    _.each(collection.definition, function(item, key){
        if (item.type){
            attribute.push(key,item.type);
        }
        if (item.unique){
            unique.push(key);
        }
        if (item.primaryKey && !primary){
            primary = key;
        }
        if (item.index && key != "id"){
            index.push(key);
        }
        if (item.autoIncrement){
            auto.push(key);
        }
    });
    if (!primary){
        primary = id;
    }

    meta = ['index',index,'unique',unique,'primary',primary,'auto',auto];

    var cmd = 'add_schema_meta';
    
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run(cmd, [collection.identity], meta, function(err, result) {});  

    var cmd = 'add_schema_attribute';
    
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run(cmd, [collection.identity], attribute, function(err, result) {});    

    return callback();    
  },

  teardown: function(cb) {
    cb();
  },

  // REQUIRED method if integrating with a schemaful database
  define: function(collectionName, definition, cb) {
    // Define a new "table" or "collection" schema in the data store
    cb();
  },

  describe: function(collection, callback) {
      
    var cmd = 'describe';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run(cmd, [collection], [], function(err, result) {

        var meta = JSON.parse(result[0]);
        var attr = JSON.parse(result[1]);
          
        var desc = {};
        _.each(attr,function(item, key){
            if(!_.has(key,desc)){
                desc[key] = {};
            }
            desc[key].type = item;
        });
            
        desc[meta.primary].primaryKey = true;
 
        if (meta.auto.length > 0){
            _.each(meta.auto.split(","),function(item, key){
                desc[item].autoIncrement = true;
            });
        }

        if (meta.unique.length > 0){
            _.each(meta.unique.split(","),function(item, key){
                desc[item].unique = true;
            });
        }

        if (meta.index.length > 0){
            _.each(meta.index.split(","),function(item, key){
                desc[item].index = true;
            });
        }
        
        //if(!desc) return callback(adapterErrors.collectionNotRegistered);
        callback(null, desc);
    });    
  },

  exists : function (collectionName, cb){
  
    var cmd = 'drop';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run(cmd, [collectionName], [], function(err, result) {
            cb(err,parseInt(result));
        });
    },

  drop: function(collectionName, cb) {

    var cmd = 'drop';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run(cmd, [collectionName], [], function(err, result) {
        cb(err, parseInt(result));
    });

  },

  create: function(collectionName, values, cb) {
    
    var cmd = 'create';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    
    var input = Schema.preParse(collectionName,values)

    scriptManager.run(cmd, [collectionName], input, function(err, result) {
        if (err){ 
            cb(err,null);
        }else{
            var obj = Schema.parse(collectionName,result);
            cb(null, obj);
        }
    });
  },

  find: function(collectionName, options, cb) {

    if (options.where && options.where.like){
        var likePairs = _.pairs(options.where.like)
        delete options.where.like
        _.each(likePairs,function(val, key){
            options.where[val[0]] = {like : val[1]};
        });
    }
    
    if (options.where && options.where.or){
        _.each(options.where.or,function(val, key){
            var v = _.pairs(val).pop();
            if (!options.where[v[0]]){
                options.where[v[0]] = [];
            }
            options.where[v[0]].push(v[1]);
        });
        delete options.where.or
    }
    
    var use_group = false;
    if (options && options.groupBy){
         var use_group = true;
    }

    output = [];

    var cmd = 'find';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    
    scriptManager.run(cmd, [collectionName], options, function(err, result) {
        var values = Schema.parse(collectionName,result);
        // TOOD : move this into a lua script
        output = _.filter(values,function(item){
            return Utils.match(item,options);
        });

        if (options){
            
            if (!use_group){
                var change = _.reduce(Utils.aggregators,function(memo,item,key){
                    if(_.has(options,key)){
                        for(var i = 0; i < options[key].length; i++){
                            memo[options[key][i]] = item(options[key][i],output);
                        }
                    }
                    return memo;            
                },{});
                output = _.map(output,function(item,index,list){
                    return _.extend(item,change);
                });
              
            }else{
                var aggKeys = _.keys(Utils.aggregators);
                var keys = _.pick(options,aggKeys);
                if (_.isEmpty(keys)){
                    cb({"Error":"No calculations given"}, null);
                    return;
                }
                output = Utils.groupBy(options.groupBy,keys,output)
            }
            _.each(Utils.modifiers, function(fn, key){
                if(_.has(options,key) && options[key]){
                    output = fn(options[key],output);
                }
            });
        }        
        cb(null, output);
    });
  },

  update: function(collectionName, options, values, cb) {

     var cmd = 'update';
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');

    var criteria = {};
    if (_.has(options,'where')){
        criteria.where = options.where;
    }else{
        criteria.where = options;
    }
    var output = null
    this.find(collectionName,criteria,function(err, resp){
        if(resp.length>0){
            var output = [];
            async.each(resp,function(item, done){   
                input = _.extend(item, values);
                input = _.flatten(_.pairs(input));
                scriptManager.run(cmd, [collectionName], input, function(err, result) {
                    if (err){
                        done(err);
                    }else{
                        var obj =  Schema.parse(collectionName,result);
                        output.push(obj);
                        done();
                    }
                });
            },function(err){
                if (err){
                    cb(err,null);
                }else{
                    if (output.length == 1){
                        output = output.pop();
                    }
                    cb (null,output);
                }
            });
        }else{
            cb (err,[]);
        }
    });
  },

  destroy: function(collectionName, options, cb) {

    scriptManager.loadFromFile('destroy',__dirname+'/scripts/destroy.lua');    
    scriptManager.loadFromFile('destroyAll',__dirname+'/scripts/destroyAll.lua');

    var criteria = {};
    if (options == null){
        criteria.where = null;
    }else if (_.has(options,'where')){
        criteria.where = options.where;
    }else{
        criteria.where = options;
    }
    
    if (criteria.where == null){
        scriptManager.run("destroyAll", [collectionName], [], function(err, result) {
            cb(err,1)
        });
    }else{
        var output = null
        this.find(collectionName,criteria,function(err, resp){            
            if(resp.length>0){
                var error = []
                error.push(err);
                _.each(resp,function(item,key){
                    scriptManager.run('destroy', [collectionName], _.flatten(_.pairs(item)), function(err, result) {
                        error.push(err);
                    });
                });
                cb ();
            }else{
                cb(err,[])
            }
        });
    }
  },


  stream: function(collectionName, options, stream) {

  }


}

  return adapter;
})();

//////////////                 //////////////////////////////////////////
////////////// Private Methods //////////////////////////////////////////
//////////////                 //////////////////////////////////////////
