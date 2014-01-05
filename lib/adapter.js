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


var commands = [
    "describe", "exists",   "addMeta",  "addAttribute", 
    "drop",     "create",   "destroy",  "destroyAll",
    "findAll",  "find",     "update"
];

_.each(commands,function(cmd){
    scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');    
});



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

  register: function(cb){
    
    cb();
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
    Schema.setDataStrings(collection.identity,collection.definition)
    
    scriptManager.run("addMeta", [collection.identity], Schema._meta[collection.identity], function(err, result) {
        //console.log(err,result);
    });
    scriptManager.run('addAttribute', [collection.identity], Schema._attr[collection.identity], function(err, result) {
        //console.log(err,result);
    });

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

    scriptManager.run('describe', [collection], [], function(err, result) {

        var meta = JSON.parse(result[0]);
        var attr = JSON.parse(result[1]);
        
        var desc = {};
        _.each(attr,function(item, key){
            desc[key] = {};
            desc[key].type = item;
        });

        var meta_set = _.map(meta,function(item, key){

            if (key == "primary"){ 
                desc[item].primaryKey = true;
            }
            if (key == "auto"){ 
                _.each(JSON.parse(item),function(val){
                    desc[val].autoIncrement = true;
                });
            }
            if (key == "index"){ 
                _.each(JSON.parse(item),function(val){
                    desc[val].index = true;
                });
            }
            if (key == "unique"){ 
                _.each(JSON.parse(item),function(val){
                    desc[val].unique = true;
                });
            }
        });         

        //if(!desc) return callback(adapterErrors.collectionNotRegistered);
        callback(null, desc);
    });    
  },

  exists : function (collectionName, cb){
  
    var cmd = 'exists';
    //scriptManager.loadFromFile(cmd,__dirname+'/scripts/'+cmd+'.lua');
    scriptManager.run('exists', [collectionName], [], function(err, result) {
            cb(err,parseInt(result));
        });
    },

  drop: function(collectionName, cb) {
    scriptManager.run('exists', [collectionName], [], function(err, result) {
        cb(err, parseInt(result));
    });
  },

  create: function(collectionName, values, cb) {
    
    var input = Schema.preParse(collectionName,values)
    var key_obj = Schema.getKeyObject(collectionName)
    
    scriptManager.run('create', key_obj, input, function(err, result) { 
        
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

    //var criteria = {}

    output = [];

    var key_obj = Schema.getKeyObject(collectionName)    
    var criteria = Schema.findCriteria(collectionName,options);

    scriptManager.run('find', key_obj, criteria, function(err, result) {

        var values = Schema.parse(collectionName,result);
        
        // TOOD : move *some* of this into a lua script
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

  update: function(collectionName, criteria, values, cb) {

    var key_obj = Schema.getKeyObject(collectionName)
    
    var output = null
    this.find(collectionName,criteria,function(err, resp){
        if(resp.length>0){
            var output = [];
            async.each(resp,function(item, done){   
                input = _.extend(item, values);

                scriptManager.run('update', key_obj, JSON.stringify(input), function(err, result) {
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

  destroy: function(collectionName, criteria, cb) {
        
    var key_obj = Schema.getKeyObject(collectionName)

    if (criteria == null){
        scriptManager.run("destroyAll", key_obj, [], function(err, result) {
            cb(err,1)
        });
    }else{
        var output = null

        this.find(collectionName,criteria,function(err, resp){

            if(resp.length>0){
                var error = []
                error.push(err);
                _.each(resp,function(item,key){
                    scriptManager.run('destroy', key_obj, JSON.stringify(item), function(err, result) {
                        error.push(err);
                    });
                });
                cb (null,[]);
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
