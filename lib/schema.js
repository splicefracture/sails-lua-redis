
var utils = require('./utils'),
    _ = require('underscore');

function Schema() {
  this._schema = {};
  this._blank = {}
}

Schema.prototype = {
    
    register : function(collection, schema) {
        this._schema[collection] = _.clone(schema);
        
        //_.each(_.keys(this._schema[collection]),function(item){
        //    this._blank[item] = null;
        //});

    },
    
    retrieve : function(collection) {
        return this._schema[collection];
    },

    getType : function(collection,key){
        return this._schema[collection][key].type;
    },
    
    preParse : function(collection,values){

        schema = this._schema;
        values = _.map(_.pairs(values),function(val,key){
            var v = val.pop();
            var k = val.pop();
            switch(schema[collection][k].type){
                case 'json':
                    v = JSON.stringify(v);
                    break;
            }
            
            return [k,v];
        });

        return _.flatten(values,true);
    },
    
    parse : function(collection, str) {

        var type, data = JSON.parse(str);
        
        if(!this._schema[collection]) return data;

        var self = this;

        var parseObject = function(data,key){
            
            if (typeof data[key] == "undefined" || data[key] == "null") data[key] = null;
            
            if(!self._schema[collection][key]) return data;

            switch(self._schema[collection][key].type) {
                case 'date':
                case 'time':
                case 'datetime':
                    data[key] = new Date(data[key]);
                    break;
                case 'boolean': 
                    data[key] = data[key] == 'true'  || data[key] === true;
                    break;
                case 'JSON':
                case 'json':
                    data[key] = JSON.parse(data[key]);
                    break;
            }
            return data;
        }


        if (_.isArray(data)){
            for(var i in data) {
                for(var key in data[i]) {
                    data[i] = parseObject(data[i],key)
                }
            }
            data = _.sortBy(data, function(item){ 
                if (item.id){
                    return item.id;
                }else{
                    return 0;
                }
            });
        }else{            
            for(var key in data) {
                data = parseObject(data,key)
            }
        }

        return data;
    }
}

module.exports = new Schema();
