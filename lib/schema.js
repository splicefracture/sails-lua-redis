
var utils = require('./utils'),
    _ = require('underscore');

function Schema() {
  this._schema = {}
  
  this._meta = {}
  this._attr = {}

}

Schema.prototype = {
    
    register : function(collection, schema) {
        this._schema[collection] = _.clone(schema);
    },
    
    retrieve : function(collection) {
        return this._schema[collection];
    },

    getType : function(collection,key){
        return this._schema[collection][key].type;
    },
 
    findCriteria : function(collection,criteria){
        
        var min = -1 * Math.pow(2,64)
        var max = Math.pow(2,64)
        
        var index = [];
        var primary = null;
        _.each(this._schema[collection], function(item, key){
            if (item.primaryKey && !primary){
                primary = key;
            }
            if (item.index && key != "id"){
                index.push(key);
            }
        });

        if (criteria && criteria.where){
            var ids = [];
            if (criteria.where[primary]){
                var row = {}
                row[primary] = criteria.where[primary]
                ids.push(row)
            }
            var minmax = {}
            var indicies = _.intersection(_.keys(criteria.where), index);
            _.each(indicies,function(idx){
                
                minmax[idx] = {};

                _.each(criteria[idx],function(val,key){
                    
                    if (key == "greaterThan" || key == "greaterThanOrEqual" || key == ">" || key == ">="){
                        
                        if (minmax[idx].min){
                            minmax[idx].min = Math.min(minmax[idx].min,val)
                        }else{
                            minmax[idx].min = val
                        }
                    }else if (key == "lessThan" || key == "lessThanOrEqual" || key == "<" || key == "<="){
                        if (minmax[idx].max){
                            minmax[idx].max = Math.max(minmax[idx].max,val)
                        }else{
                            minmax[idx].max = val
                        }
                    }else{
                        var row = {}
                        row[idx] = val
                        ids.push(row)
                    }
                })
                
                if (_.isEmpty(minmax[idx])){
                    delete minmax[idx];
                }else if (minmax[idx].min && !minmax[idx].max){
                    minmax[idx].max = max;
                }else if (minmax[idx].max && !minmax[idx].min){
                    minmax[idx].min = min;
                }
            });
            
            if (!ids.length){ids.push({})};
            
            var id_str = JSON.stringify(ids)
            id_str = id_str=="[{}]" ? "{}": id_str;
            
            var minmax_str = JSON.stringify([minmax])
            minmax_str = minmax_str=="[{}]" ? "{}": minmax_str;
            
            return [id_str,minmax_str];
        }
        return ["{}","{}"];
    },
    
    setDataStrings : function(collection, definition){

        var attribute = {};
        var unique = [];
        var primary = null;
        var index = [];
        var auto = [];
 
        _.each(definition, function(item, key){
            if (item.type){
                attribute[key] = item.type;
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

        var meta = {
            index:index,
            unique:unique,
            primary:primary,
            auto:auto
        };
  
        this._meta[collection] = JSON.stringify(meta);
        this._attr[collection] = JSON.stringify(attribute);
  
    },
    
    
    getKeyObject : function(collection){
        return [collection, this._meta[collection],this._attr[collection] ];
    },
    
    preParse : function(collection,values){

        schema = this._schema;
        for(attr in values){
            switch(schema[collection][attr].type){
                case 'json':
                    values[attr] = JSON.stringify(values[attr]);
                    break;
            }
        }
        return JSON.stringify(values);
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
                case 'integer':
                    data[key] = parseInt(data[key]);
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
