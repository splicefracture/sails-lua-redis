
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

        var primary_set = {}
        var id_set = {}
        var minmax_set = {}

        if (criteria && criteria.where){
            var out =  _.each(criteria.where,function(value,key){
                
                if (!id_set[key]){
                    id_set[key] = []
                }
                
                if (!primary_set[key]){
                    primary_set[key] = []
                }
                
                if (key == primary){
                    primary_set[key] = primary_set[key].concat(value)
                }
                
                if(_.contains(index,key)&&(_.isArray(value) || !_.isObject(value))){
                    
                    id_set[key] = id_set[key].concat(value)

                }else if(_.isObject(value) && _.contains(index,key)){

                    _.each(_.pairs(value),function(pr, _){
                        
                        var val = pr.pop()
                        var oper  = pr.pop()

                        nkey = utils.filterRange(oper)  
                        
                        if (nkey == 'min' || nkey == 'max'){
                            if (!minmax_set[key]){
                                minmax_set[key] = {}
                            }
                            if (nkey == 'min' && minmax_set[key]['min']){
                                minmax_set[key]['min'] = Math.min(minmax_set[key]['min'],val)
                            }else if (nkey == 'min'){
                                minmax_set[key]['min'] = val
                            }
                            if (nkey == 'max' && minmax_set[key]['max']){
                                minmax_set[key]['max'] = Math.max(minmax_set[key]['max'],val)
                            }else if (nkey == 'max'){
                                minmax_set[key]['max'] = val
                            }
                        }
                    })
                }
                if (_.isEmpty(id_set[key])){
                    delete id_set[key]
                }
                if (_.isEmpty(primary_set[key])){
                    delete primary_set[key]
                }
            });
        }
        return [JSON.stringify(id_set),JSON.stringify(minmax_set),JSON.stringify(primary_set)]
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
                case 'integer':
                    values[attr] = Number(parseInt(values[attr]).toFixed())
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
                    data[key] = parseInt(data[key])
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
