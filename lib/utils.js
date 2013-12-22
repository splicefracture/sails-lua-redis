/**
 * Utility dependencies
 */

var _ = require('underscore');
_.str = require('underscore.string');
_.mixin(_.str.exports());


var DataGrouper = (function() {
    var has = function(obj, target) {
        return _.any(obj, function(value) {
            return _.isEqual(value, target);
        });
    };

    var keys = function(data, names) {
        return _.reduce(data, function(memo, item) {
            var key = _.pick(item, names);
            if (!has(memo, key)) {
                memo.push(key);
            }
            return memo;
        }, []);
    };

    var group = function(data, names) {
        var stems = keys(data, names);
        return _.map(stems, function(stem) {
            return {
                key: stem,
                vals:_.map(_.where(data, stem), function(item) {
                    return _.omit(item, names);
                })
            };
        });
    };

    group.register = function(name, converter) {
        return group[name] = function(data, names) {
            return _.map(group(data, names), converter);
        };
    };

    return group;
}());




/**
 * Expose adapter utility functions
 */

var utils = module.exports = exports;

/**
 * Serialize the configuration object
 *
 * @param {Object} collection
 * @return {Object}
 */

function serializeConfig(config) {
  return {
    port: config.port,
    host: config.host,
    options: config.options,
    password: config.password
  };
};

/**
 * Escape a string to be used as a `RegExp`
 *
 * @param {String} str
 */

function escapeString(text) {
  if (!arguments.callee.sRE) {
    var specials = [
      '/', '.', '*', '+', '?', '|',
      '(', ')', '[', ']', '{', '}', '\\'
    ];
    arguments.callee.sRE = new RegExp(
      '(\\' + specials.join('|\\') + ')', 'g'
    );
  }
  return text.replace(arguments.callee.sRE, '\\$1');
};

/**
 * Return a filter function satisfies criteria
 *
 * @param {Object} criteria
 * @return {Function}
 */


utils.aggregators = {

    average : function(key, values){
        return _.reduce(_.pluck(values,key), function(memo, item){
            return memo + item;
        }, 0) / values.length;
    },

    max : function(key,values){
        return _.max(_.pluck(values,key), function(val){ return val; });
    },

    min : function(key,values){
        return _.min(_.pluck(values,key), function(val){ return val; });
    },

    sum : function(key,values){
        return _.reduce(_.pluck(values,key), function(memo, val){ return memo + val; }, 0);
    }
}

function dynamicSort(property) { 
    return function (obj1,obj2) {
        return obj1[property[0]] > obj2[property[0]] ? property[1]
            : obj1[property[0]] < obj2[property[0]] ? property[1]*-1 : 0;
    }
}

function dynamicSortMultiple(sorter) {
    var props = sorter;
    return function (obj1, obj2) {
        var i = 0, result = 0, numberOfProperties = props.length;
        while(result === 0 && i < numberOfProperties) {
            result = dynamicSort(props[i])(obj1, obj2);
            i++;
        }
        return result;
    }
}

 utils.groupBy = function(grouper, agg, values){

    var grouped = DataGrouper(values,grouper)
    var output = []

    for(var g in grouped){
        var row = {};
        _.each(agg,function(v,k){
            _.each(v,function(v2,_){
                row[v2] = utils.aggregators[k](v2,grouped[g].vals)
            });
        });
        output.push(_.extend(grouped[g].key,row));
    }
    return output;
};

utils.modifiers = {
    
    limit : function(lim,values){
        return _.first(values,lim);
    },
    
    skip : function(offset,values){
        return _.rest(values,offset);
    },
        
    sort : function(sorter,values){
        values.sort(dynamicSortMultiple(_.pairs(sorter)));
        return values;
    }
}

testFns = {
    

    bitWiseAnd : function(a,b){
        return a & b;
    },

    bitWiseOr : function(a,b){
        return a | b;
    },
    
    lessThan : function(a,b){
        return a < b;
    },
    
    greaterThan : function(a,b){
        return a > b;
    },

    lessThanOrEqual : function(a,b){
        return a <= b;
    },
    
    greaterThanOrEqual : function(a,b){
        return a >= b;
    },

    not : function(a,b){
        return a != b;
    },
    
    startsWith : function(a,b){
        return _.startsWith(a.toLowerCase(),b.toLowerCase());
    },
    
    endsWith : function(a,b){
        return _.endsWith(a.toLowerCase(),b.toLowerCase());
    },
    
    contains : function(a,b){
        return ~a.toLowerCase().indexOf(b.toLowerCase());
    },
    
    like : function(a,b){
        
        b = escapeString(b)
        
        /*
        var c = (b.split("%").length - 1)
        if (c == 1 && _.startsWith(b,"%")){
            return _.startsWith(a,b.substring(1,b.length))
        }else if (c == 1 && _.endsWith(b,"%")){
            return _.endsWith(a,b.substring(0,b.length-1))
        }
        */
        
        matcher = new RegExp(b.replace(/%/g, '.*'), 'i');
        return matcher.test(a);

/*
        var t = escapeString(b).toLowerCase();
        t = t.replace(/%/g, ".*");
        //t = t.replace(/_/g, ".");
        return a.toLowerCase().match(t)
*/
    }
    
}

function filterOperands(key){
    
    if(key == ">="){
        return "greaterThanOrEqual";
    }
    if(key == "<="){
        return "lessThanOrEqual";
    }
    if(key == "<"){
        return "lessThan";
    }
    if(key == ">"){
        return "greaterThan";
    }
    if(key == "!"){
        return "not";
    }
    return key;
}


utils.match = function match(record, criteria) {
        
    if (criteria.where == null || !criteria ) return true;

    var where = _.pairs(criteria.where); 

    return _.reduce(where,function(memo,item){
        
        test_value = item.pop();
        key = item.pop();
        
        if (!memo || !_.has(record,key)) return false;        
        record_value = record[key]

        if (_.isArray(test_value) ){
            return ~_.indexOf(test_value,record_value);
        }else if(_.isObject(test_value)){
            
            return _.reduce(_.pairs(test_value),function(memo,value){
            
                if (!memo) return false;
                test_cond = value.pop();
                operand = filterOperands(value.pop());
            
                if (!_.has(testFns,operand)) return false;
                return testFns[operand](record_value,test_cond);
            },true);
        }else{
            if (typeof record_value == "string" && typeof test_value == "string"){
                return record_value.toLowerCase() === test_value.toLowerCase();
            }else{
                return record_value === test_value;   
            }
        }
    },true);
};

