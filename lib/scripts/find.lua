local _key        = KEYS[1]
--local primary    = KEYS[2]

local _meta      = cjson.decode(KEYS[2])
local _attribute = cjson.decode(KEYS[3])

local _id_index      = cjson.decode(ARGV[1])
local _minmax_index  = cjson.decode(ARGV[2])
local _primary_index = cjson.decode(ARGV[3])

local next = next

-- do findAll
if next(_id_index) == nil and next(_minmax_index) == nil then

    local output = {}
    local all, err = redis.call("hgetall", "waterline:".._key);

    for idx, key in pairs(all) do
        if idx % 2 == 1 then
        
            local head={}
            local i=1
            for str in string.gmatch(key, "([^:]+)") do
                head[i] = str
                i = i + 1
            end
            
            local attr  = head[1]
            local pri = head[2]
            local val = all[idx+1]
            
            local row = output[pri]
            if not row then
                row = {}
            end
            
            if _attribute[attr] == "integer" or _attribute[attr] == "float" then
               val = tonumber(val) 
            end
            
            if _attribute[attr] == "array" then
                val = cjson.encode(val)
            end
            
            row[attr] = val
            output[pri] = row

        end
    end

    local final = {}
    for midx, val in pairs(output) do
        table.insert(final,val)
    end

    return cjson.encode(final)

end

-- more targeted search
local retrieve_keys = {}

for _, key in pairs(_primary_index) do
    table.insert(retrieve_keys,key)
end

for key, obj in pairs(_id_index) do
    for _, val in pairs(obj) do
        local pri, err = redis.call("zrangebyscore", "waterline:".._key..":"..key, val, val);
        for _,pval in pairs(pri) do
            table.insert(retrieve_keys,pval)
        end
    end
end

for key, obj in pairs(_minmax_index) do    

    local _min = 0
    if obj['min'] then 
        local _min = obj['min']
    end
    
    local _max = 2^64
    if obj['max'] then 
        _max = obj['max']
    end
        
    local pri, err = redis.call("zrangebyscore", "waterline:".._key..":"..key, _min, _max);

    for _,pval in pairs(pri) do
        table.insert(retrieve_keys,pval)
    end
    
end



local output = {}
for idx, key in pairs(retrieve_keys) do
    local row = {}
    for attr, type in pairs(_attribute) do
        local val, err = redis.call("hget", "waterline:".._key, attr..":"..key);
        if type == "integer" or type == "float" then
           val = tonumber(val) 
        end
        if type == "array" then
            val = cjson.decode(val)
        end
        row[attr] = val
    end    
    table.insert(output,row)
end

return cjson.encode(output)
