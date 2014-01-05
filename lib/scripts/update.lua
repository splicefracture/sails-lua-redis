local _key   = KEYS[1]

local _meta      = cjson.decode(KEYS[2])
local _attribute = cjson.decode(KEYS[3])

local _values    = cjson.decode(ARGV[1])

local primary_key = _meta["primary"]
local primary = _values[primary_key]


-- TODO : Use reverse index to lookup
-- very inefficient
-- unique constraint
local mem, err = redis.call("hgetall", "waterline:".._key);
for uidx, ukey in pairs(mem) do
    if uidx % 2 == 1 then
        local value = mem[uidx+1]
        for _,unkey in pairs(_meta["unique"]) do
            if string.find(unkey..":", unkey) and _values[unkey] == value and not (string.gsub(ukey,unkey..":","") == primary) then
                 return {err="Record does not satisfy unique constraints"}
            end
        end
    end
end
--------------------------------------

for _,idx in pairs(_meta["index"]) do
    if _values[idx] and type(_values[idx]) == "number" then
        local iresp, ierr = redis.call("zrem", "waterline:".._key..":"..idx, primary); 
        local iresp, ierr = redis.call("zadd", "waterline:".._key..":"..idx, _values[idx], primary); 
    end
end

local output = {}

for key,value in pairs(_attribute) do    

    local resp, err = redis.call("hset", "waterline:".._key, key..":"..primary, _values[key]);
    if _attribute[key] == "integer" or _attribute[key] == "float" then
        _values[key] = tonumber(_values[key])
    end
    
    if value == "array" then
        --_values[key] = cjson.encode(_values[key])
    end
    
    output[key] = _values[key]
end

return cjson.encode(output)
