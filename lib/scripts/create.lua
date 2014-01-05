local _key       = KEYS[1]

local _meta      = cjson.decode(KEYS[2])
local _attribute = cjson.decode(KEYS[3])

local _values    = cjson.decode(ARGV[1])

-- resolve auto increment keys
for idx, key in pairs(_meta["auto"]) do

    if _values[key] then
        return {err="An attribute that is auto increment cannot be set"}
    end
    if _attribute[key] == "integer" then

        local new_key, err =redis.call("hincrby", "waterline:".._key..":meta","current-key:"..key,1); 
        _values[key] = new_key

    else
        return {err="Primary key was not set and is not auto increment"}
    end
end

local primary_key = _meta["primary"]
local primary = _values[primary_key]

if not primary_key or not primary then
    return {err="Primary key was not set and is not auto increment"}
end

-- check primary key
local hx, err = redis.call("hexists", "waterline:".._key, _meta["primary"]..":"..primary);
if tonumber(hx) == 1 then
    return {err="Record does not satisfy unique constraints"}
end

-----------------------------------------------
-- TODO : Use reverse index to lookup
-- very inefficient
-- unique constraint
local mem, err = redis.call("hgetall", "waterline:".._key);
for idx, key in pairs(mem) do
    if idx % 2 == 1 then
        local value = mem[idx+1]
        for _,key2 in pairs(_meta["unique"]) do
            if string.find(key..":", key2) and _values[key2] == value then
                return {err="Record does not satisfy unique constraints"}
            end
        end
    end
end
-----------------------------------------------

--- build reverse index keys
for _,idx in pairs(_meta["index"]) do
    if _values[idx] and type(_values[idx]) == "number" then
        local resp, err = redis.call("zadd", "waterline:".._key..":"..idx, _values[idx], primary); 
    end
end

-- add the value
local output = {}
for key,value in pairs(_values) do    

    local resp, err = redis.call("hset", "waterline:".._key, key..":"..primary, value);
    if _attribute[key] == "integer" or _attribute[key] == "float" then
        value = tonumber(value)
    end
    if _attribute[key] == "json" then
        value = cjson.decode(value)
    end
    output[key] = value;
end

return cjson.encode(output)
