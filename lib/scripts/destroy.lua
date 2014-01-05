local _key         = KEYS[1]

local _meta      = cjson.decode(KEYS[2])
local _attribute = cjson.decode(KEYS[3])

local _values    = cjson.decode(ARGV[1])

local primary = _values[_meta["primary"]]

for _,idx in pairs(_meta["index"]) do
    if _values[idx] and type(_values[idx]) == "number" then
        local resp, err = redis.call("zrem", "waterline:".._key..":"..idx, primary); 
    end
end


local output = 0
for key, value in pairs(_attribute) do    
    local resp, err = redis.call("hdel", "waterline:".._key, key..":"..primary);
    output = tonumber(resp) + output
end


return output
