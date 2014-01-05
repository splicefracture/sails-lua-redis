local _key        = KEYS[1]
local _attribute  = cjson.decode(ARGV[1])

for key,value in pairs(_attribute) do
    local resp, err = redis.call("hset", "waterline:".._key..":attribute", key, value);
end

