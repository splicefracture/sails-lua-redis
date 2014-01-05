local key        = KEYS[1]

local m, err = redis.call("del", "waterline:"..key..":meta");
local a, err = redis.call("del", "waterline:"..key..":attribute");
local k, err = redis.call("del", "waterline:"..key);

return (tonumber(m) + tonumber(a) + tonumber(k)) == 3
