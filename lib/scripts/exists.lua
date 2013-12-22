local key        = KEYS[1]

local m, err = redis.call("exists", "waterline:"..key..":meta");
local a, err = redis.call("exists", "waterline:"..key..":attribute");
local k, err = redis.call("exists", "waterline:"..key);

return (tonumber(m) + tonumber(a) + tonumber(k)) == 3
