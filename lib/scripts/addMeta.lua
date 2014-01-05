local _key       = KEYS[1]
local _meta  = cjson.decode(ARGV[1])

    
local resp, err = redis.call("hset", "waterline:".._key..":meta", "primary", _meta["primary"]);
local resp, err = redis.call("hset", "waterline:".._key..":meta", "auto",    cjson.encode(_meta["auto"]));
local resp, err = redis.call("hset", "waterline:".._key..":meta", "unique",  cjson.encode(_meta["unique"]));
local resp, err = redis.call("hset", "waterline:".._key..":meta", "index",   cjson.encode(_meta["index"]));


