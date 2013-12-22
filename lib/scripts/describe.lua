local key        = KEYS[1]

local meta, err = redis.call("hgetall", "waterline:"..key..":meta");
local attr, err = redis.call("hgetall", "waterline:"..key..":attribute");


if not meta or not attr then
    return {err="   "}
end


local meta_obj = {}
for midx,mkey in pairs(meta) do
    if midx % 2 == 1 then
        meta_obj[mkey] = meta[midx+1]
    end
end

local attr_obj = {}
for aidx,akey in pairs(attr) do
    if aidx % 2 == 1 then
        attr_obj[akey] = attr[aidx+1]
    end
end

local output = {}

table.insert(output, cjson.encode(meta_obj))
table.insert(output, cjson.encode(attr_obj))

return output
