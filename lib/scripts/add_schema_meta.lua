local key        = KEYS[1]
local attribute  = ARGV

for idx,attr in pairs(attribute) do
    if (idx % 2 == 1) then
        local value = attribute[idx+1]
        local madd, err2 = redis.call("hset", "waterline:"..key..":meta", attr, value);
    end
end

