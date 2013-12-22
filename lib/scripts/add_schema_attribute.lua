local key        = KEYS[1]
local attribute  = ARGV

for idx,attr in pairs(attribute) do
    if (idx % 2 == 1) then
        local value = attribute[idx+1]
        local aadd, err1 = redis.call("hset", "waterline:"..key..":attribute", attr, value);
    end
end

