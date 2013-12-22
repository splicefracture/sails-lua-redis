local key         = KEYS[1]

-- table attributes values
local aa, err = redis.call("hgetall", "waterline:"..key..":attribute");
local attr = {}
for aaidx,aakey in pairs(aa) do
    if aaidx % 2 == 1 then
        local aaval = aa[aaidx+1]
        attr[aakey] = aaval
    end
end

for akey,avalue in pairs(attr) do        
    local resp, err = redis.call("del", "waterline:"..key..":"..akey);
end
local resp, err = redis.call("del", "waterline:"..key);


return 1;
