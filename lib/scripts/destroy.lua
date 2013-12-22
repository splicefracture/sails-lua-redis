local key         = KEYS[1]

--local attributes  = ARGV

local meta = {}

local index  = {}
local unique = {}
local auto   = {}

local mm, err = redis.call("hgetall", "waterline:"..key..":meta");
for mmidx,mmkey in pairs(mm) do
    if mmidx % 2 == 1 then
        local mmval = mm[mmidx+1]
        if mmkey == "index" or mmkey == "unique" or mmkey == "auto" then
            for item,_ in string.gmatch(mmval,"%w+") do
                if mmkey == "index" then
                    table.insert(index,item)
                elseif mmkey == "unique" then
                    table.insert(unique,item)
                elseif mmkey == "auto" then
                    table.insert(auto,item)
                end
            end
        else
            meta[mmkey] = mmval
        end
    end
end

-- table attributes values
local aa, err = redis.call("hgetall", "waterline:"..key..":attribute");
local attr = {}
for aaidx,aakey in pairs(aa) do
    if aaidx % 2 == 1 then
        local aaval = aa[aaidx+1]
        attr[aakey] = aaval
    end
end

local values = {}
for vidx,vkey in pairs(ARGV) do
    if vidx % 2 == 1 then
        local value = ARGV[vidx+1]
        values[vkey] = value
    end
end


local primary_key = meta["primary"]
local primary = values[primary_key]

for _,idx in pairs(index) do
    if values[idx] and type(values[idx]) == "number" then
        local iresp, ierr = redis.call("zrem", "waterline:"..key..":"..akey, primary); 
    end
end


local output = {}

for akey,avalue in pairs(attr) do    
    local resp, err = redis.call("hdel", "waterline:"..key, akey..":"..primary);
    --table.insert(key_set, akey)
    --table.insert(value_set, avalue)
    
    table.insert(output, akey..":"..primary)
    table.insert(output, resp)
end


return cjson.encode(output)
