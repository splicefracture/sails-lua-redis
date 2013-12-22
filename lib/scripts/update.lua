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

if 1==1 then
    --return cjson.encode(values)
end

local primary_key = meta["primary"]
local primary = values[primary_key]


-- TODO : Use reverse index to lookup
-- very inefficient
-- unique constraint
local mem, err = redis.call("hgetall", "waterline:"..key);
for uidx, ukey in pairs(mem) do
    if uidx % 2 == 1 then
        local value = mem[uidx+1]
        for _,unkey in pairs(unique) do
            if string.find(unkey..":", unkey) and values[unkey] == value and not (string.gsub(ukey,unkey..":","") == primary) then
                 return {err="Record does not satisfy unique constraints"}
            end
        end
    end
end

for _,idx in pairs(index) do
    if values[idx] and type(values[idx]) == "number" then
        local iresp, ierr = redis.call("zrem", "waterline:"..key..":"..akey, primary); 
        local iresp, ierr = redis.call("zadd", "waterline:"..key..":"..akey, value, primary); 
    end
end

local output = {}

for akey,avalue in pairs(attr) do    

    local resp, err = redis.call("hset", "waterline:"..key, akey..":"..primary, values[akey]);
    if attr[akey] == "integer" or attr[akey] == "float" then
        values[akey] = tonumber(values[akey])
    end
    
    if avalue == "array" then
        local arval = {}
        for item,_ in string.gmatch(avalue,"%w+") do
            table.insert(arval,item)
        end
        values[akey] = arval
    end
    
    output[akey] = values[akey]
end

return cjson.encode(output)
