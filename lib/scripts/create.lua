local key        = KEYS[1]
--local primary    = KEYS[2]


local primary = nil

local meta = {}

local index  = {}
local unique = {}
local auto   = {}

-- table meta values
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

-- set primary key
local primary_key = "id"
if meta["primary"] then
    primary_key = meta["primary"]
end

-- table values
local values = {}
for vidx,vkey in pairs(ARGV) do
    if vidx % 2 == 1 then
        local value = ARGV[vidx+1]
        values[vkey] = value
    end
end


--resolve primary key
if table.getn(auto) == 0 and not values[primary_key] then
    return {err="Primary key was not set and is not auto increment"}
end


-- resolve auto increment keys
for aidx, akey in pairs(auto) do

    if values[akey] then
        return {err="An attribute that is auto increment cannot be set"}
    end

    if attr[akey] == "string" then
        return {err="Primary key was not set and is not auto increment"}
    elseif attr[akey] == "integer" then
        local k, err1 =redis.call("hincrby", "waterline:"..key..":meta","current-key:"..akey,1); 
        values[akey] = k
    end
end

primary = values[primary_key]

-- check primary key
local hx, err = redis.call("hexists", "waterline:"..key, primary_key..":"..primary);
if tonumber(hx) == 1 then
    return {err="Record does not satisfy unique constraints"}
end

-- TODO : Use reverse index to lookup
-- very inefficient
-- unique constraint
local mem, err = redis.call("hgetall", "waterline:"..key);
for uidx, ukey in pairs(mem) do
    if uidx % 2 == 1 then
        local value = mem[uidx+1]
        for _,unkey in pairs(unique) do
            if string.find(ukey..":", unkey) and values[unkey] == value then
                return {err="Record does not satisfy unique constraints"}
            end
        end
    end
end
-----------------------------------------------

--- build reverse index keys
for _,idx in pairs(index) do
    if values[idx] and type(values[idx]) == "number" then
        local iresp, ierr = redis.call("zadd", "waterline:"..key..":"..akey, value, primary); 
    end
end

-- add the value
local output = {}
for akey,avalue in pairs(values) do    
    local resp, err = redis.call("hset", "waterline:"..key, akey..":"..primary, avalue);
    
    if attr[akey] == "integer" or attr[akey] == "float" then
        avalue = tonumber(avalue)
    end
    
    if attr[akey] == "array" then
        local arval = {}
        for item,_ in string.gmatch(avalue,"%w+") do
            table.insert(arval,item)
        end
        avalue = arval
    end
    
    output[akey] = avalue;
end

return cjson.encode(output)
