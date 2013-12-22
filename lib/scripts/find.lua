local key        = KEYS[1]
local primary    = KEYS[2]
local criteria    = ARGV[1]

-- table attributes values
local aa, err = redis.call("hgetall", "waterline:"..key..":attribute");
local attr = {}
for aaidx,aakey in pairs(aa) do
    if aaidx % 2 == 1 then
        local aaval = aa[aaidx+1]
        attr[aakey] = aaval
    end
end

local output = {}
local mem, err = redis.call("hgetall", "waterline:"..key);
for midx, mkey in pairs(mem) do
    if midx % 2 == 1 then
    
        local head={}
        local i=1
        for str in string.gmatch(mkey, "([^:]+)") do
            head[i] = str
            i = i + 1
        end
        
        local vattr  = head[1]
        local pri = head[2]
        local val = mem[midx+1]
        
        local row = output[pri]
        if not row then
            row = {}
        end
        
        if attr[vattr] == "integer" or attr[vattr] == "float" then
           val = tonumber(val) 
        end
        
        if attr[vattr] == "array" then
            local arval = {}
            for item,_ in string.gmatch(val,"%w+") do
                table.insert(arval,item)
            end
            val = arval
        end
        
        row[vattr] = val
        output[pri] = row

    end
end

local final = {}
for midx, val in pairs(output) do
    table.insert(final,val)
end

return cjson.encode(final)
