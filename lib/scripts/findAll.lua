local _key        = KEYS[1]

--local _meta      = cjson.decode(KEYS[2])
local _attribute = cjson.decode(KEYS[3])



local output = {}
local mem, err = redis.call("hgetall", "waterline:".._key);

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
        
        if _attribute[vattr] == "integer" or _attribute[vattr] == "float" then
           val = tonumber(val) 
        end
        
        if _attribute[vattr] == "array" then
            val = cjson.encode(val)
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
