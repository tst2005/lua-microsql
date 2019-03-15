-- lunajson instead of dkjson
local cjson = require "cjson"
local nullv = assert(cjson.null)
local json = {
        encode = function(x)
                return cjson.encode(x)
        end,
	decode = function(content)
		local ok, x = pcall(function() return cjson.decode(content) end)
		if not ok then
                        return nil, x
                end
                return x
	end,
        null = nullv,
}
--[[
json.dkjson = setmetatable(
	{
		decode = function(content)
			local ok, x, pos = pcall(lunajson.decode, content, nil, nullv)
			if not ok then
				return nil, pos, x
			end
			return x, pos, nil
		end,
	},{	__index = json}
)
]]--

return json
