-- lunajson instead of dkjson
local lunajson = require "lunajson"
local nullv = {} -- an uniq value
local json = {
        encode = function(x)
                return lunajson.encode(x, nullv)
        end,
	decode = function(content)
		local ok, x = pcall(function() return lunajson.decode(content, nil, nullv) end)
		if not ok then
                        return nil, x
                end
                return x
	end,
        null = nullv,
}

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

return json
