-- lunajson instead of dkjson
local lunajson = require "lunajson"
local nullv = {} -- an uniq value
local json = {
        encode = function(x)
                return lunajson.encode(x, nullv)
        end,
        decode = function(x)
		return lunajson.decode(x, nil, nullv)
	end,
        null = nullv,
}
return json
