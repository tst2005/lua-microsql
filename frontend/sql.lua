
local Parser = require 'parser'

local function query(sql)
	local parser = Parser:init(sql)

	local command, args = parser:readNextStatement()
	while args == nil do
		command, args = parser:readNextStatement()
	end
	return command, args
end

return { query = query }
