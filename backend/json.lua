
--local json = require 'dkjson'
local json = require "lunajson-to-replace-dkjson"

local unpack = table.unpack or unpack

local function open(file)
	if not file then return {} end

	-- if file exists laod it with json
	local file, err = io.open(file)
	if err then
		return nil
	end

	local content = file:read('*a')
	file:close()

	local ok, obj = pcall(json.decode, content)
	if not ok then return nil end

	--local obj, pos, err = json.dkjson.decode(content) -- FIXME: dkjson ne raise pas d'erreur ?!
	--if err then return nil end

	return obj
end

local function save(file, data, indent)
	if not file then return end
	local content = json.encode(data, { indent = indent })

	local file = assert(io.open(file, 'w'))
	file:write(content)
	file:close()
end

return {open=open, save=save}
