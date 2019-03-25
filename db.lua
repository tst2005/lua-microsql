
--local json = require 'dkjson'
local json = require "replace-dkjson-by.lunajson"

local Parser = require 'parser'

local default_backend = require "backend.json"
local default_frontend = require"frontend.sql"

local unpack = table.unpack or unpack

local coroutine = assert(coroutine)
local wrap, yield = assert(coroutine.wrap), assert(coroutine.yield)

local db = {}

local function backendsave(self, name)
	if not self.file then return false end

	local backend = assert(self.backend)
	local save = assert(backend.save)

--	local data = self.data
--	if name then
--		data = {[name]=data[name]} -- save only this table
--	end
--	for name, tbl in pairs(data) do
--		savetable(self.file, name, tbl)
--	end

	save(self.file, self.data, self.indent)
	return true
end

local function backendload(self)
	local backend = assert(self.backend)
	local open = assert(backend.open)
	assert(self.file)
	return open(self.file, self.data, self.indent)
end

local function needflush(self, name)
	if self.file then
		local changes = (self.changes or 0) +1
		local autoflush = self.autoflush
		if autoflush==nil or autoflush==true then autoflush = 0 end
		if type(autoflush)=="number" and changes >= autoflush then
			--print("autoflush", changes, autoflush)
			backendsave(self, name)
			changes = 0
		end
		self.changes = changes
	end
end


-- Iterates over every row in the database
-- and yields the index and row
local function iterate(data, name)
	local tbl = data[name]
	for i = 1, #tbl.rows[1] do
		local row = {}
		for j = 1, #tbl.columns do
			local column = tbl.columns[j]
			local value = tbl.rows[j][i]
			row[column] = value
		end
		yield(row, i)
	end
end


local function getIterator(self, name)
	return wrap(function() iterate(self.data, name) end)
	-- local co = coroutine.create(function() iterate(data, name) end)
	-- return function() local code, i, column, value = coroutine.resume(co) return i, column, value end
end

local function integrity_check(obj)
	-- tables integrity check
	for name, tbl in pairs(obj) do
		assert(tbl.columns)
		assert(tbl.defaults)
		assert(tbl.rows) -- data[tablename].rows[colname][rowid]=value
	end
	return obj
end

local function db_open(file, text, indent)
	local self = {}

	self.indent = indent or false
	self.file = file
	self.backend = default_backend
	self.changes = 0
	self.data = {} -- empty db

	if text then
		self.data = json.decode(text)
		self.changes = 1 -- the file is open, the db from text is not written
	elseif file then
		local got = backendload(self)
		if got then
			self.data = integrity_check(got)
			self.changes = 0 -- do not rewrite the file
		end
	else
		self.changes = 1 -- no file, data is not written
	end

	setmetatable(self, { __index = db, __tostring = function(self) return json.encode(self.data, { indent = self.indent }) end })

	return self
end
db.open = db_open -- FIXME allow reopen ? there is no db:close() ; x = db.open(...) ; x:close() ; x.open(...) ?

function db:flush()
	--print("forced-flush called")
	if self.file and self.changes and self.changes > 0 then
		--print("forced-flush=done", self.changes)
		if not backendsave(self, nil) then
			return false
		end
		self.changes = 0
	end
	return true
end

function db:save()
	if not backendsave(self, nil) then
		return false
	end
	return true
end

function db:close()
	db:flush()
end

-- not used ?!
--[[
function db:setFile(file)
	self.file = file
end
]]--

-- Name - tables name
function db:create(name, columns)
	local tbl = {}
	tbl.columns = {}
	tbl.defaults = {}
	tbl.rows = {}
	self.data[name] = tbl

	local i = 1
	for k, v in pairs(columns) do
		local field
		local default = json.null

		if type(k) == 'number' then
			field = v
		else
			field = k
			default = v
		end

		tbl.columns[i] = field
		tbl.defaults[i] = default
		tbl.rows[i] = {}
		i = i + 1
	end

	-- for i, field in ipairs(columns) do
	--     tbl.columns[i] = field
	--     tbl.rows[i] = {}
	-- end
end


local function contains(rows, id)
	for i = 1, #rows do
		if rows[i] == id then return true end
	end
end

local function getUniqueID(rows)
	for i = 1, 10000000 do
		if not contains(rows, i) then return i end
	end
end


local getUniqueIDversion="v1"

if getUniqueIDversion=="v2" then
	io.stderr:write("Use getUniqueIDversion\t"..getUniqueIDversion.."\n")
	function getUniqueID(rows)
		local max = 10000000
		local random = assert(math.random)
		local rand = function() return random(1,max) end
		for i = 1, 10000000 do
			local id = rand()
			if not contains(rows, i) then return i end
			for i = 1, math.min(10, max-id) do -- FIXME max-id or max-id+1 ?
				if not contains(rows, id+i) then return id+i end
			end
		end
	end
end

-- Name - tables name
-- ... - Table or strings containing values to be inserted into table
--       set id to nil to autoincrement.
function db:insert(name, row)
	local tbl = assert(self.data[name], "Table '" .. name .. "' does not exist")

	local index = #tbl.rows[1] + 1

	for i = 1, #tbl.columns do
		local column = tbl.rows[i]
		local value = row[i]

		if tbl.columns[i] == 'rowid' then
			if value == nil then
				value = getUniqueID(tbl.rows[i])	-- tbl.rows[colnum] => {[rowid]=value} == rows
			elseif type(value) ~= 'number' then
				error('rowid must be a number')
			elseif tbl.rows[i][value] then
				error('ID already exists')
			end
		end

		if value == nil then value = tbl.defaults[i] end
		column[index] = value
	end
	needflush(self, name)
end

-- Name - tables name
-- ... - Table containing columns to be returned
function db:select(name, columns, action)
	assert(self.data[name], "Table '" .. name .. "' does not exist")
	local all = false
	if not columns or #columns < 1 then
		all = true
	else
		for i = 1, #columns do
			columns[columns[i]] = true
		end
	end

	local iterator = getIterator(self, name)
	local results = {}

	for row, i in iterator do
		local passed = false
		if action then passed = action(row) end

		for column, value in pairs(row) do
			if all or columns[column] then

				if action then
					if passed then
						if not results[i] then results[i] = {} end
						results[i][column] = value
					end
				else
					if not results[i] then results[i] = {} end
					results[i][column] = value
				end
			end
		end

	end

	return wrap(function() for i, v in pairs(results) do yield(v, i) end end)
end

-- Same argumetns as where and
-- Set - table containg columns to update and their new values
function db:update(name, set, action)
	for row, i in self:where(name, action) do
		local tbl = self.data[name]
		for index = 1, #tbl.columns do
			local column = tbl.columns[index]
			if set[column] then tbl.rows[index][i] = set[column] end
		end
	end
	needflush(self, name)
end

-- Name - tables name
-- Action - Takes row as argument and returns true for
--          that row to be returned
function db:where(name, action)
	assert(self.data[name], "Table '" .. name .. "' does not exist")
	assert(type(action) == 'function', "Where must be passed a function")

	local iterator = getIterator(self, name)
	local results = {}

	for row, i in iterator do
		if action(row) == true then
			results[i] = row
		end
	end

	return wrap(function() for i, v in pairs(results) do yield(v, i) end end)
end

-- Name - tables name or column name
-- NewName - Tables new name or table containing columns name with new name.
function db:rename(name, newName)
	local tbl = assert(self.data[name], "Table '" .. name .. "' does not exist")

	if type(newName) == 'string' then
		self.data[name] = nil
		self.data[newName] = tbl
	else
		for i = 1, #tbl.columns do
			local column = tbl.columns[i]
			local value = newName[column]

			if value then tbl.columns[i] = value end
		end
	end
	--needflush(self, name, "delete") -- backendremove(self, name)
	needflush(self, newName)
end

function db:drop(name)
	assert(self.data[name], "Table '" .. name .. "' does not exist")
	self.data[name] = nil
	needflush(self, name)
	--backendsave(self, name) --backendremove(self, name)
end

-- Takes same argumetns as where
function db:delete(name, action)
	assert(type(action) == 'function', "Delete must be passed a function")

	local tbl = self.data[name]
	for row, i in self:where(name, action) do
		local cols = tbl.rows
		for index = 1, #tbl.columns do
			table.remove(cols[index], i)
		end
	end
	needflush(self, name)
end

function db:getColumns(name)
	return #self.data[name].columns
end

function db:getRows(name)
	return #self.data[name].rows[1]
end

function db:exists(name)
	return self.data[name] ~= nil
end

function db:query(sql)
	local command, args = default_frontend.query(sql)
	return self[command](self, unpack(args))
end

return { open = db_open }
