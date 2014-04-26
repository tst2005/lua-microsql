local json = require 'dkjson'
local Parser = require 'parser'

local db = {}

local function createTable(...)
    local tbl
    if type(...) ~= 'table' then
        tbl = { ... }
    else
        tbl = ...
    end

    return tbl
end

local function open(file)
    if not file then return {} end

    -- if file exists laod it with json
    local file, err = io.open(file)
    if err then return {} end

    local content = file:read('*all')
    file:close()

    local obj, pos, err = json.decode(content)
    if err then
        obj = {}
    else
        -- Converts indexs from strings to ints.
        for name, tbl in pairs(obj) do
            local new = {}
            for k, v in pairs(obj[name]) do
                new[tonumber(k) or k] = v
            end
            obj[name] = new
        end
    end

    return obj
end

local function save(file, data, indent)
    if not file then return end
    local content = json.encode(data, { indent = indent })

    local file = assert(io.open(file, 'w'))
    file:write(content)
    file:close()
end

local function contains(tbl, id)
    for i = 1, #tbl do
        if tbl[i] == id then return true end
    end
end

local function getUniqueID(tbl)
    for i = 1, 10000000 do
        if not contains(tbl, i) then return i end
    end
end

-- Iterates over every row in the database
-- and yields the index and row
local function iterate(data, name)
    for i = 1, #data[name][1] do
        local row = {}
        for j = 1, #data[name].columns do
            local column = data[name].columns[j]
            local value = data[name][j][i]
            row[column] = value
        end
        coroutine.yield(row, i)
    end
end


local function getIterator(self, name)
    return coroutine.wrap(function() iterate(self.data, name) end)
    -- local co = coroutine.create(function() iterate(data, name) end)
    -- return function() local code, i, column, value = coroutine.resume(co) return i, column, value end
end

function db.open(file, text, indent)
    local self = {}

    self.indent = indent or false
    self.file = file

    if text then
        self.data = json.decode(text)
    else
        self.data = open(self.file)
    end

    setmetatable(self, { __index = db, __tostring = function(self) return json.encode(self.data, { indent = self.indent }) end })

    return self
end

function db:setFile(file)
    self.file = file
end

-- Name - tables name
function db:create(name, columns)
    self.data[name] = {}
    self.data[name].columns = {}
    self.data[name].defaults = {}

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

        self.data[name].columns[i] = field
        self.data[name].defaults[i] = default
        self.data[name][i] = {}
        i = i + 1
    end

    -- for i, field in ipairs(columns) do
    --     self.data[name].columns[i] = field
    --     self.data[name][i] = {}
    -- end
end

-- Name - tables name
-- ... - Table or strings containing values to be insrted into table
--       set id to nil to autoincrement.
function db:insert(name, row)
    assert(self.data[name], "Table '" .. name .. "' does not exist")

    local index = #self.data[name][1] + 1

    for i = 1, #self.data[name].columns do
        local column = self.data[name][i]
        local value = row[i]

        if self.data[name].columns[i] == 'rowid' then
            if value == nil then
                value = getUniqueID(self.data[name][i])
            elseif type(value) ~= 'number' then
                error('rowid must be a number')
            elseif self.data[name][i][value] then
                error('ID already exists')
            end
        end

        if value == nil then value = self.data[name].defaults[i] end
        column[index] = value
    end

    save(self.file, self.data, self.indent)
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

    return coroutine.wrap(function() for i, v in pairs(results) do coroutine.yield(v, i) end end)
end

-- Same argumetns as where and
-- Set - table containg columns to update and their new values
function db:update(name, set, action)
    for row, i in self:where(name, action) do
        for index = 1, #self.data[name].columns do
            local column = self.data[name].columns[index]
            if set[column] then self.data[name][index][i] = set[column] end
        end
    end

    save(self.file, self.data, self.indent)
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

    return coroutine.wrap(function() for i, v in pairs(results) do coroutine.yield(v, i) end end)
end

-- Name - tables name or column nmae
-- NewName - Tables new name or table containing columns name with new name.
function db:rename(name, newName)
    assert(self.data[name], "Table '" .. name .. "' does not exist")

    if type(newName) == 'string' then
        local value = self.data[name]
        self.data[name] = nil
        self.data[newName] = value
    else
        for i = 1, #self.data[name].columns do
            local column = self.data[name].columns[i]
            local value = newName[column]

            if value then self.data[name].columns[i] = value end
        end
    end

    save(self.file, self.data, self.indent)
end

function db:drop(name)
    assert(self.data[name], "Table '" .. name .. "' does not exist")
    self.data[name] = nil
end

-- Takes same argumetns as where
function db:delete(name, action)
    assert(type(action) == 'function', "Delete must be passed a function")

    for row, i in self:where(name, action) do
        for index = 1, #self.data[name].columns do
            table.remove(self.data[name][index], i)
        end
    end

    save(self.file, self.data, self.indent)
end

function db:getColumns(name)
    return #self.data[name].columns
end

function db:getRows(name)
    return #self.data[name][1]
end

function db:exists(name)
    return self.data[name] ~= nil
end

function db:query(query)
    parser = Parser:init(query)

    local command, args = parser:readNextStatement()
    while args == nil do
        command, args = parser:readNextStatement()
    end

    return self[command](self, unpack(args))
end

local function test()
    local player_db = db.open()
    player_db:create('players', { 'age', 'name', date = 2013 })

    assert(player_db:getColumns('players') == 3, "Table created incorrectly")

    player_db:insert('players', { 19, 'Frank'})
    player_db:insert('players', {21, 'Bob'})
    player_db:insert('players', { 22, 'Delete Me!' })
    player_db:insert('players', { 20, 'Frank' })

    player_db:delete('players', function(row) return row.name == 'Delete Me!' end)
    assert(player_db:getRows('players') == 3, "Failed to delete a row")

    for row in player_db:select('players', { 'name' }) do
        for k, v in pairs(row) do
            assert(k == 'name')
        end
    end

    player_db:update('players', { date = 2012 }, function(row) return row.name == 'Bob' end)

    player_db:rename('players', { date = 'birthday' })
    for row in player_db:select('players', nil, function(row) return row.name == 'Bob' end) do
        assert(row.name == 'Bob')
        assert(row.age == 21)
        assert(row.birthday == 2012, "Failed to update row, and failed to rename column")
    end

    for row in player_db:where('players', function(row) return row.name == 'Frank' and row.age == 20 end) do
        assert(row.birthday == 2013, "Failed to insert default value")
    end

    player_db:rename('players', 'people')
    assert(player_db:exists('players') == false,  'Failed to remove table while renaming')
    assert(player_db:exists('people'),  'Failed to add table while renaming')

    player_db:create('idTest', { 'name', 'rowid'})
    player_db:insert('idTest', { 'Mr Smith'})

    if pcall(function() player_db:insert('idTest', { 'Mrs Smith', 1 }) end) then
        error('Failed to prevent overwriting of same id')
    else
        player_db:delete('idTest', function(row) return row.name == 'Mrs Smith' end)
    end

    print(player_db)
end

local function queryTest()
    local player_db = db.open('query')

    player_db:query("CREATE TABLE players (age, name, date default 2013)")
    assert(player_db:getColumns('players') == 3, "Table created incorrectly")

    player_db:query("INSERT INTO players VALUES (19, 'Frank')")
    player_db:query("INSERT INTO players VALUES (21, 'Bob')")
    player_db:query("INSERT INTO players VALUES (22, 'Delete Me!')")
    player_db:query("INSERT INTO players VALUES (20, 'Frank')")

    player_db:query("DELETE * FROM players WHERE name = 'Delete Me!'")
    assert(player_db:getRows('players') == 3, "Failed to delete a row")

    for row in player_db:query('SELECT name FROM players') do
        for k, v in pairs(row) do
            assert(k == 'name')
        end
    end

    player_db:query("UPDATE players SET date = 2012 WHERE name = 'Bob'")
    for row in player_db:query("SELECT * FROM players WHERE name = 'Bob'") do
        assert(row.name == 'Bob')
        assert(row.age == 21)
        assert(row.date == 2012, "Failed to update row, and failed to rename column")
    end

    player_db:query("CREATE TABLE idTest (name, rowid)")
    player_db:query("INSERT INTO idTest VALUES ('Mr Smith')")
    player_db:query("INSERT INTO idTest VALUES ('Mr Frank')")
    player_db:query("INSERT INTO idTest VALUES ('Mr Bob')")

    player_db:query("DROP TABLE idTest")
    print(player_db)
end

local function benchmark()
    local counter = 100000

    local regdb = db.open('new')
    local querydb = db.open('query')

    local begin = os.clock()
    for i = 1, counter do
        querydb:select('players', nil, function(row) return row.name == 'Bob' end)
    end
    local reg = os.clock() - begin

    local begin = os.clock()
    for i = 1, counter do
        querydb:query("SELECT * FROM players WHERE name = 'Bob'")
    end
    local query = os.clock() - begin

    print(reg, ' - ', query)
end

test()
queryTest()
benchmark()

return { open = db.open }
