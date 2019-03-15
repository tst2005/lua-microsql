
local Parser = require 'parser'
local db = require "db"

local function test()
	local player_db = db.open()
	player_db:create('players', { 'age', 'name', date = 2013 })

	assert(player_db:getColumns('players') == 3, "Table created incorrectly")

	player_db:insert('players', { 19, 'Frank' })
	player_db:insert('players', { 21, 'Bob' })
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

