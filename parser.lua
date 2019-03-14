local loadstring = loadstring or load

local StringBuilder = {}

function StringBuilder:init(text)
    local self = {}
    self.buffer = { text }

    setmetatable(self, { __index = StringBuilder, __tostring = function(self) return table.concat(self.buffer) end })
    return self
end

function StringBuilder:append(char)
    self.buffer[#self.buffer + 1] = char
end

local function read(source)
    for c in source:gmatch '.' do
        coroutine.yield(c)
    end

    coroutine.yield('\0')
end

local tokens = {
    word = 1,    -- UPDATE, FROM, DELETE, AND, OR
    int = 2,     -- 1, 2, 3, 4
    str = 3,     -- 'hello', "world"
    symbol = 4,  -- >, <, =, >=, <=, !=, (, )
}

local Lexer = {}

function Lexer:getAtEndOfSouce()
    return self.currentChar == '\0'
end

function Lexer:readChar()
    local c = self.read()
    self.currentChar = c
end

function Lexer:init(source)
    local self = {}
    self.currentChar = ''
    self.tokenBuffer = StringBuilder:init()

    local read = coroutine.wrap(read)
    self.read = function() return read(source) end

    setmetatable(self, { __index = Lexer })
    self:readChar()

    return self
end


function Lexer:readWord()
    repeat
        self:readAndStore()
    until not self.currentChar:match('%w')

    return { type = tokens.word, value = self:extractStoredChars() }
end

function Lexer:readDigit()
    repeat
        self:readAndStore()
    until not self.currentChar:match('%d')

    return { type = tokens.int, value = self:extractStoredChars() }
end

function Lexer:readString(quote)
    self:readChar()

    while not self.atEndOfSource and not self.currentChar:match(quote) do
        self:readAndStore()
    end

    self:readChar()

    return { type = tokens.str, value = self:extractStoredChars() }
end

function Lexer:readSymbol()
    if self.currentChar:match('[,()*]') then
        self:readAndStore()
    elseif self.currentChar:match('[=!<>]') then
        if self.currentChar == '!' then
            self.currentChar = '~'
        end

        self:readAndStore()
        if self.currentChar == '=' then
            self:readAndStore()
        end
    else
        error("SQL: Invalid expression")
    end
    return { type = tokens.symbol, value = self:extractStoredChars() }
end

function Lexer:readAndStore()
    self.tokenBuffer:append(self.currentChar)
    self:readChar()
end

function Lexer:readNextToken()
    self:skipWhitespace()
    local c = self.currentChar

    if self:getAtEndOfSouce() then
        return
    elseif c:match('%d') then
        return self:readDigit()
    elseif c:match('%a') then
        return self:readWord()
    elseif c:match('[\'"]') then
        return self:readString(c:match('[\'"]'))
    else
        return self:readSymbol()
    end
end


function Lexer:skipWhitespace()
    while self.currentChar:match('%s') do
        self:readChar()
    end
end

function Lexer:extractStoredChars()
    local value = tostring(self.tokenBuffer)
    self.tokenBuffer.buffer = {}
    return value
end

function Lexer:getTokens()
    local action = function()
        local token = self:readNextToken()
        while token ~= nil do
            coroutine.yield(token)
            token = self:readNextToken()
        end
    end

    return coroutine.wrap(action)
end


local Parser = {}

-- For some reason table.insert returns nil in funciton below
local insert = table.insert

function Parser:getAtEndOfSouce()
    return self.currentToken == nil
end

function Parser:parseUpdateStatement()
    self:readNextToken() -- Skip 'update'

    local table = self.currentToken.value

    self:readNextToken()
    self:skipToken(tokens.word, 'set')

    local columns, column = {}
    local value
    while not self:getAtEndOfSouce() and string.lower(self.currentToken.value) ~= 'where' do
        if self.currentToken.type == tokens.symbol then
            if self.currentToken.value == '=' then
                self:readNextToken()
                if self.currentToken.type == tokens.int then
                    value = tonumber(self.currentToken.value)
                else
                    value = self.currentToken.value
                end
            else
                columns[column] = value
                column = nil
            end
        else
            column = self.currentToken.value
        end

        self:readNextToken()
    end
    columns[column] = value

    insert(self.args, table)
    insert(self.args, columns)
end

function Parser:parseWhereStatement()
    self:readNextToken()

    local action = StringBuilder:init('return ')

    while not self:getAtEndOfSouce() do

        if self.currentToken.type == tokens.word and self.currentToken.value ~= 'and' and self.currentToken.value ~= 'or' then
            action:append('row.' .. self.currentToken.value)
        elseif self.currentToken.type == tokens.symbol and self.currentToken.value == '=' then
            action:append('==')
        elseif self.currentToken.type == tokens.str then
            action:append("'" .. self.currentToken.value .. "'")
        else
            action:append(self.currentToken.value .. ' ')
        end

        self:readNextToken()
    end

    local func = loadstring('return function(row) ' .. tostring(action) .. ' end')()
    insert(self.args, func)
end

function Parser:parseDeleteStatement()
    self:readNextToken() -- Skip 'delete'

    if self.currentToken.type == tokens.symbol then
        self:skipToken(tokens.symbol, '*')
    end
end

function Parser:parseFromStatement()
    self:readNextToken() -- skip 'from'

    table.insert(self.args, 1, self.currentToken.value)
    self:readNextToken()
end

function Parser:parseCreateStatement()
    self:readNextToken()
    self:skipToken(tokens.word, 'table')

    self:checkForUnexpectedEnd()
    local table = self.currentToken.value
    self:readNextToken()

    self:skipToken(tokens.symbol, '(')

    local columns = {}
    local count, value, default = 1

    while not self:getAtEndOfSouce() do
        if self.currentToken.value == ',' or self.currentToken.value == ')' then
            if default then
                columns[value] = default
            else
                columns[count] = value
            end

            value, default = nil, nil
            count = count + 1
        elseif string.lower(self.currentToken.value) == 'default' then
            self:readNextToken()

            if self.currentToken.type == tokens.int then
                default = tonumber(self.currentToken.value)
            else
                default = self.currentToken.value
            end
        else
            value = self.currentToken.value
        end

        self:readNextToken()
    end

    insert(self.args, table)
    insert(self.args, columns)
end

function Parser:parseSelectStatement()
    self:readNextToken()

    local values = {}

    while string.lower(self.currentToken.value) ~= 'from' do
        if self.currentToken.type == tokens.word then
            insert(values, self.currentToken.value)
        end

        self:readNextToken()
        self:checkForUnexpectedEnd()
    end

    insert(self.args, values)
end

function Parser:parseInsertStatement()
    self:readNextToken()
    self:skipToken(tokens.word, 'into')

    local table = self.currentToken.value
    self:readNextToken()

    self:skipToken(tokens.word, 'values')
    self:skipToken(tokens.symbol, '(')

    local row = {}
    local i = 1

    while not self:getAtEndOfSouce() do
        if self.currentToken.type ~= tokens.symbol then
            if self.currentToken.type == tokens.int then
                insert(row, tonumber(self.currentToken.value))
            elseif self.currentToken.value ~= 'null' then
                row[i] = self.currentToken.value
            end
            i = i + 1
        end

        self:readNextToken()
    end

    insert(self.args, table)
    insert(self.args, row)

    self:readNextToken()
end

function Parser:parseDropStatement()
    self:readNextToken()
    self:skipToken(tokens.word, 'table')

    local table = self.currentToken.value
    self:readNextToken()

    insert(self.args, table)
end

local words = {
    update = Parser.parseUpdateStatement,
    where = Parser.parseWhereStatement,
    from = Parser.parseFromStatement,
    select = Parser.parseSelectStatement,
    delete = Parser.parseDeleteStatement,
    create = Parser.parseCreateStatement,
    insert = Parser.parseInsertStatement,
    drop = Parser.parseDropStatement,
}

function Parser:init(source)
    local lexer = Lexer:init(source)
    local self = {}

    self.currentToken = {}
    self.args = {}

    self.readNextToken = function(self) self.currentToken = lexer:getTokens()() end

    setmetatable(self, { __index = Parser})
    self:readNextToken()
    return self
end

function Parser:checkForUnexpectedEnd()
    if self:getAtEndOfSouce() then
        error("SQL: Unexpected end of soucre")
    end
end

function Parser:skipToken(type, value)
    self:checkForUnexpectedEnd()
    if self.currentToken.type ~= type or string.lower(self.currentToken.value) ~= value then
        error("SQL: Expected '" .. value .. "'")
    end
    self:readNextToken()
end

function Parser:readNextStatement()
    local word
    if self.currentToken then word = string.lower(self.currentToken.value) end

    if self:getAtEndOfSouce() then
        return self.command, self.args
    elseif self.currentToken.type ~= tokens.word then
        error("SQL: Expected a statement")
    elseif words[word] then
        if not self.command then self.command = word end
        words[word](self)
        return
    end

    error("SQL: Unrecgonized statement '" .. self.currentToken.value .. "'")
end

return Parser
