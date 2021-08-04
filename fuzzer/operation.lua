-- !!!
local json = require('json')

-- currently supported operations from Tarantool API
local types = {
    space_insert = 'space_insert',
    space_select = 'space_select',
    space_update = 'space_update',
    space_delete = 'space_delete',
}

-- decode and execute operation
local function execute(op)
    -- !!!
    print(json.encode(op))

    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/insert/
    if op.type == types.space_insert then
        box.space[op.space]:insert(op.tuple)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/select/
    elseif op.type == types.space_select then
        r = box.space[op.space].index[op.index]:select(op.key, op.options)
        -- !!!
        print(json.encode(r))
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/update/
    elseif op.type == types.space_update then
        box.space[op.space]:update(op.key, op.operators)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/delete/
    elseif op.type == types.space_delete then
        box.space[op.space]:delete(op.key)
    else
        error('unsupported operation type')
    end
end

return {
    types = types,
    execute = execute,
}