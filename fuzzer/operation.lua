-- !!!
local json = require('json')

-- currently supported operations from Tarantool API
local types = {
    space_insert =    'space_insert',
    space_select =    'space_select',
    space_update =    'space_update',
    space_delete =    'space_delete',
    space_add_index = 'space_add_index',
}

-- decode and execute operation
local function execute(op)
    -- !!!
    --print(json.encode(op))

    --validate operation
    if not box.space[op.space] then
        print(("skipping operation - space %s does not exist"):format(op.space))
        return
    elseif op.index and not box.space[op.space].index[op.index] then
        print(("skipping operation - index %s for space %s does not exist"):format(op.index, op.space))
        return
    end

    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/insert/
    if op.type == types.space_insert then
        box.space[op.space]:insert(op.tuple)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/select/
    elseif op.type == types.space_select then
        box.space[op.space].index[op.index]:select(op.key, op.options)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/update/
    elseif op.type == types.space_update then
        box.space[op.space]:update(op.key, op.operators)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/delete/
    elseif op.type == types.space_delete then
        box.space[op.space]:delete(op.key)
    -- https://www.tarantool.io/en/doc/1.10/reference/reference_lua/box_space/create_index/
    elseif op.type == types.space_add_index then
        box.space[op.space]:create_index(op.index_name, op.index_options)
    else
        error('unsupported operation type')
    end
end

return {
    types = types,
    execute = execute,
}