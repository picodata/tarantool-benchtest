box.cfg{
    listen = 3501,
    replication = {'replicator:password@127.0.0.1:3501',  -- master
                   'replicator:password@127.0.0.1:3502'}, -- replica
    read_only = false
}
box.once("schema", function()
    box.schema.user.create('replicator', {password = 'password'})
    box.schema.user.grant('replicator', 'replication') -- grant replication role
end)

-- !!!
local json = require('json')

local fiber = require('fiber')
local clock = require('clock')

local operation = require('fuzzer.operation')
local space_fuzzer = require('fuzzer.space')
local utils = require('fuzzer.utils')

-- fuzzing paramters
local fuzz_start_delay = 10
local fuzz_duration = 1000
local dml_insert_load = 250
local dml_read_load = 50
local dml_update_load = 50
local dml_delete_load = 150
local dml_rate = 0.3
local ddl_rate = 3
local space_count = 2

-- init operation generators

local function tuple_ctor(i)
    return {
        -- primary index
        i,
        -- other fields
        utils.rand_number(5),
        utils.rand_number(5),
        utils.rand_number(5),
        utils.rand_number(5),
    }
end

local spaces = {}
for i = 1,space_count do
    local space_name = ("space_%d"):format(i)
    local space = box.schema.space.create(space_name, {
        engine = 'vinyl',
        format = {
            {'f1', 'integer'},
            {'f2', 'integer'},
            {'f3', 'integer'},
            {'f4', 'integer'},
            {'f5', 'integer'},
        }
    })
    space:create_index('primary', {unique = true, parts = { 'f1' }})
    space:create_index('secondary_1', {unique = false, parts = { 'f1', 'f4' }})
    space:create_index('secondary_2', {unique = false, parts = { 'f2', 'f3' }})

    table.insert(spaces, space_fuzzer.new(space_name, tuple_ctor))
end

-- fuzzing routines

local function fuzz_space_insert(space)
    for j=1,dml_insert_load do
        operation.execute(space:insert())
    end
end

local function fuzz_space_read(space)
    for i=1,dml_read_load do
        operation.execute(space:select_by_pk())
    end
    for i=1,dml_read_load do
        operation.execute(space:select_by_full_sk())
    end
    for i=1,dml_read_load do
        operation.execute(space:select_by_partial_sk())
    end
    for i=1,dml_read_load do
         operation.execute(space:select_with_offset_by_full_sk())
    end
    for i=1,dml_read_load do
        operation.execute(space:select_with_offset_by_partial_sk())
    end
end

local function fuzz_space_update(space)
    for i=1,dml_update_load do
        operation.execute(space:update())
    end
end

local function fuzz_space_delete(space)
    for i=1,dml_delete_load do
        operation.execute(space:delete())
    end
end

local function fuzz_space_dml(space)
    local fuzz_start = clock.realtime()
    while true do
        fuzz_space_insert(space)
        fiber.sleep(dml_rate)
        fuzz_space_read(space)
        fiber.sleep(dml_rate)
        fuzz_space_update(space)
        fiber.sleep(dml_rate)
        fuzz_space_delete(space)
        fiber.sleep(dml_rate)
        if clock.realtime() - fuzz_start > fuzz_duration then
            break
        end
    end
end

local function fuzz_space_ddl(space)
    local fuzz_start = clock.realtime()
    while true do
        -- try generate random ddl operation for space
        local ddl_op = {}
        local ddl_type = utils.rand_number(2)
        if ddl_type == 2 then
            ddl_op = space:add_si_via_adding_field()
        else
            ddl_op = space:add_si_via_removing_field()
        end

        -- if ddl operation has been successfully generated,
        -- we will execute it, otherwise try to generate another
        -- operation
        if ddl_op then
            operation.execute(ddl_op)
            fiber.sleep(ddl_rate)
        end

        if clock.realtime() - fuzz_start > fuzz_duration then
            break
        end
    end
end

-- start fuzzing with delay

fiber.sleep(fuzz_start_delay)

for _, s in pairs(spaces) do
    fiber.create(fuzz_space_dml, s)
    fiber.create(fuzz_space_ddl, s)
end

-- wait a bit more before dropping spaces to avoid errors if fuzzing fibers
fiber.sleep(fuzz_duration + 0.5)

os.exit(0)