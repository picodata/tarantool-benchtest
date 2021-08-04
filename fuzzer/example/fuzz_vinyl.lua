-- !!!
local json = require('json')

local fiber = require('fiber')
local clock = require('clock')

local operation = require('./fuzzer/operation')
local space_fuzzer = require('./fuzzer/space')
local utils = require('./fuzzer/utils')


-- fuzzing paramters
local fuzz_duration = 0.3
local dml_insert_load = 1000
local dml_read_load = 50
local dml_update_load = 75
local dml_delete_load = 75

-- create vinyl spaces
box.cfg{}

-- !!!
--box.space.space_1:drop()

-- 'space_1'

local space_1 = box.schema.space.create('space_1', {
    engine = 'vinyl',
    format = {
        {'f1', 'integer'},
        {'f2', 'integer'},
        {'f3', 'integer'},
        {'f4', 'integer'},
        {'f5', 'integer'},
    }
})
space_1:create_index('primary', {unique = true, parts = { 'f1' }})
space_1:create_index('secondary_1', {unique = false, parts = { 'f1', 'f4' }})
space_1:create_index('secondary_2', {unique = false, parts = { 'f2', 'f3' }})

local function space_1_ctor(i)
    return {
        -- primary index
        i,
        -- other fields
        utils.rand_number(20),
        utils.rand_number(20),
        utils.rand_number(20),
        utils.rand_number(20),
    }
end

-- init operation generators
local spaces = {
    space_fuzzer.new('space_1', space_1_ctor),
}

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
        fuzz_space_read(space)
        --fuzz_space_update(space)
        --fuzz_space_delete(space)
        if clock.realtime() - fuzz_start > fuzz_duration then
            break
        end
    end
end

for i=1,1 do--#spaces do
    fiber.create(fuzz_space_dml, spaces[i])
end

-- wait a bit more before dropping spaces to avoid errors if fuzzing fibers
fiber.sleep(fuzz_duration + 0.5)

-- cleanup
space_1:drop()

os.exit(0)