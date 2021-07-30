#!/usr/bin/env tarantool

box.cfg{
    listen = 3502,
    replication = {'replicator:password@127.0.0.1:3501',  -- master
                   'replicator:password@127.0.0.1:3502'}, -- replica
    read_only = true
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
local fuzz_start_delay = 15
local fuzz_duration = 1000
local read_load = 25
local read_rate = 0.3
local tuple_refresh_rate = 3
local space_count = 2

-- init dml operation generators (only 'select' is used on replica)
local spaces = {}
for i = 1,space_count do
    local space_name = ("space_%d"):format(i)
    table.insert(spaces, space_fuzzer.new(space_name, nil))
end

-- fuzzing routines

local function fuzz_space_read(space)
    for i=1,read_load do
        operation.execute(space:select_by_pk())
    end
    for i=1,read_load do
        operation.execute(space:select_by_full_sk())
    end
    for i=1,read_load do
        operation.execute(space:select_by_partial_sk())
    end
    for i=1,read_load do
         operation.execute(space:select_with_offset_by_full_sk())
    end
    for i=1,read_load do
        operation.execute(space:select_with_offset_by_partial_sk())
    end
end

local function fuzz_space_dml(space)
    local fuzz_start = clock.realtime()
    local last_tuple_refresh = 0

    while true do
        if clock.realtime() - last_tuple_refresh > tuple_refresh_rate then
        space.tuples = box.space[space.name]:select{}
            last_tuple_refresh = clock.realtime()
        end

        fuzz_space_read(space)
        fiber.sleep(read_rate)

        if clock.realtime() - fuzz_start > fuzz_duration then
            break
        end
    end
end

-- start fuzzing with delay

fiber.sleep(fuzz_start_delay)

for _, s in pairs(spaces) do
    --fiber.create(fuzz_space_dml, s)
end

-- wait a bit more before dropping spaces to avoid errors if fuzzing fibers
fiber.sleep(fuzz_duration + 0.5)

os.exit(0)