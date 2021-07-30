# Tarantool data API fuzzer:
- fuzzer/executor.lua - execute DDL/DML operations (and other operations on data), represented by Lua tables.
- fuzzer/space.lua - state tracking and operation generation for Tarantool space - https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/

# Vynil:
- example/fuzz_vinyl - reprduce/debug for https://github.com/tarantool/tarantool/issues/4572 (and stress vinyl egnine in general...)