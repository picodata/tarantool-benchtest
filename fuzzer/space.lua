-- !!!
local json = require('json')

local operation = require('fuzzer.operation')
local utils = require('fuzzer.utils')

local ddl_tires = 3

-- common and helper functions

-- check if index contains particular field
local function index_contains_fieldno(fieldno, index_parts)
    for _, p in pairs(index_parts) do
        if p[1] == fieldno-1 then
            return true
        end
    end
    return false
end

-- check if index two indexes are equal
local function equal_indexes(parts_1, parts_2)
    if #parts_1 ~= #parts_2 then
        return false
    end
    for i, p_1 in utils.spairs(parts_1) do
        p_2 = parts_2[i]
        if p_1[1] ~= p_2[1] then
            return false
        end
    end
    return true
end

-- select random secondary index
local function rand_si(self)
    return utils.rand_item(self.si_list)
end

-- get list of fieldno for space
local function space_fieldno(self)
    local fn = {}
    for i, _ in utils.spairs(self.fields) do
        fn[#fn+1] = i
    end
    return fn
end

local function try_add_field_to_index(self, parts)
    -- find candidate fields to add to index
    local cf = {}
    for i, f in utils.spairs(self.fields) do
        if not index_contains_fieldno(i, parts) then
            cf[#cf+1] = {
                i - 1,
                f.type,
            }
        end
    end
    if #cf == 0 then
        return nil
    end

    -- new field to add to index parts
    local nf = utils.rand_item(cf)

    -- new index parts
    local np = utils.copy_items(parts)
    np[#np+1] = nf

    return np
end

local function try_remove_field_from_index(self, parts)
    -- check index length
    if #parts == 1 then
        return nil
    end

    -- index part to remove
    local _, pi = utils.rand_item(parts)

    -- new index parts
    local np = utils.copy_items(parts)
    table.remove(np, pi)

    return np
end

local function validate_pi(self, parts)
     -- check that new primary index does not duplicate any of secondary indexes
     for _, si in pairs(self.si_list) do
        if equal_indexes(si.parts, parts) then
            return false
        end
    end
    return true
end

local function validate_si(self, si_parts, si_idx)
    -- check that new secondary index does not duplicate primary index
    if equal_indexes(self.pi, si_parts) then
        return false
    end
    -- check that new secodnary index does not duplicate any of existing
    -- secondary indexes
    for idx, other_si in pairs(self.si_list) do
        if idx ~= si_idx then
            if equal_indexes(other_si.parts, si_parts) then
                return false
            end
        end
    end
    return true
end

-- select random tuple
local function rand_tuple(self)
    return utils.rand_item(self.tuples)
end

-- shorutcut to create select operation
local function select_op(self, index, key, offset, limit)
    return {
        type = operation.types.space_select,
        space = self.name,
        index = index,
        key = key,
        options = {
            offset = offset,
            limit = limit,
        }
    }
end

-- generate random offset/limit
local function rand_select_offset_or_limit(self)
    local min_offset = 1
    local max_offset = 200
    return math.random(min_offset, max_offset)
end

-- get list of fieldno for index
local function index_fieldno(self, parts)
    local fn = {}
    for _, p in utils.spairs(parts) do
        fn[#fn+1] = p[1]+1
    end
    return fn
end

-- shortuct to create index_create operation
local function create_index_op(self, index_name, index_parts)
    -- fieldno in index parts, used internally, is zero-based,
    -- but fieldno in index parts, used by API is one-based.
    local parts =  {}
    for _, p in pairs(index_parts) do
        parts[#parts+1] = {
            p[1] + 1,
        }
    end

    return {
        type = operation.types.space_add_index,
        space = self.name,
        index_name = index_name,
        index_options = {
            unique = false,
            parts = parts,
        }
    }
end

-- DDL operations

-- update list of indexes via dropping existing indexes and adding them back later

local function try_add_si(self)
    -- try to bring back random secondary index
    local si, si_idx = utils.rand_item(self.si_del)
    if not validate_si(self, si.parts, si_idx) then
        return nil
    end

    table.remove(self.si_del, si_idx)
    table.insert(self.si_list, si)

    return si
end

local function add_si(self)
    -- check for previously removed secondary indexes
    if #self.si_del == 0 then
        return nil
    end

    for i = 1,ddl_tires do
        local si = try_add_si(self)
        if si then
            -- TODO: return operation
            return nil
        end
    end

    return nil
end

local function remove_si(self)
    -- check for existing secondary indexes
    if #self.si_list == 0 then
        return nil
    end

    -- select random index to remove
    local si, si_idx = rand_si(self)
    table.remove(self.si_list, si_idx)
    table.insert(self.si_del, si)

    -- primary index is first in list
    -- first secondary index had index 2
    local idx = si_idx+1

    -- TODO: return operation
    return nil
end

-- add new secondary indexes via mutation of existing secondary indexes

local function gen_new_si_name(self)
    local name = ("secondary_%d"):format(self.si_name_counter)
    self.si_name_counter = self.si_name_counter + 1
    return name
end

local function try_add_si_via_adding_field(self)
    local si, si_idx = rand_si(self)
    local np = try_add_field_to_index(self, si.parts)
    if not np then
        return nil
    end

    if not validate_si(self, np, si_idx) then
        return nil
    end

    local nsi = {
        name = gen_new_si_name(self),
        parts = np,
    }
    self.si_list[#self.si_list+1] = nsi

    return nsi
end

local function add_si_via_adding_field(self)
    for i = 1,ddl_tires do
        local si = try_add_si_via_adding_field(self)
        if si then
            return create_index_op(self, si.name, si.parts)
        end
    end
end

local function try_add_si_via_removing_field(self)
    local si, si_idx = rand_si(self)
    local np = try_remove_field_from_index(self, si.parts)
    if not np then
        return nil
    end

    if not validate_si(self, np, si_idx) then
        return nil
    end

    local nsi = {
        name = gen_new_si_name(self),
        parts = np,
    }
    self.si_list[#self.si_list+1] = nsi

    return nsi
end

local function add_si_via_removing_field(self)
    for i = 1,ddl_tires do
        local si = try_add_si_via_removing_field(self)
        if si then
            return create_index_op(self, si.name, si.parts)
        end
    end
end

-- alter existing indexes

local function try_add_pi_field(self)
    local nf, npi = try_add_field_to_index(self, self.pi)
    if not nf then
        return nil
    end

    if not validate_pi(self, npi) then
        return nil
    end

    self.pi = npi

    return nf
end

local function add_pi_field(self)
    for i = 1,ddl_tires do
        local nf = try_add_pi_field(self)
        if nf then
            -- TODO: return operation
            return nil
        end
    end
    return nil
end

local function try_remove_pi_field(self)
    local fi, npi = try_remove_field_from_index(self, self.pi)
    if not fi then
        return nil
    end

    if not validate_pi(self, npi) then
        return nil
    end

    self.pi = npi

    return fi
end

local function remove_pi_field(self)
    for i = 1,ddl_tires do
        local fi = try_remove_pi_field(self)
        if fi then
            -- TODO: return operation
            return nil
        end
    end
    return nil
end

local function try_add_si_field(self)
    local si, si_idx = rand_si(self)
    local nf, nsi = try_add_field_to_index(self, si.parts)
    if not nf then
        return nil
    end

    if not validate_si(self, nsi, si_idx) then
        return nil
    end

    si.parts = nsi

    return si.name, nf
end

local function add_si_field(self)
    -- check that at least one secondary index exists
    if #self.si_list == 0 then
        return nil
    end

    for i = 1,ddl_tires do
        local si_name, nf = try_add_si_field(self)
        if nf then
            -- TODO: return operation
            return nil
        end
    end

    return nil
end

local function try_remove_si_field(self)
    local si, si_idx = rand_si(self)
    local fi, nsi = try_remove_field_from_index(self, si.parts)
    if not fi then
        return nil
    end

    if not validate_si(self, nsi, si_idx) then
        return nil
    end

    si.parts = nsi

    return si.name, fi
end

local function remove_si_field(self)
    -- check that at least one secondary index exists
    if #self.si_list == 0 then
        return nil
    end

    for i = 1,ddl_tires do
        local si_name, fi = try_remove_si_field(self)
        if fi then
            -- TODO: return operation
            return nil
        end
    end

    return nil
end

-- DML operations

local function insert(self)
    self.tuple_counter = self.tuple_counter + 1
    local t = self.tuple_ctor(self.tuple_counter)
    self.tuples[#self.tuples+1] = t

    return {
        type = operation.types.space_insert,
        space = self.name,
        tuple = t,
    }
end

local function select_by_pk(self)
    local t = rand_tuple(self)
    local fn = index_fieldno(self, self.pi)
    local key = utils.items_by_index(t, fn)

    return select_op(self, 'primary', key)
end

local function select_by_full_sk(self)
    local t = rand_tuple(self)
    local si = rand_si(self)
    local fn = index_fieldno(self, si.parts)
    local key = utils.items_by_index(t, fn)

    return select_op(self, si.name, key)
end

local function select_by_partial_sk(self)
    local t = rand_tuple(self)
    local si = rand_si(self)
    local fn = utils.first_rand_items(index_fieldno(self, si.parts), #si.parts-1)
    local key = utils.items_by_index(t, fn)

    return select_op(self, si.name, key)
end

local function select_with_offset_by_full_sk(self)
    local op = select_by_full_sk(self)
    op.options.offset = rand_select_offset_or_limit(self)

    return op
end

local function select_with_offset_by_partial_sk(self)
    local op = select_by_partial_sk(self)
    op.options.offset = rand_select_offset_or_limit(self)

    return op
end

local function update(self)
    -- copy some fields from t1 to t2
    local t1 = rand_tuple(self)
    local t2 = rand_tuple(self)

    -- construct update operators from t1
    local operator_fn = utils.rand_items(space_fieldno(self), #self.fields)
    local operator_val = utils.items_by_index(t1, operator_fn)
    local operators = {}
    for i, fn in utils.spairs(operator_fn) do
        operators[#operators+1] = { '=', fn, operator_val[i] }
    end

    -- update t2 tuple by applying operators
    for _, o in utils.spairs(operators) do
        t2[o[2]] = o[3]
    end

    -- primary key for t2
    local pk_fn = index_fieldno(self, self.pi)
    local pk = utils.items_by_index(t2, pk_fn)

    return {
        type = operation.types.space_update,
        space = self.name,
        key = pk,
        operators = operators,
    }
end

local function delete(self)
    local t, ti = rand_tuple(self)
    table.remove(self.tuples, ti)

    local pk_fn = index_fieldno(self, self.pi)
    local pk = utils.items_by_index(t, pk_fn)

    return {
        type = operation.types.space_delete,
        space = self.name,
        key = pk,
    }
end

local function new(space_name, tuple_constructor)
    local self = {
        name = space_name,

        fields = {},
        pi = {},
        si_list = {},
        si_del = {},
        si_name_counter = 1000,

        tuple_ctor = tuple_constructor,
        tuples = {},
        tuple_counter = 1
    }

    -- space format
    local space = box.space[space_name]
    for _, f in utils.spairs(space:format()) do
        self.fields[#self.fields+1] = {
            name = f.name,
            type = f.type,
        }
    end

    -- space indexes
    for _, idx in pairs(box.space._index:select{space.id}) do
        if idx.name == 'primary' then
            self.pi = utils.copy_items(idx.parts)
        else
            self.si_list[#self.si_list+1] = {
                name = idx.name,
                parts = utils.copy_items(idx.parts),
            }
        end
    end

    local index_metatable = {
        -- DDL operations

        -- TODO:
        -- update list of indexes via dropping existing indexes and adding them back later
        -- add_si = add_si,
        -- remove_si = remove_si,

        -- add new secondary indexes via mutation of existing secondary indexes
        add_si_via_adding_field = add_si_via_adding_field,
        add_si_via_removing_field = add_si_via_removing_field,

        -- TODO:
        -- alter existing indexes
        -- add_pi_field = gen_diff_add_pi_field,
        -- remove_pi_field = gen_diff_remove_pi_field,
        -- add_si_field = gen_diff_add_si_field,
        -- remove_si_field = gen_diff_remove_si_field,

        -- DML operations

        -- insert tuple

        insert = insert,

        -- select tuples

        -- primary key
        select_by_pk = select_by_pk,
        -- full secondary key
        select_by_full_sk = select_by_full_sk,
        -- partial secondary key
        select_by_partial_sk = select_by_partial_sk,
        -- offset with full secondary key
        select_with_offset_by_full_sk = select_with_offset_by_full_sk,
        -- offset with partial secondary key
        select_with_offset_by_partial_sk = select_with_offset_by_partial_sk,

        -- update tuple

        update = update,

        -- delete tuple

        delete = delete,
    }

    return setmetatable(self, {
        __index = index_metatable,
    })
end

return {
    new = new,
}