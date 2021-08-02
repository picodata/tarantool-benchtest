local ddl_tires = 3

-- common and helper functions

-- get field names
local function field_names(self) {
    local ns = {}
    for _, f in spairs(self.fields) do
        ns[#ns+1] = f.name
    end
    return ns
}

-- get field names from index parts
local function index_fields(self, parts) {
    local fs = {}
    for _, p in spairs(parts) do
        fn = self.fields[p.fieldno]
        fs[#fs+1] = fn
    end
    return fs
}

-- select random secondary index
local function rand_si(self)
    return rand_item(self.si_list)
end

local function try_add_field_to_index(self, index)
    -- find candidate fields to add to index
    local cf = {}
    for _, f in spairs(self.fields) do
        if not has_item(index, f) then
            cf[#cf+1] = f
        end
    end
    if #cf == 0 then
        return nil
    end

    -- new field to add
    local nf = rand_item(cf)

    -- new index
    local ni = copy_items(index)
    ni[#ni+1] = nf

    return nf, ni
end

local function try_remove_field_from_index(self, index)
    -- check index length
    if #index == 1 then
        return nil
    end

    -- index of field to remove
    local _, fi = rand_item(index)

    -- new index
    local ni = copy_items(index)
    table.remove(ni, fi)

    return fi, ni
end

local function validate_pi(self, pi)
     -- check that new primary index does not duplicate any of secondary indexes
     for _, si in pairs(self.si_list) do
        if equal_items(si.parts, pi) then
            return false
        end
    end
    return true
end

local function validate_si(self, si, si_idx)
    -- check that new secondary index does not duplicate primary index
    if equal_items(self.pi, si) then
        return false
    end
    -- check that new secodnary index does not duplicate any of existing
    -- secondary indexes
    for idx, other_si in pairs(self.si_list) do
        if idx ~= si_idx then
            if equal_items(other_si.parts, si) then
                return false
            end
        end
    end
    return true
end

-- select random tuple
local function rand_tuple(self)
    return rand_item(self.tuples)
end

-- DDL operations

-- update list of indexes via dropping existing indexes and adding them back later

local function try_add_si(self)
    -- try to bring back random secondary index
    local si, si_idx = rand_item(self.si_del)
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
    local nf, np = try_add_field_to_index(self, si.parts)
    if not nf then
        return nil
    end

    if not validate_si(self, np, si_idx) then
        return nil
    end

    local nsi = {
        parts = np,
        name = gen_new_si_name(self),
    }
    self.si_list[#self.si_list+1] = nsi

    return nsi
end

local function add_si_via_adding_field(self)
    for i = 1,ddl_tires do
        local si = try_add_si_via_adding_field(self)
        if si then
            -- TODO: return operation
            return nil
        end
    end
end

local function try_add_si_via_removing_field(self)
    local si, si_idx = rand_si(self)
    local fi, np = try_remove_field_from_index(self, si.parts)
    if not fi then
        return nil
    end

    if not validate_si(self, np, si_idx) then
        return nil
    end

    local nsi = {
        parts = np,
        name = gen_new_si_name(self),
    }
    self.si_list[#self.si_list+1] = nsi

    return nsi
end

local function add_si_via_removing_field(self)
    for i = 1,ddl_tires do
        local si = try_add_si_via_removing_field(self)
        if si then
            -- TODO: return operation
            return nil
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
    local t = self.ctor(#self.tuples+1)
    self.tuples[#self.tuples+1] = t

   -- TODO: return operation
   return nil
end

local function select_by_pk(self)
    local t = rand_tuple(self)
    local pk = fields_by_name(t, index_fields(self.pi))

    -- TODO: return operation
    return nil
end

local function select_by_full_sk(self)
    local t = rand_tuple(self)
    local si = rand_si(self)
    local sk = fields_by_name(t, index_fields(si.parts))

    -- TODO: return operation
    return nil
end

local function select_by_partial_sk(self)
    local t = rand_tuple(self)
    local si = rand_si(self)
    local sk = fields_by_name(t, first_rand_items(index_fields(si.parts) #si.parts-1))

    -- TODO: return operation
    return nil
end

local function select_with_offset(self)
    local t = rand_tuple(self)
    local o = fields_by_name(t, index_fields(self.pi))

    -- TODO: return operation
    return nil
end

local function select_with_offset_by_full_sk(self)
    -- random offset
    local _, o = rand_tuple(self)

    -- random select by secondary key
    local t = rand_tuple(self)
    local si = rand_si(self)
    local sk = fields_by_name(t, index_fields(si.parts))

    -- TODO: return operation
    return nil
end

local function select_with_offset_by_partial_sk(self)
    -- random offset by primary key
    local _, o = rand_tuple(self)

    -- random select by secondary key
    local t = rand_tuple(self)
    local si = rand_si(self)
    local sk = fields_by_name(t, rand_items(index_fields(si.parts), #si.parts-1))

    -- TODO: return operation
    return nil
end

local function update(self)
    local t1 = rand_tuple(self)
    local t2 = rand_tuple(self)

    -- select fields to copy from r1 to r2
    local fn = rand_items(field_names(self), #self.fields)
    local upd_fields = fields_by_name(t1, fn)

    -- update record
    for _, f in pairs(upd_fields) do
        t2[f.name] = f.value
    end

    -- operation
    local pk = fields_by_name(t2, self.pi)
    local upd = {}
    for _, f in pairs(upd_fields) do
        upd[f.name] = f.value
    end

    -- TODO: return operation
    return nil
end

local function delete(self)
    local t, ti = rand_tuple(self)
    table.remove(self.tuples, ti)

    local pk = fields_by_name(t, self.pi)

    -- TODO: return operation
    return nil
end

local function new(space_name, tuple_constructor)
    local self = {
        name = space_name,

        fields = {},
        pi = {},
        si_list = {},
        si_del = {},
        si_name_counter = 1000,

        tuple_ctor = constructor,
        tuples = {},
    }

    -- space format
    space = box.space[space_name]
    for _, f in spairs(space:format()) do
        self.fields[#self.fields+1] = {
            name = f.name,
            type = f.type,
        }
    end

    -- space indexes
    for _, idx in spairs(space.index) do
        if idx.name == 'primary' then
            self.pi = idx.parts
        else
            self.si_list[#self.si_list+1] = {
                name = idx.name,
                parts = copy_items(idx.parts),
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
        add_si_via_adding_field = gen_op_add_si_by_adding_field,
        add_si_via_removing_field = gen_diff_add_si_by_removing_field,

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

        select_by_pk = function(self)
        end,

        select_by_full_sk = function(self)
        end,

        select_by_partial_sk = function(self)
        end,

        select_with_offset = function(self)
        end,

        select_with_offset_by_full_sk = function(self)
        end,

        select_with_offset_by_partial_sk = function(self)
        end,

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