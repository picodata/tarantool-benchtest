local function new(space_name, tuple_constructor)
    local self = {
        space = space_name,

        fields = {},
        pi = {},
        si_list = {},
        si_del = {},
        si_name_counter = 1000,

        tuple_ctor = constructor,
        tuples = {},
    }

    -- fill schema/index information

    local index_metatable = {
        -- DDL operation generation

        -- new index generation
        gen_ddl_add_si_by_adding_field = gen_op_add_si_by_adding_field,
        gen_ddl_add_si_by_removing_field = gen_diff_add_si_by_removing_field,

        -- DML and data query operation generation

        -- insert tuple

        gen_insert = gen_insert,

        -- select tuple

        gen_select_pk = function(self)
        end,

        gen_select_full_sk = function(self)
        end,

        gen_select_partial_sk = function(self)
        end,

        gen_select_offset = function(self)
        end,

        gen_select_offset_and_full_sk = function(self)
        end,

        gen_select_offset_and_partial_sk = function(self)
        end,

        -- update tuple

        gen_update = gen_update,

        -- delete tuple

        gen_delete = gen_delete,
    }

    return setmetatable(self, {
        __index = index_metatable,
    })
end

return {
    new = new,
}