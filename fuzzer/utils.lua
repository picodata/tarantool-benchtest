local function rand_number(n)
    return math.random(n)
end

local function rand_item(t)
    local idx = rand_number(#t)
    return t[idx], idx
end

local function rand_items(t, max_count)
    local items = {}

    local from_idx = 1
    local to_idx = #t
    while (from_idx < #t) and (#items <= max_count) do
        local idx = math.random(from_idx, to_idx)
        items[#items+1] = t[idx]
        from_idx = idx+1
    end

    return items
end

local function first_rand_items(t, max_count)
    local items = {}

    local count = rand_number(max_count)
    for idx=1,count do
        items[#items+1] = t[idx]
    end

    return items
end

local function spairs(t)
    -- collect the keys
    local keys = {}
    for k in pairs(t) do keys[#keys+1] = k end

    table.sort(keys)

    -- return the iterator function
    local i = 0
    return function()
        i = i + 1
        if keys[i] then
            return keys[i], t[keys[i]]
        end
    end
end

local function has_item(t, item)
    for _, v in pairs(t) do
        if v == item then
            return true
        end
    end
    return false
end

local function equal_items(t1, t2)
    for i, v in spairs(t1) do
        if v ~= t2[i] then
            return false
        end
    end
    return true
end

local function copy_items(t)
    local tc = {}
    for _, item in spairs(t) do
        tc[#tc+1] = item
    end
    return tc
end

local function items_by_index(t, idxs)
    local fields = {}
    for _, i in spairs(idxs) do
        fields[#fields+1] = t[i]
    end
    return fields
end

return {
    rand_number = rand_number,
    rand_item = rand_item,
    rand_items = rand_items,
    first_rand_items = first_rand_items,
    spairs = spairs,
    has_item = has_item,
    equal_items = equal_items,
    copy_items = copy_items,
    fields_by_index = fields_by_index,
}