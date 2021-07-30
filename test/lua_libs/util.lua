local function wait_master(test_run, replicaset, master)
    log.info('Waiting until slaves are connected to a master')
    local all_is_ok
    while true do
        all_is_ok = true
        for _, replica in pairs(replicaset) do
            if replica == master then
                goto continue
            end
            local info = test_run:eval(replica, 'box.info.replication')
            if #info == 0 or #info[1] < 2 then
                all_is_ok = false
                goto continue
            end
            info = info[1]
            for _, replica_info in pairs(info) do
                local upstream = replica_info.upstream
                if upstream and upstream.status ~= 'follow' then
                    all_is_ok = false
                    goto continue
                end
            end
::continue::
        end
        if not all_is_ok then
            fiber.sleep(0.1)
        else
            break
        end
    end
    log.info('Slaves are connected to a master "%s"', master)
end