test_run = require('test_run').new()

REPLICASET = { 'storage_a', 'storage_b' }
test_run:create_cluster(REPLICASET, 'misc')

util = require('util')
util.wait_master(test_run, REPLICASET, 'storage_a')

-- Apply schema on master.

-- Run fuzzer on master.

_ = test_run:cmd("switch default")
test_run:drop_cluster(REPLICASET)