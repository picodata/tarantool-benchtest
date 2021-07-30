Look at https://www.tarantool.io/en/doc/latest/book/replication/repl_bootstrap/ for insturction for bootstraping replica set (master-replica). You have to start master and replica in different consoles and in appropriate directories...

The following tests peroforms concurrent DML/DDL operations on master. It is quiet "gentle" in terms of performance:
1) Load generation starts only after 10-15 seconds after replication pair is ready.
2) Fibers, performing DML and DDL operations, always sleep fair amount of time before generating next "batch" of load.
3) There is no load on replica.

Replica stabely crashes in first 10 minutes of test for the following builds/versions of Tarantool:
- 1.10.10-67-g2babf65ee
- 2.9.0-254-g68851b351

If read operations are enabled on replica - https://gitlab.com/picodata/tarantool-repro-suite/-/blob/reproduce-vinyl-crash-tnt-issue-4572/example/fuzz_vinyl/replica/fuzz_vinyl_replica.lua#L83, the crash will not occure anymore. However, it will seriously affect replica's CPU usage.
