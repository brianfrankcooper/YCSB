box.cfg {
   listen=3303,
   logger="tarantool.log",
   log_level=5,
   logger_nonblock=true,
   wal_mode="none",
   pid_file="tarantool.pid"
}

box.schema.space.create("ycsb", {id = 1024})
box.space.ycsb:create_index('primary', {type = 'hash', parts = {1, 'STR'}})
box.schema.user.grant('guest', 'read,write,execute', 'universe')
