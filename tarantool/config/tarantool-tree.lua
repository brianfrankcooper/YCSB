box.cfg {
   listen=3301,
   logger="tarantool.log",
   log_level=6,
   logger_nonblock=true,
   wal_mode="none",
--   background=true,
   pid_file="tarantool.pid"
}

box.schema.space.create("ycsb", {id = 1024})
box.space.ycsb:create_index('primary', { type = 'tree', parts = {1, 'STR'} })

box.schema.user.grant('guest', 'read,write,execute', 'universe')

function select_range(space, index, key, limit) 
   limit = limit or -1
   local response = {}
   for _, tuple in box.space[space].index[index]:pairs(key, {iterator = box.index.GE}) do
      if limit == 0 then
         break
      end
      table.insert(response, tuple)
      limit = limit - 1
   end
   return response
end
