package rmap

import "github.com/redis/go-redis/v9"

var (
	// luaAppend is the Lua script used to append an item to an array key and
	// return its new value.
	luaAppend = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])

	   -- If the value exists, append the new value, otherwise assign ARGV[2] directly
	   v = (v and v .. "," .. ARGV[2]) or ARGV[2]

	   -- Set the updated value in the hash and publish the change
	   redis.call("HSET", KEYS[1], ARGV[1], v)
	   local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(v), v)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)

	   return v
	`)

	// luaAppendUnique is the Lua script used to append an item to a set and return
	// the result.
	luaAppendUnique = redis.NewScript(`
	  local v = redis.call("HGET", KEYS[1], ARGV[1])
	  local newValues = {}
	  local changed = false

	  -- Split ARGV[2] into a table of new values
	  for value in string.gmatch(ARGV[2], "[^,]+") do
	    table.insert(newValues, value)
	  end

	  -- If the value exists, process it, else set it directly
	  if v then
	    local existingValues = {}
	    -- Split existing values into a table
	    for value in string.gmatch(v, "[^,]+") do
	      existingValues[value] = true
	    end

	    -- Append unique new values to v
	    for _, newValue in ipairs(newValues) do
	      if not existingValues[newValue] then
	        v = (v == "") and newValue or v .. "," .. newValue
	        changed = true
	      end
	    end
	  else
	    v = table.concat(newValues, ",")
	    changed = true
	  end

	  -- If changes were made, update the hash and publish the event
	  if changed then
	    redis.call("HSET", KEYS[1], ARGV[1], v)
	    local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(v), v)
	    redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	  end

	  return v
	`)

	// luaDelete is the Lua script used to delete a key and return its previous
	// value.
	luaDelete = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HDEL", KEYS[1], ARGV[1])
	   local msg = struct.pack("ic0", string.len(ARGV[1]), ARGV[1])
	   redis.call("PUBLISH", KEYS[2], "del:" .. msg)
	   return v
	`)

	// luaIncr is the Lua script used to increment a key and return the new value.
	luaIncr = redis.NewScript(`
	   redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(v), v)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return v
	`)

	// luaRemove is the Lua script used to remove items from an array value and
	// return the result along with a flag indicating if any value was removed.
	luaRemove = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   local removed = false

	   if v then
	      -- Create a set of current values
	      local curr = {}
	      for s in string.gmatch(v, "[^,]+") do
	         curr[s] = true
	      end

	      -- Remove specified values
	      for s in string.gmatch(ARGV[2], "[^,]+") do
	         if curr[s] then
	            curr[s] = nil
	            removed = true
	         end
	      end

	      -- Collect the remaining values
	      local newValues = {}
	      for key, _ in pairs(curr) do
	         table.insert(newValues, key)
	      end

	      -- Update the hash or delete the key if empty
	      if #newValues == 0 then
	         redis.call("HDEL", KEYS[1], ARGV[1])
	         local msg = struct.pack("ic0", string.len(ARGV[1]), ARGV[1])
	         redis.call("PUBLISH", KEYS[2], "del:" .. msg)
	         v = ""
	      else
	         v = table.concat(newValues, ",")
	         redis.call("HSET", KEYS[1], ARGV[1], v)
	         local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(v), v)
	         redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	      end
	   end

	   return {v, removed}
	`)

	// luaReset is the Lua script used to reset the map.
	luaReset = redis.NewScript(`
	   redis.call("DEL", KEYS[1])
	   redis.call("PUBLISH", KEYS[2], "reset:*")
	`)

	// luaSet is the Lua script used to set a key and return its previous value.  We
	// use Lua scripts to publish notifications "at the same time" and preserve the
	// order of operations (scripts are run atomically within Redis).
	luaSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
	   local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2])
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return v
	`)

	// luaTestAndDel is the Lua script used to delete a key if it has a specific value.
	luaTestAndDel = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HDEL", KEYS[1], ARGV[1])
	      local msg = struct.pack("ic0", string.len(ARGV[1]), ARGV[1])
	      redis.call("PUBLISH", KEYS[2], "del:" .. msg)
	   end
	   return v
	`)

	// luaTestAndReset is the Lua script used to reset the map if all the given keys
	// have the given values.
	luaTestAndReset = redis.NewScript(`
	  local hash = KEYS[1]
	  local n = (#ARGV - 1) / 2
	  
	  for i = 2, n + 1 do
	      if redis.call("HGET", hash, ARGV[i]) ~= ARGV[i + n] then
	          return 0
	      end
	  end
	  
	  redis.call("DEL", hash)
	  redis.call("PUBLISH", KEYS[2], "reset:*")
	  return 1
	`)

	// luaTestAndSet is the Lua script used to set a key if it has a specific value.
	luaTestAndSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
	      local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[3]), ARGV[3])
	      redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   end
	   return v
	`)

	// luaSetIfNotExists is the Lua script used to set a key if it does not exist.
	luaSetIfNotExists = redis.NewScript(`
        local v = redis.call("HGET", KEYS[1], ARGV[1])
        if not v then
            redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
            local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2])
            redis.call("PUBLISH", KEYS[2], "set:" .. msg)
            return 1  -- Successfully set the value
        end
        return 0    -- Value already existed
    `)
)
