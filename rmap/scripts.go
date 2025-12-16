package rmap

import "github.com/redis/go-redis/v9"

var (
	// luaAppend is the Lua script used to append an item to an array key and
	// return its new value.
	luaAppend = redis.NewScript(`
	   local key = ARGV[1]
	   local v = redis.call("HGET", KEYS[1], key)

	   if #ARGV == 1 then
	      return v
	   end

	   local values = {}
	   if v then
	      local ok, decoded = pcall(cjson.decode, v)
	      if ok and type(decoded) == "table" then
	         values = decoded
	      else
	         for s in string.gmatch(v, "[^,]+") do
	            table.insert(values, s)
	         end
	      end
	   end

	   for i = 2, #ARGV do
	      table.insert(values, ARGV[i])
	   end

	   local encoded = cjson.encode(values)
	   redis.call("HSET", KEYS[1], key, encoded)
	   local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	   local msg = struct.pack("ic0ic0ic0", string.len(key), key, string.len(encoded), encoded, string.len(rev), rev)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return encoded
	`)

	// luaAppendUnique is the Lua script used to append an item to a set and return
	// the result.
	luaAppendUnique = redis.NewScript(`
	  local key = ARGV[1]
	  local v = redis.call("HGET", KEYS[1], key)

	  if #ARGV == 1 then
	     return v
	  end

	  local values = {}
	  if v then
	     local ok, decoded = pcall(cjson.decode, v)
	     if ok and type(decoded) == "table" then
	        values = decoded
	     else
	        for s in string.gmatch(v, "[^,]+") do
	           table.insert(values, s)
	        end
	     end
	  end

	  local present = {}
	  for _, item in ipairs(values) do
	     present[item] = true
	  end

	  local changed = false
	  for i = 2, #ARGV do
	     local item = ARGV[i]
	     if not present[item] then
	        table.insert(values, item)
	        present[item] = true
	        changed = true
	     end
	  end

	  if changed then
	     local encoded = cjson.encode(values)
	     redis.call("HSET", KEYS[1], key, encoded)
	     local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	     local msg = struct.pack("ic0ic0ic0", string.len(key), key, string.len(encoded), encoded, string.len(rev), rev)
	     redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	     return encoded
	  end

	  return v
	`)

	// luaDelete is the Lua script used to delete a key and return its previous
	// value.
	luaDelete = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HDEL", KEYS[1], ARGV[1])
	   local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	   local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(rev), rev)
	   redis.call("PUBLISH", KEYS[2], "del:" .. msg)
	   return v
	`)

	// luaIncr is the Lua script used to increment a key and return the new value.
	luaIncr = redis.NewScript(`
	   redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	   local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(v), v, string.len(rev), rev)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return v
	`)

	// luaRemove is the Lua script used to remove items from an array value and
	// return the result along with a flag indicating if any value was removed.
	luaRemove = redis.NewScript(`
	   local key = ARGV[1]
	   local v = redis.call("HGET", KEYS[1], key)

	   if not v or #ARGV == 1 then
	      return {v, 0}
	   end

	   local values = {}
	   local ok, decoded = pcall(cjson.decode, v)
	   if ok and type(decoded) == "table" then
	      values = decoded
	   else
	      for s in string.gmatch(v, "[^,]+") do
	         table.insert(values, s)
	      end
	   end

	   local toRemove = {}
	   for i = 2, #ARGV do
	      toRemove[ARGV[i]] = true
	   end

	   local removed = 0
	   local remaining = {}
	   for _, item in ipairs(values) do
	      if toRemove[item] then
	         removed = 1
	      else
	         table.insert(remaining, item)
	      end
	   end

	   if removed == 0 then
	      return {v, 0}
	   end

	   if #remaining == 0 then
	      redis.call("HDEL", KEYS[1], key)
	      local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	      local msg = struct.pack("ic0ic0", string.len(key), key, string.len(rev), rev)
	      redis.call("PUBLISH", KEYS[2], "del:" .. msg)
	      return {"", 1}
	   end

	   local encoded = cjson.encode(remaining)
	   redis.call("HSET", KEYS[1], key, encoded)
	   local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	   local msg = struct.pack("ic0ic0ic0", string.len(key), key, string.len(encoded), encoded, string.len(rev), rev)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return {encoded, 1}
	`)

	// luaReset is the Lua script used to reset the map.
	luaReset = redis.NewScript(`
	   local rev = redis.call("HINCRBY", KEYS[1], "=rev", 1)
	   redis.call("DEL", KEYS[1])
	   redis.call("HSET", KEYS[1], "=rev", rev)
	   redis.call("PUBLISH", KEYS[2], "reset:" .. tostring(rev))
	`)

	// luaDestroy is the Lua script used to delete the map entirely.
	luaDestroy = redis.NewScript(`
	   redis.call("DEL", KEYS[1])
	   redis.call("PUBLISH", KEYS[2], "reset:*")
	`)

	// luaSet is the Lua script used to set a key and return its previous value.  We
	// use Lua scripts to publish notifications "at the same time" and preserve the
	// order of operations (scripts are run atomically within Redis).
	luaSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
	   local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	   local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2], string.len(rev), rev)
	   redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   return v
	`)

	// luaTestAndDel is the Lua script used to delete a key if it has a specific value.
	luaTestAndDel = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HDEL", KEYS[1], ARGV[1])
	      local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	      local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(rev), rev)
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
	  local rev = redis.call("HINCRBY", hash, "=rev", 1)
	  redis.call("DEL", hash)
	  redis.call("HSET", hash, "=rev", rev)
	  redis.call("PUBLISH", KEYS[2], "reset:" .. tostring(rev))
	  return 1
	`)

	// luaTestAndSet is the Lua script used to set a key if it has a specific value.
	luaTestAndSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
	      local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
	      local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[3]), ARGV[3], string.len(rev), rev)
	      redis.call("PUBLISH", KEYS[2], "set:" .. msg)
	   end
	   return v
	`)

	// luaSetIfNotExists is the Lua script used to set a key if it does not exist.
	luaSetIfNotExists = redis.NewScript(`
        local v = redis.call("HGET", KEYS[1], ARGV[1])
        if not v then
            redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
            local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
            local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2], string.len(rev), rev)
            redis.call("PUBLISH", KEYS[2], "set:" .. msg)
            return 1  -- Successfully set the value
        end
        return 0    -- Value already existed
    `)
)
