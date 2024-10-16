package rmap

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"goa.design/pulse/pulse"

	"github.com/redis/go-redis/v9"
)

type (
	// Map is a replicated map that emits events when elements
	// change. Multiple processes can join the same replicated map and
	// update it.
	Map struct {
		Name               string
		closing            bool                  // true if Close was called
		closed             bool                  // stopped is true once Close finishes.
		chankey            string                // Redis pubsub channel name
		hashkey            string                // Redis hash key
		msgch              <-chan *redis.Message // channel to receive map updates
		chans              []chan EventKind      // channels to send notifications
		ichan              chan struct{}         // internal channel to send notifications
		done               chan struct{}         // channel to signal shutdown
		wait               sync.WaitGroup        // wait for read goroutine to exit
		logger             pulse.Logger          // logger
		sub                *redis.PubSub         // subscription to map updates
		rdb                *redis.Client
		lock               sync.Mutex
		content            map[string]string
		setScript          *redis.Script
		testAndSetScript   *redis.Script
		appendScript       *redis.Script
		appendUniqueScript *redis.Script
		removeScript       *redis.Script
		incrScript         *redis.Script
		delScript          *redis.Script
		testAndDelScript   *redis.Script
		testAndResetScript *redis.Script
		resetScript        *redis.Script
	}

	// EventKind is the type of map event.
	EventKind int
)

const (
	// EventChange is the event emitted when a key is added, changed or deleted.
	EventChange EventKind = iota + 1
	// EventReset is the event emitted when the map is reset.
	EventReset
)

var (
	// luaAppend is the Lua script used to append an item to an array key and
	// return its new value.
	luaAppend = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])

	   -- If the value exists, append the new value, otherwise assign ARGV[2] directly
	   v = (v and v .. "," .. ARGV[2]) or ARGV[2]

	   -- Set the updated value in the hash and publish the change
	   redis.call("HSET", KEYS[1], ARGV[1], v)
	   redis.call("PUBLISH", KEYS[2], ARGV[1] .. "=" .. v)

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
	    redis.call("PUBLISH", KEYS[2], ARGV[1] .. "=" .. v)
	  end

	  return v
	`)

	// luaDelete is the Lua script used to delete a key and return its previous
	// value.
	luaDelete = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HDEL", KEYS[1], ARGV[1])
	   redis.call("PUBLISH", KEYS[2], ARGV[1].."=")
	   return v
	`)

	// luaIncr is the Lua script used to increment a key and return the new value.
	luaIncr = redis.NewScript(`
	   redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("PUBLISH", KEYS[2], ARGV[1].."="..v)
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
	         v = ""
	      else
	         v = table.concat(newValues, ",")
	         redis.call("HSET", KEYS[1], ARGV[1], v)
	      end

	      -- Publish the result
	      redis.call("PUBLISH", KEYS[2], ARGV[1] .. "=" .. v)
	   end

	   return {v, removed}
	`)

	// luaReset is the Lua script used to reset the map.
	luaReset = redis.NewScript(`
	   redis.call("DEL", KEYS[1])
	   redis.call("PUBLISH", KEYS[2], "*=")
	`)

	// luaSet is the Lua script used to set a key and return its previous value.  We
	// use Lua scripts to publish notifications "at the same time" and preserve the
	// order of operations (scripts are run atomically within Redis).
	luaSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
	   redis.call("PUBLISH", KEYS[2], ARGV[1].."="..ARGV[2])
	   return v
	`)

	// luaTestAndDel is the Lua script used to delete a key if it has a specific value.
	luaTestAndDel = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HDEL", KEYS[1], ARGV[1])
	      redis.call("PUBLISH", KEYS[2], ARGV[1].."=")
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
	  redis.call("PUBLISH", KEYS[2], "*=")
	  return 1
	`)

	// luaTestAndSet is the Lua script used to set a key if it has a specific value.
	luaTestAndSet = redis.NewScript(`
	   local v = redis.call("HGET", KEYS[1], ARGV[1])
	   if v == ARGV[2] then
	      redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
	      redis.call("PUBLISH", KEYS[2], ARGV[1].."="..ARGV[3])
	   end
	   return v
	`)
)

// Join retrieves the content of the replicated map with the given name and
// subscribes to updates. The local content is eventually consistent across all
// nodes that join the replicated map with the same name.
//
// Clients can call the Map method on the returned Map to retrieve a copy of
// its content and subscribe to its C channel to receive updates when the
// content changes (note that multiple remote changes may result in a single
// notification). The returned Map is safe for concurrent use.
//
// Clients should call Close before exiting to stop updates and release
// resources resulting in a read-only point-in-time copy.
func Join(ctx context.Context, name string, rdb *redis.Client, opts ...MapOption) (*Map, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("pulse map: not a valid map name %q", name)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	o := parseOptions(opts...)
	sm := &Map{
		Name:               name,
		chankey:            fmt.Sprintf("map:%s:updates", name),
		hashkey:            fmt.Sprintf("map:%s:content", name),
		ichan:              make(chan struct{}, 1),
		done:               make(chan struct{}),
		logger:             o.Logger.WithPrefix("map", name),
		rdb:                rdb,
		content:            make(map[string]string),
		setScript:          luaSet,
		testAndSetScript:   luaTestAndSet,
		incrScript:         luaIncr,
		appendScript:       luaAppend,
		appendUniqueScript: luaAppendUnique,
		removeScript:       luaRemove,
		delScript:          luaDelete,
		testAndDelScript:   luaTestAndDel,
		testAndResetScript: luaTestAndReset,
		resetScript:        luaReset,
	}
	if err := sm.init(ctx); err != nil {
		return nil, err
	}

	// read updates
	sm.wait.Add(1)
	go sm.run()

	sm.logger.Info("joined")
	return sm, nil
}

// Map returns a copy of the replicated map content.
func (sm *Map) Map() map[string]string {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	hash := make(map[string]string, len(sm.content))
	for k, v := range sm.content {
		hash[k] = v
	}
	return hash
}

// Keys returns a copy of the replicated map keys.
func (sm *Map) Keys() []string {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	keys := make([]string, 0, len(sm.content))
	for k := range sm.content {
		keys = append(keys, k)
	}
	return keys
}

// Subscribe returns a channel that receives notifications when the map
// changes. The channel is closed when the map is stopped. This channel simply
// notifies that the map has changed, it does not provide the actual changes,
// instead the Map method should be used to read the current content.  This
// allows the notification to be sent without blocking. Multiple remote updates
// may result in a single notification.
// Subscribe returns nil if the map is stopped.
func (sm *Map) Subscribe() <-chan EventKind {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.closing {
		return nil
	}
	c := make(chan EventKind, 1) // Buffer 1 notification so we don't have to block.
	sm.chans = append(sm.chans, c)
	return c
}

// Unsubscribe removes the given channel from the list of subscribers and closes it.
func (sm *Map) Unsubscribe(c <-chan EventKind) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.closing {
		return
	}
	for i, ch := range sm.chans {
		if ch == c {
			close(sm.chans[i])
			sm.chans = append(sm.chans[:i], sm.chans[i+1:]...)
			return
		}
	}
}

// Len returns the number of items in the replicated map.
func (sm *Map) Len() int {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	return len(sm.content)
}

// Get returns the value for the given key.
func (sm *Map) Get(key string) (string, bool) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	res, ok := sm.content[key]
	return res, ok
}

// GetValues returns the comma separated values for the given key.
// This is a convenience method intended to be used in conjunction with
// AppendValues and RemoveValues.
func (sm *Map) GetValues(key string) ([]string, bool) {
	val, ok := sm.Get(key)
	if !ok {
		return nil, false
	}
	return strings.Split(val, ","), true
}

// Set sets the value for the given key and returns the previous value.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// Set(ctx, "color", "blue") would set the "color" key to "blue"
// and return the previous value, if any.
func (sm *Map) Set(ctx context.Context, key, value string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "set", sm.setScript, key, value)
	if err != nil {
		return "", err
	}
	if prev == nil {
		return "", nil
	}
	return prev.(string), nil
}

// SetAndWait is a convenience method that calls Set and then waits for the
// update to be applied and the notification to be sent.
func (sm *Map) SetAndWait(ctx context.Context, key, value string) (string, error) {
	prev, err := sm.Set(ctx, key, value)
	if err != nil {
		return "", err
	}
	// Wait for the update to be applied.
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case _, ok := <-sm.ichan:
			if !ok {
				return "", fmt.Errorf("pulse map: %s is stopped", sm.Name)
			}
			sm.lock.Lock()
			v, ok := sm.content[key]
			sm.lock.Unlock()
			if ok && v == value {
				return prev, nil
			}
		}
	}
}

// TestAndSet sets the value for the given key if the current value matches the
// given test value. The previous value is returned.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// TestAndSet(ctx, "color", "red", "blue") would set "color" to "blue"
// only if its current value is "red", and return the previous value.
func (sm *Map) TestAndSet(ctx context.Context, key, test, value string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "testAndSet", sm.testAndSetScript, key, test, value)
	if err != nil {
		return "", err
	}
	if prev == nil {
		return "", nil
	}
	return prev.(string), nil
}

// Inc increments the value for the given key and returns the result.
// The value must represent an integer.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - The value does not represent an integer
// - There's an issue with the Redis operation
//
// Example:
// Inc(ctx, "counter", 1) would increment the "counter" by 1
// and return the new value.
func (sm *Map) Inc(ctx context.Context, key string, delta int) (int, error) {
	res, err := sm.runLuaScript(ctx, "incr", sm.incrScript, key, delta)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(res.(string), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("pulse map: %s: %s", key, err)
	}
	return int(v), nil
}

// AppendValues appends the given items to the value for the given key and
// returns the result. The array of items is stored as a comma-separated list.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// AppendValues(ctx, "fruits", "apple", "banana") would append "apple" and "banana"
// to the existing list of fruits and return the updated list.
func (sm *Map) AppendValues(ctx context.Context, key string, items ...string) ([]string, error) {
	sitems := strings.Join(items, ",")
	res, err := sm.runLuaScript(ctx, "append", sm.appendScript, key, sitems)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return strings.Split(res.(string), ","), nil
}

// AppendUniqueValues appends the given items to the value for the given key if
// they are not already present and returns the result. The array of items is
// stored as a comma-separated list.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// AppendUniqueValues(ctx, "fruits", "apple", "banana") would append only unique values
// to the existing list of fruits and return the updated list.
func (sm *Map) AppendUniqueValues(ctx context.Context, key string, items ...string) ([]string, error) {
	sitems := strings.Join(items, ",")
	res, err := sm.runLuaScript(ctx, "appendUnique", sm.appendUniqueScript, key, sitems)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return strings.Split(res.(string), ","), nil
}

// RemoveValues removes the given items from the value for the given key and
// returns the remaining values after removal. The function behaves as follows:
//
//  1. The value for the key is expected to be a comma-separated list of items.
//  2. It removes all occurrences of the specified items from this list.
//  3. If the removal results in an empty list, the key is automatically deleted.
//  4. Returns the remaining items as a slice of strings, a boolean indicating
//     whether any value was removed, and an error (if any).
//  5. If the key doesn't exist, it returns nil, false, nil.
//
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// Given a key "fruits" with value "apple,banana,cherry,apple"
// RemoveValues(ctx, "fruits", "apple", "cherry") would return (["banana"], true, nil)
// and update the value in Redis to "banana"
func (sm *Map) RemoveValues(ctx context.Context, key string, items ...string) ([]string, bool, error) {
	sitems := strings.Join(items, ",")
	res, err := sm.runLuaScript(ctx, "remove", sm.removeScript, key, sitems)
	if err != nil {
		return nil, false, err
	}
	result := res.([]any)
	if result[0] == nil {
		return nil, false, nil // Key didn't exist
	}
	remaining := result[0].(string)
	if remaining == "" {
		return nil, true, nil // All items were removed, key was deleted
	}
	removed := result[1].(int64) == 1
	return strings.Split(remaining, ","), removed, nil
}

// Delete deletes the value for the given key and returns the previous value.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// Delete(ctx, "color") would delete the "color" key and return its previous value, if any.
func (sm *Map) Delete(ctx context.Context, key string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "delete", sm.delScript, key)
	if err != nil {
		return "", err
	}
	if prev == nil {
		return "", nil
	}
	return prev.(string), nil
}

// TestAndDelete tests that the value for the given key matches the test value
// and deletes the key if it does. It returns the previous value.
// An error is returned if:
// - The key is empty
// - The key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// TestAndDelete(ctx, "color", "blue") would delete the "color" key only if
// its current value is "blue", and return the previous value.
func (sm *Map) TestAndDelete(ctx context.Context, key, test string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "testAndDelete", sm.testAndDelScript, key, test)
	if err != nil {
		return "", err
	}
	if prev == nil {
		return "", nil
	}
	return prev.(string), nil
}

// Reset clears the map content.
func (sm *Map) Reset(ctx context.Context) error {
	_, err := sm.runLuaScript(ctx, "reset", sm.resetScript, "*")
	return err
}

// TestAndReset tests that the values for the given keys match the test values
// and clears the map if they do. It returns true if the map was cleared, false otherwise.
// An error is returned if:
// - Any key is empty
// - Any key contains an equal sign
// - There's an issue with the Redis operation
//
// Example:
// TestAndReset(ctx, []string{"color", "size"}, []string{"blue", "large"}) would clear the map
// only if the "color" key has value "blue" and the "size" key has value "large",
// and return true if the map was cleared.
func (sm *Map) TestAndReset(ctx context.Context, keys, tests []string) (bool, error) {
	args := make([]any, 1+len(keys)+len(tests))
	args[0] = "*"
	for i, k := range keys {
		args[i+1] = k
	}
	for i, t := range tests {
		args[len(keys)+i+1] = t
	}
	res, err := sm.runLuaScript(ctx, "testAndReset", sm.testAndResetScript, args...)
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

// Close closes the connection to the map, freeing resources. It is safe to
// call Close multiple times.
func (sm *Map) Close() {
	sm.lock.Lock()
	if sm.closing {
		return
	}
	sm.closing = true
	close(sm.done)
	sm.lock.Unlock()
	sm.wait.Wait()
	sm.lock.Lock()
	defer sm.lock.Unlock()
	close(sm.ichan)
	sm.closed = true
}

// init initializes the map.
func (sm *Map) init(ctx context.Context) error {
	// Make sure scripts are cached.
	for _, script := range []*redis.Script{
		sm.appendScript,
		sm.appendUniqueScript,
		sm.delScript,
		sm.incrScript,
		sm.removeScript,
		sm.resetScript,
		sm.setScript,
		sm.testAndDelScript,
		sm.testAndResetScript,
		sm.testAndSetScript,
	} {
		if err := script.Load(ctx, sm.rdb).Err(); err != nil {
			return fmt.Errorf("pulse map: %s failed to load Lua scripts %v: %w", sm.Name, script, err)
		}
	}

	// Subscribe to updates.
	sm.sub = sm.rdb.Subscribe(ctx, sm.chankey)
	_, err := sm.sub.Receive(ctx) // Fail fast if we can't subscribe.
	if err != nil {
		return fmt.Errorf("pulse map: %s failed to join: %w", sm.Name, err)
	}
	sm.msgch = sm.sub.Channel()

	// read initial content
	// Note: there's a (very) small window where we might be receiving
	// updates for changes that are already applied by the time we call
	// HGetAll. This is not a problem because we'll just overwrite the
	// local copy with the same data.
	cmd := sm.rdb.HGetAll(ctx, sm.hashkey)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("pulse map: %s failed to read initial content: %w", sm.Name, err)
	}
	sm.content = cmd.Val()

	return nil
}

// run updates the local copy of the replicated map whenever a remote update is
// received and sends notifications when needed.
func (sm *Map) run() {
	for {
		select {
		case msg, ok := <-sm.msgch:
			if !ok {
				// disconnected from Redis server, attempt to reconnect forever
				sm.logger.Error(fmt.Errorf("disconnected"))
				sm.reconnect()
				continue
			}
			parts := strings.SplitN(msg.Payload, "=", 2)
			if len(parts) != 2 {
				sm.logger.Error(fmt.Errorf("invalid payload"), "payload", msg.Payload)
				continue
			}
			key, val := parts[0], parts[1]
			sm.lock.Lock()
			kind := EventChange
			switch {
			case key == "*":
				sm.content = make(map[string]string)
				sm.logger.Debug("reset")
				kind = EventReset
			case val == "":
				delete(sm.content, key)
				sm.logger.Debug("deleted", "key", key)
			default:
				sm.content[key] = val
				sm.logger.Debug("set", "key", key, "val", val)
			}
			select {
			case sm.ichan <- struct{}{}:
			default:
			}
			for _, c := range sm.chans {
				select {
				case c <- kind:
				default:
				}
			}
			sm.lock.Unlock()

		case <-sm.done:
			sm.logger.Info("stopped")
			// no need to lock, stopping is true
			for _, c := range sm.chans {
				close(c)
			}
			if err := sm.sub.Unsubscribe(context.Background(), sm.chankey); err != nil {
				sm.logger.Error(fmt.Errorf("failed to unsubscribe: %w", err))
			}
			if err := sm.sub.Close(); err != nil {
				sm.logger.Error(fmt.Errorf("failed to close subscription: %w", err))
			}
			sm.wait.Done()
			return
		}
	}
}

// runLuaScript runs the given Lua script, the first argument must be the key.
// It is the caller's responsibility to make sure the map is locked.
func (sm *Map) runLuaScript(ctx context.Context, name string, script *redis.Script, args ...any) (any, error) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.closing {
		return "", fmt.Errorf("pulse map: %s is stopped", sm.Name)
	}
	key := args[0].(string)
	if len(key) == 0 {
		return nil, fmt.Errorf("pulse map: %s key cannot be empty in %q", sm.Name, name)
	}
	if strings.Contains(key, "=") {
		return nil, fmt.Errorf("pulse map: %s key %q cannot contain '=' in %q", sm.Name, key, name)
	}
	res, err := script.EvalSha(ctx, sm.rdb, []string{sm.hashkey, sm.chankey}, args...).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("pulse map: %s failed to run %q for key %s: %w", sm.Name, name, key, err)
	}

	return res, nil
}

// reconnect attempts to reconnect to the Redis server forever.
func (sm *Map) reconnect() {
	var count int
	for {
		count++
		sm.logger.Info("reconnect", "attempt", count)
		sm.lock.Lock()
		if sm.closing {
			sm.lock.Unlock()
			return
		}
		sm.sub = sm.rdb.Subscribe(context.Background(), sm.chankey)
		_, err := sm.sub.Receive(context.Background())
		if err != nil {
			sm.lock.Unlock()
			sm.logger.Error(fmt.Errorf("failed to reconnect: %w", err), "attempt", count)
			time.Sleep(time.Duration(rand.Float64()*5+1) * time.Second)
			continue
		}
		sm.msgch = sm.sub.Channel()
		sm.lock.Unlock()
		sm.logger.Info("reconnected")
		break
	}
}

// redisKeyRegex is a regular expression that matches valid Redis keys.
var redisKeyRegex = regexp.MustCompile(`^[^ \0\*\?\[\]]{1,512}$`)

func isValidRedisKeyName(key string) bool {
	return redisKeyRegex.MatchString(key)
}
