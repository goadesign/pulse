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

	"goa.design/ponos/ponos"

	"github.com/redis/go-redis/v9"
)

type (
	// Map is a replicated map that emits events when elements
	// change. Multiple processes can join the same replicated map and
	// update it.
	Map struct {
		// C is the channel that receives notifications when the map
		// changes. The channel is closed when the map is closed.  This
		// channel simply notifies that the map has changed, it does not
		// provide the actual changes, instead the Map method should be
		// used to read the current content.  This allows the
		// notification to be sent without blocking. Multiple remote
		// updates may result in a single notification.
		C <-chan struct{}

		stopping bool // true if Stop was called
		stopped  bool // stopped is true once Stop finishes.
		name     string
		chankey  string                // Redis pubsub channel name
		hashkey  string                // Redis hash key
		msgch    <-chan *redis.Message // channel to receive map updates
		notifych chan struct{}         // channel to send notifications
		done     chan struct{}         // channel to signal shutdown
		wait     sync.WaitGroup        // wait for read goroutine to exit
		logger   ponos.Logger          // logger
		sub      *redis.PubSub         // subscription to map updates
		set      *redis.Script
		append   *redis.Script
		remove   *redis.Script
		incr     *redis.Script
		del      *redis.Script
		reset    *redis.Script
		rdb      *redis.Client
		lock     sync.Mutex
		content  map[string]string
	}
)

// luaSet is the Lua script used to set a key and return its previous value.  We
// use Lua scripts to publish notifications "at the same time" and preserve the
// order of operations (scripts are run atomically within Redis).
const luaSet = `
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
   redis.call("PUBLISH", KEYS[2], ARGV[1].."="..ARGV[2])
   return v
`

// luaDelete is the Lua script used to delete a key and return its previous
// value.
const luaDelete = `
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   redis.call("HDEL", KEYS[1], ARGV[1])
   redis.call("PUBLISH", KEYS[2], ARGV[1].."=")
   return v
`

// luaReset is the Lua script used to reset the map.
const luaReset = `
   redis.call("DEL", KEYS[1])
   redis.call("PUBLISH", KEYS[2], "*=")
`

// luaIncr is the Lua script used to increment a key and return the new value.
const luaIncr = `
   redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   redis.call("PUBLISH", KEYS[2], ARGV[1].."="..v)
   return v
`

// luaAppend is the Lua script used to append an item to an array key and
// return its new value.
const luaAppend = `
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   if v then
      v = v .. "," .. ARGV[2]
   else
      v = ARGV[2]
   end
   redis.call("HSET", KEYS[1], ARGV[1], v)
   redis.call("PUBLISH", KEYS[2], ARGV[1].."="..v)
   return v
`

// luaRemove is the Lua script used to remove an item from an array value and
// return the result.
const luaRemove = `
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   if v then
      local curr = {}
      for s in string.gmatch(v, "[^,]+") do
	 table.insert(curr, s)
      end
      local args = {}
      for s in string.gmatch(ARGV[2], "[^,]+") do
	 table.insert(args, s)
      end
      for _, s in ipairs(args) do
	 for i = #curr, 1, -1 do
	    if curr[i] == s then
	       table.remove(curr, i)
	    end
	 end
      end
      if #curr == 0 then
         v = ""
	 redis.call("HDEL", KEYS[1], ARGV[1])
      else
         v = table.concat(curr, ",")
         redis.call("HSET", KEYS[1], ARGV[1], v)
      end
      redis.call("PUBLISH", KEYS[2], ARGV[1].."="..v)
   end
   return v
`

// Join retrieves the content of the replicated map with the given name and
// subscribes to updates. The local content is eventually consistent across all
// nodes that join the replicated map with the same name.
//
// Clients can call the Map method on the returned Map to retrieve a copy of
// its content and subscribe to its C channel to receive updates when the
// content changes (note that multiple remote changes may result in a single
// notification). The returned Map is safe for concurrent use.
//
// Call Close to stop updates and release resources resulting in a read-only
// point-in-time copy.
func Join(ctx context.Context, name string, rdb *redis.Client, options ...MapOption) (*Map, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("ponos map: not a valid map name %q", name)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	opts := defaultOptions()
	for _, o := range options {
		o(opts)
	}
	c := make(chan struct{}, 1) // Buffer 1 notification so we don't have to block.
	sm := &Map{
		C:        c,
		name:     name,
		chankey:  fmt.Sprintf("ponos:map:%s:updates", name),
		hashkey:  fmt.Sprintf("ponos:map:%s:content", name),
		notifych: c,
		done:     make(chan struct{}),
		logger:   opts.Logger.WithPrefix("ponos:map", name),
		rdb:      rdb,
		set:      redis.NewScript(luaSet),
		incr:     redis.NewScript(luaIncr),
		append:   redis.NewScript(luaAppend),
		remove:   redis.NewScript(luaRemove),
		del:      redis.NewScript(luaDelete),
		reset:    redis.NewScript(luaReset),
		content:  make(map[string]string),
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

// Get returns the value for the given key.
func (sm *Map) Get(key string) (string, bool) {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	res, ok := sm.content[key]
	return res, ok
}

// Set sets the value for the given key and returns the previous value. An error
// is returned if the key is empty or contains an equal sign.
func (sm *Map) Set(ctx context.Context, key, value string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "set", sm.set, key, value)
	if err != nil {
		return "", err
	}
	if prev == nil {
		return "", nil
	}
	return prev.(string), nil
}

// Increment increments the value for the given key and returns the result, the
// value must represent an integer. An error is returned if the key is empty,
// contains an equal sign or if the value does not represent an integer.
func (sm *Map) Increment(ctx context.Context, key string, delta int) (int, error) {
	res, err := sm.runLuaScript(ctx, "incr", sm.incr, key, delta)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(res.(string), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ponos map: %s: %s", key, err)
	}
	return int(v), nil
}

// AppendValues appends the given items to the value for the given key and
// returns the result. The array of items is stored as a comma-separated list
// (so items should not have commas in them). An error is returned if the key is
// empty or contains an equal sign.
func (sm *Map) AppendValues(ctx context.Context, key string, items ...string) ([]string, error) {
	sitems := strings.Join(items, ",")
	res, err := sm.runLuaScript(ctx, "append", sm.append, key, sitems)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return strings.Split(res.(string), ","), nil
}

// RemoveValues removes the given items from the value for the given key and
// returns the result. The array of items is stored as a comma-separated list.
// If the removal results in an empty slice then the key is automatically
// deleted. An error is returned if key is empty or contains an equal sign.
func (sm *Map) RemoveValues(ctx context.Context, key string, items ...string) ([]string, error) {
	sitems := strings.Join(items, ",")
	res, err := sm.runLuaScript(ctx, "remove", sm.remove, key, sitems)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	if res.(string) == "" {
		return nil, nil
	}
	return strings.Split(res.(string), ","), nil
}

// Delete deletes the value for the given key and returns the previous value.
func (sm *Map) Delete(ctx context.Context, key string) (string, error) {
	prev, err := sm.runLuaScript(ctx, "delete", sm.del, key)
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
	_, err := sm.runLuaScript(ctx, "reset", sm.reset, "*")
	return err
}

// Stop closes the connection to the map, freeing resources. It is safe to
// call Stop multiple times.
func (sm *Map) Stop() error {
	sm.lock.Lock()
	if sm.stopping {
		return nil
	}
	sm.stopping = true
	close(sm.done)
	sm.lock.Unlock()
	sm.wait.Wait()
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.stopped = true
	return nil
}

// init initializes the map.
func (sm *Map) init(ctx context.Context) error {
	// Subscribe to updates.
	sm.sub = sm.rdb.Subscribe(ctx, sm.chankey)
	_, err := sm.sub.Receive(ctx) // Fail fast if we can't subscribe.
	if err != nil {
		return fmt.Errorf("ponos map: %s failed to join: %w", sm.name, err)
	}
	sm.msgch = sm.sub.Channel()

	// Make sure scripts are cached.
	for _, script := range []string{luaSet, luaDelete, luaReset, luaIncr, luaAppend, luaRemove} {
		if err := sm.rdb.ScriptLoad(ctx, script).Err(); err != nil {
			return fmt.Errorf("ponos map: %s failed to load Lua scripts %q: %w", sm.name, script, err)
		}
	}

	// read initial content
	cmd := sm.rdb.HGetAll(ctx, sm.hashkey)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("ponos map: %s failed to read initial content: %w", sm.name, err)
	}
	sm.content = cmd.Val()

	return nil
}

// run updates the local copy of the replicated map whenever a remote update is
// received and sends notifications when needed.
func (sm *Map) run() {
	defer func() {
		close(sm.notifych)
		sm.sub.Close()
		sm.wait.Done()
	}()
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
			switch {
			case key == "*":
				sm.doReset()
			case val == "":
				sm.doDelete(key)
			default:
				sm.doSet(key, val)
			}
			select {
			case sm.notifych <- struct{}{}:
			default:
			}
			sm.lock.Unlock()

		case <-sm.done:
			sm.logger.Info("closed")
			return
		}
	}
}

// runLuaScript runs the given Lua script, the furst argument must be the key.
func (sm *Map) runLuaScript(ctx context.Context, name string, script *redis.Script, args ...any) (any, error) {
	key := args[0].(string)
	if len(key) == 0 {
		return nil, fmt.Errorf("ponos map: %s key cannot be empty in %q", sm.name, name)
	}
	if strings.Contains(key, "=") {
		return nil, fmt.Errorf("ponos map: %s key %q cannot contain '=' in %q", sm.name, key, name)
	}
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.stopped {
		return nil, fmt.Errorf("ponos map: %s is closed", sm.name)
	}

	res, err := script.Eval(
		ctx,
		sm.rdb,
		[]string{sm.hashkey, sm.chankey},
		args...,
	).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("ponos map: %s failed to run %q for key %s: %w", sm.name, name, key, err)
	}

	return res, nil
}

// doReset resets the map content.
func (sm *Map) doReset() {
	sm.content = make(map[string]string)
	sm.logger.Info("reset")
}

// doDelete deletes the value for the given key.
func (sm *Map) doDelete(key string) {
	delete(sm.content, key)
	sm.logger.Info("deleted", "key", key)
}

// doSet sets the value for the given key.
func (sm *Map) doSet(key, val string) {
	sm.content[key] = val
	sm.logger.Info("set", key, val)
}

// reconnect attempts to reconnect to the Redis server forever.
func (sm *Map) reconnect() {
	var count int
	for {
		count++
		sm.logger.Info("reconnect", "attempt", count)
		sm.lock.Lock()
		if sm.stopping {
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
