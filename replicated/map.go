package replicated

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

		done     bool // true if context used to Join is canceled
		name     string
		chankey  string                // Redis pubsub channel name
		hashkey  string                // Redis hash key
		msgch    <-chan *redis.Message // channel to receive map updates
		notifych chan struct{}         // channel to send notifications
		logger   ponos.Logger          // logger
		sub      *redis.PubSub         // subscription to map updates
		set      *redis.Script
		append   *redis.Script
		incr     *redis.Script
		del      *redis.Script
		reset    *redis.Script
		rdb      *redis.Client
		lock     sync.Mutex
		content  map[string]string
	}
)

// luaSet is the Lua script used to set a key.  We use Lua scripts to publish
// notifications "at the same time" and preserve the order of operations
// (scripts are run atomically within Redis).
const luaSet = `
   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
   redis.call("PUBLISH", KEYS[2], ARGV[3])
`

// luaDelete is the Lua script used to delete a key.
const luaDelete = `
   redis.call("HDEL", KEYS[1], ARGV[1])
   redis.call("PUBLISH", KEYS[2], ARGV[2])
`

// luaReset is the Lua script used to reset the map.
const luaReset = `
   redis.call("DEL", KEYS[1])
   redis.call("PUBLISH", KEYS[2], "*=")
`

// luaIncr is the Lua script used to increment a value.
const luaIncr = `
   redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
   redis.call("PUBLISH", KEYS[2], ARGV[3])
`

// luaAppend is the Lua script used to append an item to an array value.
const luaAppend = `
   local v = redis.call("HGET", KEYS[1], ARGV[1])
   if v then
      v = v .. "," .. ARGV[2]
   else
      v = ARGV[2]
   end
   redis.call("HSET", KEYS[1], ARGV[1], v)
   redis.call("PUBLISH", KEYS[2], ARGV[3])
`

// Join retrieves the content of the replicated map with the given name and
// subscribes to updates. The local content is eventually consistent across all
// nodes that join the replicated map with the same name.
//
// Clients can call the Content method on the returned Map to retrieve a copy of
// its content and subscribe to its C channel to receive updates when the
// content changes (note that multiple remote changes may result in a single
// notification). The returned Map is safe for concurrent use.
//
// Cancel ctx to stop updates and release resources resulting in a read-only
// point-in-time copy.
func Join(ctx context.Context, name string, rdb *redis.Client, options ...MapOption) (*Map, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("ponos: invalid map name: %s", name)
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
		logger:   opts.Logger,
		rdb:      rdb,
		set:      redis.NewScript(luaSet),
		incr:     redis.NewScript(luaIncr),
		append:   redis.NewScript(luaAppend),
		del:      redis.NewScript(luaDelete),
		reset:    redis.NewScript(luaReset),
		content:  make(map[string]string),
	}
	if err := sm.init(ctx); err != nil {
		return nil, err
	}

	// read updates
	go sm.run(ctx)

	sm.logger.Info("ponos: map %s: joined", sm.name)
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

// Set sets the value for the given key. An error is returned if the key is
// empty or contains an equal sign.
func (sm *Map) Set(ctx context.Context, key, value string) error {
	return sm.runLuaScript(ctx, "set", sm.set, key, value, key+"="+value)
}

// Increment increments the value for the given key, the value must represent an
// integer. An error is returned if the key is empty or contains an equal sign.
// An error is logged if the value does not represent an integer.
// Use this method to avoud race conditions.
func (sm *Map) Increment(ctx context.Context, key string, delta int) error {
	return sm.runLuaScript(ctx, "incr", sm.incr, key, delta, key+"+="+strconv.Itoa(delta))
}

// Append appends the given items to the value for the given key. The array of
// items is stored as a comma-separated list (so items should not have commas
// in them). An error is returned if the key is empty or contains an equal sign.
func (sm *Map) Append(ctx context.Context, key string, items ...string) error {
	sitems := strings.Join(items, ",")
	return sm.runLuaScript(ctx, "append", sm.append, key, sitems, key+",="+sitems)
}

// Delete deletes the value for the given key.
func (sm *Map) Delete(ctx context.Context, key string) error {
	return sm.runLuaScript(ctx, "delete", sm.del, key, key+"=")
}

// Reset clears the map content.
func (sm *Map) Reset(ctx context.Context) error {
	return sm.runLuaScript(ctx, "reset", sm.reset, "*")
}

// init initializes the map.
func (sm *Map) init(ctx context.Context) error {
	// Subscribe to updates.
	sm.sub = sm.rdb.Subscribe(ctx, sm.chankey)
	_, err := sm.sub.Receive(ctx) // Fail fast if we can't subscribe.
	if err != nil {
		return fmt.Errorf("ponos: failed to join map %s: %w", sm.name, err)
	}
	sm.msgch = sm.sub.Channel()

	// Make sure scripts are cached.
	for _, script := range []string{luaSet, luaDelete, luaReset, luaIncr} {
		if err := sm.rdb.ScriptLoad(ctx, script).Err(); err != nil {
			return fmt.Errorf("ponos: failed to load Lua script for map %s: %w", sm.name, err)
		}
	}

	// read initial content
	cmd := sm.rdb.HGetAll(ctx, sm.hashkey)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("ponos: failed to read initial content for map %s: %w", sm.name, err)
	}
	sm.content = cmd.Val()

	return nil
}

// run updates the local copy of the replicated map whenever a remote update is
// received and sends notifications when needed.
func (sm *Map) run(ctx context.Context) {
	defer func() {
		sm.lock.Lock()
		sm.done = true
		sm.lock.Unlock()
		close(sm.notifych)
		sm.sub.Close()
	}()
	for {
		select {
		case msg, ok := <-sm.msgch:
			if !ok {
				// disconnected from Redis server, attempt to reconnect forever
				sm.reconnect(ctx)
				continue
			}
			parts := strings.SplitN(msg.Payload, "=", 2)
			if len(parts) != 2 {
				sm.logger.Error(fmt.Errorf("ponos: map %s: invalid message: %s", sm.name, msg.Payload))
				continue
			}
			key, val := parts[0], parts[1]
			sm.lock.Lock()
			switch {
			case key == "*":
				sm.doReset()
			case val == "":
				sm.doDelete(key)
			case strings.HasSuffix(key, "+"):
				sm.doIncrement(key[:len(key)-1], val)
			case strings.HasSuffix(key, ","):
				sm.doAppend(key[:len(key)-1], val)
			default:
				sm.doSet(key, val)
			}
			select {
			case sm.notifych <- struct{}{}:
			default:
			}
			sm.lock.Unlock()

		case <-ctx.Done():
			sm.logger.Info("ponos: map %s: closed", sm.name)
			return
		}
	}
}

// runLuaScript runs the given Lua script, the furst argument must be the key.
func (sm *Map) runLuaScript(ctx context.Context, name string, script *redis.Script, args ...any) error {
	key := args[0].(string)
	if err := validateKey(key); err != nil {
		return err
	}
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.done {
		return fmt.Errorf("ponos: map %s is closed", sm.name)
	}

	if err := script.Eval(
		ctx,
		sm.rdb,
		[]string{sm.hashkey, sm.chankey},
		args...,
	).Err(); err != nil && err != redis.Nil {
		return fmt.Errorf("ponos: map %s: failed to run script %q for key %s: %w", sm.name, name, key, err)
	}

	return nil
}

// doReset resets the map content.
func (sm *Map) doReset() {
	sm.content = make(map[string]string)
	sm.logger.Info("ponos: map %s: reset", sm.name)
}

// doDelete deletes the value for the given key.
func (sm *Map) doDelete(key string) {
	delete(sm.content, key)
	sm.logger.Info("ponos: map %s: %s deleted", sm.name, key)
}

// doIncrement increments the value for the given key.
func (sm *Map) doIncrement(key, delta string) {
	if _, ok := sm.content[key]; !ok {
		sm.content[key] = delta
		sm.logger.Info("ponos: map %s: %s=%s", sm.name, key, delta)
	} else {
		v, err := strconv.Atoi(sm.content[key])
		if err != nil {
			sm.logger.Error(fmt.Errorf("ponos: map %s: failed to increment %s (value: %s, delta: %s): %w", sm.name, key, sm.content[key], delta, err))
			return
		}
		d, err := strconv.Atoi(delta)
		if err != nil {
			sm.logger.Error(fmt.Errorf("ponos: map %s: failed to increment %s (value: %s, delta: %s): %w", sm.name, key, sm.content[key], delta, err))
			return
		}
		sm.content[key] = strconv.Itoa(v + d)
		sm.logger.Info("ponos: map %s: %s=%s", sm.name, key, sm.content[key])
	}
}

// doAppend appends the value for the given key.
func (sm *Map) doAppend(key, val string) {
	if _, ok := sm.content[key]; !ok {
		sm.content[key] = val
		sm.logger.Info("ponos: map %s: %s=%s", sm.name, key, val)
	} else {
		sm.content[key] += "," + val
		sm.logger.Info("ponos: map %s: %s=%s", sm.name, key, sm.content[key])
	}
}

// doSet sets the value for the given key.
func (sm *Map) doSet(key, val string) {
	sm.content[key] = val
	sm.logger.Info("ponos: map %s: %s=%s", sm.name, key, val)
}

// reconnect attempts to reconnect to the Redis server forever.
func (sm *Map) reconnect(ctx context.Context) {
	sm.logger.Error(fmt.Errorf("ponos: map %s: disconnected", sm.name))
	var count int
	for {
		count++
		sm.logger.Info("ponos: map %s: reconnect attempt %d", sm.name, count)
		if err := sm.init(ctx); err != nil {
			sm.logger.Error(fmt.Errorf("ponos: map %s: failed to reconnect: %w", sm.name, err))
			time.Sleep(time.Duration(rand.Float64()*5+1) * time.Second)
			continue
		}
		sm.logger.Info("ponos: map %s: reconnected", sm.name)
		break
	}
}

// redisKeyRegex is a regular expression that matches valid Redis keys.
var redisKeyRegex = regexp.MustCompile(`^[^ \0\*\?\[\]]{1,512}$`)

func isValidRedisKeyName(key string) bool {
	return redisKeyRegex.MatchString(key)
}

// validateKey returns an error if the key is empty or contains an equal sign.
func validateKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("ponos: invalid map key: %s (cannot be empty)", key)
	}
	if strings.Contains(key, "=") {
		return fmt.Errorf("ponos: invalid map key: %s (cannot contain '=')", key)
	}
	return nil
}
