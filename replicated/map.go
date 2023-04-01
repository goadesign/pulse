package replicated

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

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
		// notification to be sent without blocking.
		C <-chan struct{}

		chankey  string                // Redis pubsub channel name
		hashkey  string                // Redis hash key
		msgch    <-chan *redis.Message // channel to receive map updates from Redis
		notifych chan struct{}         // channel to send notifications to clients
		logger   ponos.Logger          // logger
		sub      *redis.PubSub         // subscription to map updates
		rdb      *redis.Client
		set      *redis.Script
		del      *redis.Script
		reset    *redis.Script
		lock     sync.Mutex
		content  map[string]string
	}
)

// luaSet is the Lua script used to set a key.
// We use a script to publish a notification "at the same time".
const luaSet = `
   redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
   redis.call("PUBLISH", KEYS[2], ARGV[3])
`

// luaDelete is the Lua script used to delete a key.
// We use a script to publish a notification "at the same time".
const luaDelete = `
   redis.call("HDEL", KEYS[1], ARGV[1])
   redis.call("PUBLISH", KEYS[2], ARGV[2])
`

// luaReset is the Lua script used to reset the map.
// We use a script to publish a notification "at the same time".
const luaReset = `
   redis.call("DEL", KEYS[1])
   redis.call("PUBLISH", KEYS[2], "*=")
`

// Join joins the shared map with the given name. It guarantees that the local
// copy of the map is initialized with the latest content or else a notification
// is sent to the notification channel (the Map method can be used to retrieve
// the current content).
// Cancel ctx to stop updates and release resources.
func Join(ctx context.Context, name string, rdb *redis.Client, options ...MapOption) (*Map, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("invalid map name: %q", name)
	}
	opts := defaultOptions()
	for _, o := range options {
		o(opts)
	}
	c := make(chan struct{}, 1) // Buffer 1 notification so we don't have to block.
	sm := &Map{
		C:        c,
		chankey:  fmt.Sprintf("ponos:map:%s:updates", name),
		hashkey:  fmt.Sprintf("ponos:map:%s:content", name),
		notifych: c,
		logger:   opts.Logger,
		rdb:      rdb,
		set:      redis.NewScript(luaSet),
		del:      redis.NewScript(luaDelete),
		reset:    redis.NewScript(luaReset),
		content:  make(map[string]string),
	}
	if err := sm.init(ctx); err != nil {
		return nil, err
	}

	// read updates
	go sm.run(ctx)

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
	if len(key) == 0 {
		return fmt.Errorf("invalid key: %q (cannot be empty)", key)
	}
	if strings.Contains(key, "=") {
		return fmt.Errorf("invalid key: %q (cannot contain '=')", key)
	}
	sm.lock.Lock()
	defer sm.lock.Unlock()

	if err := sm.set.Eval(
		ctx,
		sm.rdb,
		[]string{sm.hashkey, sm.chankey},
		key, value, key+"="+value,
	).Err(); err != nil && err != redis.Nil {
		return fmt.Errorf("failed to set value: %w", err)
	}
	sm.content[key] = value

	return nil
}

// Delete deletes the value for the given key.
func (sm *Map) Delete(ctx context.Context, key string) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	if err := sm.del.Eval(
		ctx,
		sm.rdb,
		[]string{sm.hashkey, sm.chankey},
		key, key+"=",
	).Err(); err != nil && err != redis.Nil {
		return fmt.Errorf("failed to delete value: %w", err)
	}
	delete(sm.content, key)

	return nil
}

// Reset clears the map content.
func (sm *Map) Reset(ctx context.Context) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	if err := sm.reset.Eval(
		ctx,
		sm.rdb,
		[]string{sm.hashkey, sm.chankey},
	).Err(); err != nil && err != redis.Nil {
		return fmt.Errorf("failed to reset map: %w", err)
	}
	sm.content = make(map[string]string)

	return nil
}

// init initializes the map.
func (sm *Map) init(ctx context.Context) error {
	// Subscribe to updates.
	sm.sub = sm.rdb.Subscribe(ctx, sm.chankey)
	_, err := sm.sub.Receive(ctx) // Fail fast if we can't subscribe.
	if err != nil {
		return fmt.Errorf("failed to join replicated map: %w", err)
	}
	sm.msgch = sm.sub.Channel()

	// Make sure scripts are cached.
	if err := sm.rdb.ScriptLoad(ctx, luaSet).Err(); err != nil {
		return fmt.Errorf("failed to load Lua Set script: %w", err)
	}
	if err := sm.rdb.ScriptLoad(ctx, luaDelete).Err(); err != nil {
		return fmt.Errorf("failed to load Lua Delete script: %w", err)
	}
	if err := sm.rdb.ScriptLoad(ctx, luaReset).Err(); err != nil {
		return fmt.Errorf("failed to load Lua Reset script: %w", err)
	}

	// read initial content
	cmd := sm.rdb.HGetAll(ctx, sm.hashkey)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("failed to read initial content: %w", err)
	}
	sm.content = cmd.Val()

	return nil
}

// run updates the local copy of the replicated map whenever a remote update is
// received and sends notifications when needed.
func (sm *Map) run(ctx context.Context) {
	defer func() {
		close(sm.notifych)
		sm.sub.Close()
	}()
	for {
		select {
		case msg, ok := <-sm.msgch:
			if !ok {
				return
			}
			parts := strings.SplitN(msg.Payload, "=", 2)
			if len(parts) != 2 {
				sm.logger.Error(fmt.Errorf("%q: invalid message: %q", sm.hashkey, msg.Payload))
				continue
			}
			key, val := parts[0], parts[1]
			sm.lock.Lock()
			if key == "*" {
				// reset
				sm.content = make(map[string]string)
				sm.logger.Info("%q: reset", sm.hashkey)
				sm.lock.Unlock()
				continue
			}
			if val == "" {
				delete(sm.content, key)
				sm.logger.Info("%q: deleted key %q", sm.hashkey, key)
			} else {
				sm.content[key] = val
				sm.logger.Info("%q: %q=%q", sm.hashkey, key, val)
			}
			select {
			// Non-blocking send.
			case sm.notifych <- struct{}{}:
			default:
			}
			sm.lock.Unlock()

		case <-ctx.Done():
			sm.logger.Info("%q: context canceled, stopping", sm.hashkey)
			return
		}
	}
}

var redisKeyRegex = regexp.MustCompile(`^[^ \0\*\?\[\]]{1,512}$`)

func isValidRedisKeyName(key string) bool {
	return redisKeyRegex.MatchString(key)
}
