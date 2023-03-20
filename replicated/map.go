package replicated

import (
	"context"
	"fmt"
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

		name     string                // name of replicated map
		msgch    <-chan *redis.Message // channel to receive map updates from Redis
		notifych chan struct{}         // channel to send notifications to clients
		logger   ponos.Logger          // logger
		sub      *redis.PubSub         // subscription to map updates
		rdb      *redis.Client
		lock     sync.Mutex
		content  map[string]string
	}
)

// Join joins the shared map with the given name. It guarantees that the local
// copy of the map is initialized with the latest content or else a notification
// is sent to the notification channel (the Map method can be used to retrieve
// the current content).
// Cancel ctx to stop updates and release resources.
func Join(ctx context.Context, name string, rdb *redis.Client, options ...MapOption) (*Map, error) {
	opts := defaultOptions()
	for _, o := range options {
		o(opts)
	}
	c := make(chan struct{}, 1) // Buffer 1 notification so we don't have to block.
	sm := &Map{
		C:        c,
		name:     name,
		notifych: c,
		logger:   opts.Logger,
		rdb:      rdb,
		content:  make(map[string]string),
	}
	sm.sub = rdb.Subscribe(ctx, sm.channelName())
	_, err := sm.sub.Receive(ctx) // Fail fast if we can't subscribe.
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map: %w", err)
	}
	sm.msgch = sm.sub.Channel()

	// read initial content
	cmd := rdb.HGetAll(ctx, sm.hashName())
	if err := cmd.Err(); err != nil {
		return nil, fmt.Errorf("failed to read initial content: %w", err)
	}
	sm.content = cmd.Val()

	// read updates
	go sm.run(ctx)

	return sm, nil
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

	set := func(current string, pipe redis.Pipeliner) error {
		if err := pipe.HSet(ctx, sm.hashName(), key, value).Err(); err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}
		if err := sm.rdb.Publish(ctx, sm.channelName(), key+"="+value).Err(); err != nil {
			var err2 error
			if current != "" {
				err2 = sm.rdb.HSet(ctx, sm.hashName(), key, current).Err()
			} else {
				err2 = sm.rdb.HDel(ctx, sm.hashName(), key).Err()
			}
			suffix := ""
			if err2 != nil {
				suffix = fmt.Sprintf(" (failed to restore previous value: %v)", err2)
			}
			return fmt.Errorf("failed to publish update: %w%s", err, suffix)
		}
		return nil
	}

	if err := sm.txWithCurrent(ctx, key, set); err != nil {
		return err
	}

	// send update
	if err := sm.rdb.Publish(ctx, sm.channelName(), key+"="+value).Err(); err != nil {
		return fmt.Errorf("failed to publish update: %w", err)
	}

	sm.content[key] = value

	sm.logger.Info("replicated map %q: updated key %q to %q", sm.hashName(), key, value)
	return nil
}

// Get returns the value for the given key.
func (sm *Map) Get(key string) (string, bool) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	res, ok := sm.content[key]
	return res, ok
}

// Delete deletes the value for the given key.
func (sm *Map) Delete(ctx context.Context, key string) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	del := func(current string, pipe redis.Pipeliner) error {
		if err := pipe.HDel(ctx, sm.hashName(), key).Err(); err != nil {
			return fmt.Errorf("failed to delete value: %w", err)
		}
		if err := sm.rdb.Publish(ctx, sm.channelName(), key+"=").Err(); err != nil {
			var err2 error
			if current != "" {
				err2 = sm.rdb.HSet(ctx, sm.hashName(), key, current).Err()
			}
			suffix := ""
			if err2 != nil {
				suffix = fmt.Sprintf(" (failed to restore previous value: %v)", err2)
			}
			return fmt.Errorf("failed to publish update: %w%s", err, suffix)
		}
		return nil
	}

	if err := sm.txWithCurrent(ctx, key, del); err != nil {
		return err
	}
	delete(sm.content, key)
	sm.logger.Info("replicated map %q: deleted key %q", sm.hashName(), key)
	return nil
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
				sm.logger.Error(fmt.Errorf("replicated map %q: invalid message: %q", sm.hashName(), msg.Payload))
				continue
			}
			key, val := parts[0], parts[1]
			sm.lock.Lock()
			current, exists := sm.content[key]
			if val == "" {
				if !exists {
					sm.lock.Unlock()
					continue
				}
				sm.logger.Info("replicated map %q: remote deleted key %q", sm.hashName(), key)
				delete(sm.content, key)
			} else {
				if current == val {
					sm.lock.Unlock()
					continue
				}
				sm.logger.Info("replicated map %q: remote updated key %q to %q", sm.hashName(), key, val)
				sm.content[key] = val
			}
			select {
			// Non-blocking send.
			case sm.notifych <- struct{}{}:
			default:
			}
			sm.lock.Unlock()

		case <-ctx.Done():
			sm.logger.Info("replicated map %q: context canceled, stopping", sm.hashName())
			return
		}
	}
}

// txWithCurrent runs the given function in a Redis transaction, passing the
// current value for the given key.
func (sm *Map) txWithCurrent(ctx context.Context, key string, fn func(string, redis.Pipeliner) error) error {
	try := func(tx *redis.Tx) error {
		current, err := tx.HGet(ctx, sm.hashName(), key).Result()
		exists := err != redis.Nil
		if err != nil && exists {
			return fmt.Errorf("failed to get current value: %w", err)
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			return fn(current, pipe)
		})
		return err
	}

	for {
		err := sm.rdb.Watch(ctx, try, sm.hashName())
		if err == redis.TxFailedErr {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}
		break
	}
	return nil
}

func (sm *Map) channelName() string {
	return "ponos:rmap:" + sm.name
}

func (sm *Map) hashName() string {
	return "ponos:rmap:h:" + sm.name
}
