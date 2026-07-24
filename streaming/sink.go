// This file implements Sink, the consumer-group side of a stream. A sink read
// loop XREADGROUPs events for every stream added to the sink, fans them out
// to subscribers, and settles them through the recovery acker defined in
// sink_recovery.go so the durable recovery cursor tracks exactly what was
// acknowledged. Background goroutines refresh the sink keep-alive and, under
// the fenced lease defined in sink_lease.go, claim idle messages and delete
// stale consumers left behind by dead sink instances.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/clue/log"
	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming/options"
)

type (
	// Sink represents a stream sink.
	Sink struct {
		// Name is the sink name.
		Name string
		// closed is true if Close completed.
		closed bool
		// consumer is the sink consumer name.
		consumer string
		// leaseOwner identifies this sink instance in the fenced lease used
		// for idle message claiming and stale consumer cleanup.
		leaseOwner string
		// startID is the sink start event ID.
		startID string
		// noAck is true if there is no need to acknowledge events.
		noAck bool
		// lock is the sink mutex.
		lock sync.Mutex
		// streams are the streams the sink consumes events from, indexed by
		// stream Redis key.
		streams map[string]*sinkStream
		// blockDuration is the XREADGROUP timeout.
		blockDuration time.Duration
		// maxPolled is the maximum number of events to read in one
		// XREADGROUP call.
		maxPolled int64
		// bufferSize is the sink channel buffer size.
		bufferSize int
		// chans are the sink event channels.
		chans []chan *Event
		// donechan is the sink done channel.
		donechan chan struct{}
		// wait is the sink cleanup wait group.
		wait sync.WaitGroup
		// closeOnce is used to ensure the sink is closed only once.
		closeOnce sync.Once
		// closing is set when Close starts so loops stop scheduling work.
		closing atomic.Bool
		// ctx is canceled by Close to abort all sink-owned Redis I/O,
		// including blocked XREADGROUP calls and recovery in progress.
		ctx context.Context
		// cancel cancels ctx.
		cancel context.CancelFunc
		// eventFilter is the event filter if any.
		eventFilter eventFilterFunc
		// consumersKeepAliveMap records consumer keep-alives for this
		// sink (i.e. for all in-process instances of the sink).
		consumersKeepAliveMap *rmap.Map
		// ackGracePeriod is the grace period after which an event is
		// considered unacknowledged.
		ackGracePeriod time.Duration
		// lastKeepAlive is the last keep-alive timestamp for this consumer
		// in Redis-time nanoseconds.
		lastKeepAlive int64
		// acker settles events and advances the durable recovery cursor.
		acker *recoveryAcker
		// logger is the logger used by the sink.
		logger pulse.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// sinkStream is the sink-side state for one consumed stream: the stream
	// handle, the start ID used when (re)creating the consumer group, and the
	// replicated membership map listing the consumers of each sink.
	sinkStream struct {
		// stream is the consumed stream.
		stream *Stream
		// startID is the group start position for brand new groups.
		startID string
		// consumers is the stream membership map (sink name to consumer
		// names), joined for reads and change notifications; all writes go
		// through the fenced scripts in sink_recovery.go.
		consumers *rmap.Map
	}

	// eventFilterFunc is the function used to filter events.
	eventFilterFunc func(*Event) bool
)

// checkIdlePeriod is the period at which idle messages are checked.
var checkIdlePeriod = 500 * time.Millisecond

// newSink creates a new sink.
// Sinks use one Redis consumer per stream they are consuming from.
// Pulse maintains a pool of consumers per stream and reuses them when possible.
// This is because deleting a consumer causes Redis to drop its pending messages
// which is not the semantics Pulse wants to enforce.
func newSink(ctx context.Context, name string, stream *Stream, opts ...options.Sink) (*Sink, error) {
	o := options.ParseSinkOptions(opts...)
	var eventMatcher eventFilterFunc
	if o.Topic != "" {
		eventMatcher = func(e *Event) bool { return e.Topic == o.Topic }
	} else if o.TopicPattern != "" {
		topicPatternRegexp, err := regexp.Compile(o.TopicPattern)
		if err != nil {
			return nil, fmt.Errorf("topic pattern must be a valid regex: %w", err)
		}
		eventMatcher = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
	}

	logger := stream.rootLogger.WithPrefix("sink", name)
	km, err := rmap.Join(ctx, sinkKeepAliveMapName(name), stream.rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink keep-alives %s: %w", name, err)
	}

	// runCtx outlives the caller context and is canceled by Close so all
	// sink-owned Redis I/O, including recovery in progress, stops promptly.
	runCtx, cancel := context.WithCancel(log.WithContext(context.Background(), ctx))

	sink := &Sink{
		Name:                  name,
		leaseOwner:            ulid.Make().String(),
		startID:               o.LastEventID,
		noAck:                 o.NoAck,
		streams:               make(map[string]*sinkStream, 1),
		blockDuration:         o.BlockDuration,
		maxPolled:             o.MaxPolled,
		bufferSize:            o.BufferSize,
		donechan:              make(chan struct{}),
		ctx:                   runCtx,
		cancel:                cancel,
		eventFilter:           eventMatcher,
		consumersKeepAliveMap: km,
		ackGracePeriod:        o.AckGracePeriod,
		acker:                 &recoveryAcker{rdb: stream.rdb},
		logger:                logger,
		rdb:                   stream.rdb,
	}

	state, err := sink.attachStream(ctx, stream, o.LastEventID)
	if err != nil {
		cancel()
		km.Close()
		return nil, err
	}
	sink.streams[stream.key] = state

	consumer, err := sink.newConsumer(ctx)
	if err != nil {
		// Compensate the group and cursor created by attachStream (the group
		// survives only when other sink instances are members).
		if cerr := removeSinkStream(ctx, stream, name, ""); cerr != nil {
			err = errors.Join(err, cerr)
		}
		cancel()
		state.consumers.Close()
		km.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	sink.consumer = consumer
	sink.logger = sink.logger.WithPrefix("consumer", consumer)

	sink.wait.Add(3)
	pulse.Go(logger, sink.read)
	pulse.Go(logger, sink.periodicKeepAlive)
	pulse.Go(logger, sink.periodicIdleMessageCheck)

	sink.logger.Info("created", "start", sink.startID, "stream", stream.Name, "max_polled", sink.maxPolled, "block_duration", sink.blockDuration, "buffer_size", sink.bufferSize, "no_ack", sink.noAck, "ack_grace_period", sink.ackGracePeriod)

	return sink, nil
}

// Subscribe returns a channel that receives events from the sink.
func (s *Sink) Subscribe() <-chan *Event {
	c := make(chan *Event, s.bufferSize)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.chans = append(s.chans, c)
	return c
}

// Unsubscribe removes the channel from the sink and closes it.
func (s *Sink) Unsubscribe(c <-chan *Event) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, ch := range s.chans {
		if ch == c {
			close(ch)
			s.chans = append(s.chans[:i], s.chans[i+1:]...)
			return
		}
	}
}

// Ack acknowledges the event and advances the sink recovery cursor.
func (s *Sink) Ack(ctx context.Context, e *Event) error {
	err := e.Acker.XAck(ctx, e.streamKey, e.SinkName, e.ID).Err()
	if err != nil {
		s.logger.Error(err, "ack", e.ID, "stream", e.StreamName)
		return err
	}
	s.logger.Debug("acked", "event", e.ID, "stream", e.StreamName, "from-sink", e.SinkName)
	return nil
}

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink.
// It returns ErrSinkClosed after Close.
func (s *Sink) AddStream(ctx context.Context, stream *Stream, opts ...options.AddStream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing.Load() {
		return ErrSinkClosed
	}
	if _, ok := s.streams[stream.key]; ok {
		return nil
	}
	startID := s.startID
	o := options.ParseAddStreamOptions(opts...)
	if o.LastEventID != "" {
		startID = o.LastEventID
	}
	state, err := s.attachStream(ctx, stream, startID)
	if err != nil {
		return err
	}
	if err := registerSinkConsumer(ctx, stream, s.Name, s.consumer); err != nil {
		// Compensate the group and cursor created by attachStream so a failed
		// AddStream leaves no dangling ownership state (the group survives
		// only when other sink instances are members).
		if cerr := removeSinkStream(ctx, stream, s.Name, s.consumer); cerr != nil {
			err = errors.Join(err, cerr)
		}
		state.consumers.Close()
		return err
	}
	s.streams[stream.key] = state
	s.logger.Info("added", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent. The
// distributed effects (membership removal and, for the last member, consumer
// group, recovery cursor, and lease deletion) execute in one atomic script so
// there is no partial state to compensate. It returns ErrSinkClosed after
// Close.
func (s *Sink) RemoveStream(ctx context.Context, stream *Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing.Load() {
		return ErrSinkClosed
	}
	state, ok := s.streams[stream.key]
	if !ok {
		return nil
	}
	if err := removeSinkStream(ctx, state.stream, s.Name, s.consumer); err != nil {
		return err
	}
	delete(s.streams, stream.key)
	state.consumers.Close()
	s.logger.Info("removed", "stream", stream.Name)
	return nil
}

// Close stops event polling, cancels all sink-owned Redis I/O (including any
// recovery in progress), waits for the sink goroutines to stop, and closes
// the sink channels. It is safe to call Close multiple times; concurrent
// callers block until the first Close completes.
func (s *Sink) Close(ctx context.Context) {
	s.closeOnce.Do(func() {
		// Signal shutdown without holding the lock so the read loop stops
		// even when it is parked on a fan-out send to a stalled subscriber
		// (which holds the lock) or blocked in a Redis call.
		s.closing.Store(true)
		s.cancel()
		close(s.donechan)
		s.wait.Wait()
		s.lock.Lock()
		defer s.lock.Unlock()
		for _, c := range s.chans {
			close(c)
		}
		// Note: we do not delete the consumer from the keep-alive and consumer maps
		// so that another instance may claim any pending messages.
		s.consumersKeepAliveMap.Close()
		for _, state := range s.streams {
			state.consumers.Close()
		}
		s.closed = true
		s.logger.Info("closed")
	})
}

// IsClosed returns true if the sink was closed.
func (s *Sink) IsClosed() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.closed
}

// attachStream ensures the consumer group and recovery cursor exist for the
// stream (restoring the stream TTL even on BUSYGROUP) and joins the stream
// membership map. Callers own registering the sink consumer.
func (s *Sink) attachStream(ctx context.Context, stream *Stream, startID string) (*sinkStream, error) {
	if _, _, err := ensureConsumerGroup(ctx, stream, s.Name, startID, true); err != nil {
		return nil, err
	}
	cm, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, consumersMapOptions(stream, s.logger)...)
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for stream %s: %w", stream.Name, err)
	}
	return &sinkStream{stream: stream, startID: startID, consumers: cm}, nil
}

// read reads events from the streams and sends them to the sink channels.
// NOGROUP errors trigger lossless consumer group recovery; transient Redis
// failures are retried with jittered exponential backoff.
func (s *Sink) read() {
	defer s.logger.Debug("read: exiting")
	defer s.wait.Done()
	var retry readRetry
	for {
		if err := s.ensureConsumer(s.ctx); err != nil {
			if s.closing.Load() {
				return
			}
			if !retry.wait(s.donechan, err, s.logger) {
				return
			}
			continue
		}
		args, consumer := s.readArgs()
		if len(args) == 0 {
			// No streams to read from; wait for AddStream or Close.
			select {
			case <-s.donechan:
				return
			case <-time.After(s.blockDuration):
				continue
			}
		}
		s.logger.Debug("reading", "streams", args, "max", s.maxPolled, "block", s.blockDuration)
		streams, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
			Group:    s.Name,
			Consumer: consumer,
			Streams:  args,
			Count:    s.maxPolled,
			Block:    s.blockDuration,
			NoAck:    s.noAck,
		}).Result()
		if s.closing.Load() {
			// Honor the Close contract and do not forward any more events.
			// Any events in the PEL will be claimed by another consumer.
			return
		}
		if err == nil {
			err = s.dispatch(streams)
		}
		if err == nil || err == redis.Nil {
			retry.reset()
			continue
		}
		if redis.HasErrorPrefix(err, "NOGROUP") {
			if err := s.recoverConsumerGroups(s.ctx); err == nil {
				retry.reset()
				continue
			} else if s.closing.Load() {
				return
			} else if !retry.wait(s.donechan, err, s.logger) {
				return
			}
			continue
		}
		if !retry.wait(s.donechan, err, s.logger) {
			return
		}
	}
}

// dispatch fans out one XREADGROUP reply to the subscribers, settling events
// through the recovery acker. Batches for streams removed from the sink
// concurrently with the read are acknowledged without delivery so the
// recovery cursor keeps advancing for the remaining sink instances.
func (s *Sink) dispatch(streams []redis.XStream) error {
	for _, events := range streams {
		s.lock.Lock()
		state, owned := s.streams[events.Stream]
		if !owned {
			s.lock.Unlock()
			ids := make([]string, len(events.Messages))
			for i, msg := range events.Messages {
				ids[i] = msg.ID
			}
			if err := s.acker.XAck(s.ctx, events.Stream, s.Name, ids...).Err(); err != nil {
				s.logger.Error(fmt.Errorf("failed to settle events of removed stream %s: %w", events.Stream, err))
			}
			continue
		}
		err := streamEvents(s.ctx, state.stream.Name, state.stream.key, s.Name, events.Messages, s.acker, s.noAck, s.eventFilter, s.chans, s.donechan, s.logger)
		s.lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// recoverConsumerGroups recreates missing consumer groups at the durable
// recovery cursor after Redis loses group state (e.g. XGROUP DESTROY). A
// stream that was destroyed with Stream.Destroy is dropped from the sink
// instead of being resurrected.
func (s *Sink) recoverConsumerGroups(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for key, state := range s.streams {
		created, cursor, err := ensureConsumerGroup(ctx, state.stream, s.Name, state.startID, false)
		if err != nil {
			if errors.Is(err, ErrStreamDestroyed) {
				s.logger.Info("stream destroyed, dropping from sink", "stream", state.stream.Name)
				delete(s.streams, key)
				state.consumers.Close()
				continue
			}
			return err
		}
		if created {
			s.logger.Info("recovered consumer group", "stream", state.stream.Name, "cursor", cursor)
		}
	}
	return nil
}

// readArgs snapshots the XREADGROUP stream arguments and the current consumer
// under the sink lock.
func (s *Sink) readArgs() ([]string, string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	keys := make([]string, 0, len(s.streams))
	for key := range s.streams {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 2*len(keys))
	for i, key := range keys {
		args[i] = key
		args[len(keys)+i] = ">"
	}
	return args, s.consumer
}

// ensureConsumer rotates the sink consumer when its keep-alive went stale,
// e.g. after this instance was partitioned long enough for its consumer to be
// cleaned up by a replica. Staleness is evaluated against Redis time so
// client clocks do not skew the decision.
func (s *Sink) ensureConsumer(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	now, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to read Redis time: %w", err)
	}
	if now.Sub(time.Unix(0, s.lastKeepAlive)) <= 2*s.ackGracePeriod {
		return nil
	}
	s.logger.Debug("consumer stale, creating new one")
	consumer, err := s.newConsumer(ctx)
	if err != nil {
		return fmt.Errorf("failed to create new consumer: %w", err)
	}
	s.consumer = consumer
	return nil
}

// newConsumer creates a new consumer, registers it with every sink stream,
// and records its keep-alive. Registration is failure-atomic: when any
// registration fails the consumer is detached from the streams registered so
// far so ownership state never diverges across streams. s.lock must be held.
func (s *Sink) newConsumer(ctx context.Context) (string, error) {
	consumer := ulid.Make().String()
	registered := make([]*sinkStream, 0, len(s.streams))
	rollback := func(cause error) error {
		var errs []error
		for _, state := range registered {
			if err := detachSinkConsumer(ctx, state.stream, s.Name, consumer); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(append([]error{cause}, errs...)...)
	}
	for _, state := range s.streams {
		if err := registerSinkConsumer(ctx, state.stream, s.Name, consumer); err != nil {
			return "", rollback(err)
		}
		registered = append(registered, state)
	}
	now, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return "", rollback(fmt.Errorf("failed to read Redis time for new consumer %s: %w", consumer, err))
	}
	keepAlive := now.UnixNano()
	if _, err := s.consumersKeepAliveMap.Set(ctx, consumer, strconv.FormatInt(keepAlive, 10)); err != nil {
		return "", rollback(fmt.Errorf("failed to set sink keep-alive for new consumer %s: %w", consumer, err))
	}
	s.lastKeepAlive = keepAlive
	return consumer, nil
}

// periodicKeepAlive updates this consumer keep-alive every ack grace period
// using Redis time so replicas evaluating staleness agree on the clock.
func (s *Sink) periodicKeepAlive() {
	defer s.wait.Done()
	defer s.logger.Debug("periodicKeepAlive: exiting")
	ticker := time.NewTicker(s.ackGracePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.lock.Lock()
			now, err := s.rdb.Time(s.ctx).Result()
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to read Redis time for keep-alive: %w", err))
				s.lock.Unlock()
				continue
			}
			keepAlive := now.UnixNano()
			if _, err := s.consumersKeepAliveMap.Set(s.ctx, s.consumer, strconv.FormatInt(keepAlive, 10)); err != nil {
				s.logger.Error(fmt.Errorf("failed to update sink keep-alive: %w", err))
				s.lock.Unlock()
				continue
			}
			s.lastKeepAlive = keepAlive
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// periodicIdleMessageCheck claims idle messages and deletes stale consumers
// under the per-stream fenced lease. An idle message is one that has not been
// acked for more than the ack grace period. Lease renewal and each guarded
// mutation execute in one atomic script so a stale owner cannot mutate the
// PEL after another instance takes over.
func (s *Sink) periodicIdleMessageCheck() {
	defer s.wait.Done()
	defer s.logger.Debug("periodicIdleMessageCheck: exiting")
	ticker := time.NewTicker(checkIdlePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.lock.Lock()
			for _, state := range s.streams {
				s.checkIdleMessages(s.ctx, state)
			}
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// checkIdleMessages acquires the stream lease and, when held, claims idle
// messages for this consumer and deletes stale consumers. Lease loss and
// stream destruction abort silently: another instance owns the work or the
// stream is gone. s.lock must be held.
func (s *Sink) checkIdleMessages(ctx context.Context, state *sinkStream) {
	// Note: the builtin max is shadowed by the package-level test helper
	// variable of the same name, hence the explicit floor.
	leaseMs := 2 * checkIdlePeriod.Milliseconds()
	if leaseMs < 20 {
		leaseMs = 20
	}
	acquired, fence, err := acquireSinkLease(ctx, state.stream, s.Name, s.leaseOwner, leaseMs)
	if err != nil {
		if !errors.Is(err, ErrStreamDestroyed) && ctx.Err() == nil {
			s.logger.Error(fmt.Errorf("failed to acquire idle message check lease: %w", err))
		}
		return
	}
	if !acquired {
		// Another sink instance owns the lease.
		return
	}
	start := "0-0"
	for {
		msgs, next, err := fencedAutoClaim(ctx, state.stream, s.Name, s.leaseOwner, fence, leaseMs, s.consumer, s.ackGracePeriod.Milliseconds(), start, s.maxPolled)
		if err != nil {
			if !isLeaseLostErr(err) && !isStreamDestroyedErr(err) && ctx.Err() == nil {
				s.logger.Error(fmt.Errorf("failed to claim idle messages for stream %s: %w", state.stream.Name, err))
			}
			return
		}
		if len(msgs) > 0 {
			s.logger.Info("claimed", "stream", state.stream.Name, "messages", len(msgs))
			if err := streamEvents(ctx, state.stream.Name, state.stream.key, s.Name, msgs, s.acker, s.noAck, s.eventFilter, s.chans, s.donechan, s.logger); err != nil {
				s.logger.Error(fmt.Errorf("failed to stream claimed events: %w", err))
				return
			}
		}
		if next == "0-0" {
			break
		}
		start = next
	}
	staleNs := (2 * s.ackGracePeriod).Nanoseconds()
	removed, err := fencedCleanupStaleConsumers(ctx, state.stream, s.Name, s.leaseOwner, fence, leaseMs, staleNs, s.consumer)
	if err != nil {
		if !isLeaseLostErr(err) && !isStreamDestroyedErr(err) && ctx.Err() == nil {
			s.logger.Error(fmt.Errorf("failed to delete stale consumers for stream %s: %w", state.stream.Name, err))
		}
		return
	}
	if len(removed) > 0 {
		s.logger.Info("deleted stale consumers", "stream", state.stream.Name, "consumers", removed)
	}
}

// consumersMapName is the name of the replicated map that backs a sink.
func consumersMapName(stream *Stream) string {
	return fmt.Sprintf("stream:%s:sinks", stream.Name)
}

func consumersMapOptions(stream *Stream, logger pulse.Logger) []rmap.MapOption {
	opts := []rmap.MapOption{
		rmap.WithLogger(logger),
	}
	if stream.ttl > 0 {
		if stream.ttlSliding {
			opts = append(opts, rmap.WithSlidingTTL(stream.ttl))
		} else {
			opts = append(opts, rmap.WithTTL(stream.ttl))
		}
	}
	return opts
}

// sinkKeepAliveMapName is the name of the replicated map that backs a sink keep-alives.
func sinkKeepAliveMapName(sink string) string {
	return fmt.Sprintf("sink:%s:keepalive", sink)
}
