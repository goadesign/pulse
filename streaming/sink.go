package streaming

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
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

var (
	// checkIdlePeriod is the period at which idle messages are checked.
	checkIdlePeriod = 500 * time.Millisecond
)

type (
	// sinkStream is the complete coordination state owned for one attached
	// stream incarnation.
	sinkStream struct {
		stream     *Stream
		startID    string
		consumers  *rmap.Map
		keepAlives *rmap.Map
		leaseKey   string
		leaseOwner string
	}

	// sinkSnapshot is the exact stream and consumer set used by one
	// XREADGROUP command.
	sinkSnapshot struct {
		streams  map[string]*sinkStream
		args     []string
		consumer string
	}

	// Sink represents a stream sink.
	Sink struct {
		// Name is the sink name.
		Name string
		// closed is true if Close completed.
		closed bool
		// consumer is the sink consumer name.
		consumer string
		// startID is the sink start event ID.
		startID string
		// noAck is true if there is no need to acknowledge events.
		noAck bool
		// lock is the sink mutex.
		lock sync.Mutex
		// streams is the per-incarnation coordination state indexed by physical
		// stream key.
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
		// ctx is canceled when Close begins and bounds all sink-owned Redis I/O.
		ctx context.Context
		// cancel interrupts sink-owned Redis I/O.
		cancel context.CancelFunc
		// wait is the sink cleanup wait group.
		wait sync.WaitGroup
		// stopOnce cancels background work exactly once; distributed cleanup is
		// intentionally retried until it succeeds.
		stopOnce sync.Once
		// closing is true if Close was called.
		closing atomic.Bool
		// eventFilter is the event filter if any.
		eventFilter eventFilterFunc
		// filterKind and filterValue preserve the exact shared configuration
		// applied to every later AddStream attachment.
		filterKind  string
		filterValue string
		// ackGracePeriod is the grace period after which an event is
		// considered unacknowledged.
		ackGracePeriod time.Duration
		// lastKeepAlive is the last keep-alive timestamp for this consumer.
		lastKeepAlive int64
		// logger is the logger used by the sink.
		logger pulse.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// eventFilterFunc is the function used to filter events.
	eventFilterFunc func(*Event) bool
)

var (
	// ErrSinkClosed is returned when stream ownership is changed after sink
	// shutdown begins.
	ErrSinkClosed = errors.New("pulse streaming: sink is closed")
)

// newSink creates a sink whose consumer identity spans every attached stream.
// Each stream generation owns independent membership, keepalive, recovery, and
// PEL state for that identity. Detachment deletes the Redis consumer only when
// that consumer's own PEL is empty, because deletion would otherwise discard
// pending ownership.
func newSink(ctx context.Context, name string, stream *Stream, opts ...options.Sink) (*Sink, error) {
	o := options.ParseSinkOptions(opts...)
	if err := validateSinkOptions(o); err != nil {
		return nil, err
	}
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
	if err := stream.ensureGeneration(ctx); err != nil {
		return nil, err
	}
	logger := stream.rootLogger.WithPrefix("sink", name)
	config := sinkConfigurationForOptions(o, o.LastEventID)
	state, err := attachSinkStream(ctx, stream, name, config, logger)
	if err != nil {
		return nil, err
	}

	// Preserve the caller's logger while giving all background Redis commands
	// a context Close can cancel.
	logCtx := context.Background()
	logCtx = log.WithContext(logCtx, ctx)
	runCtx, cancel := context.WithCancel(logCtx)
	sink := &Sink{
		Name:           name,
		startID:        o.LastEventID,
		noAck:          o.NoAck,
		streams:        map[string]*sinkStream{stream.key: state},
		blockDuration:  o.BlockDuration,
		maxPolled:      o.MaxPolled,
		bufferSize:     o.BufferSize,
		donechan:       make(chan struct{}),
		ctx:            runCtx,
		cancel:         cancel,
		eventFilter:    eventMatcher,
		filterKind:     config.filterKind,
		filterValue:    config.filterValue,
		ackGracePeriod: o.AckGracePeriod,
		logger:         logger,
		rdb:            stream.rdb,
	}

	// Clean up existing stale consumers under this stream's fenced lease before
	// creating our own.
	if err := sink.deleteStreamStaleConsumersWithLease(ctx, state); err != nil {
		sink.logger.Error(fmt.Errorf("failed to cleanup stale consumers: %w", err))
	}

	consumer, err := sink.newConsumer(ctx)
	if err != nil {
		cancel()
		closeSetupMembership(ctx, stream, state.consumers, err)
		state.keepAlives.Close()
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

// Subscribe returns a channel that receives events from the sink. Calls made
// after shutdown starts return an already closed channel.
func (s *Sink) Subscribe() <-chan *Event {
	c := make(chan *Event, s.bufferSize)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing.Load() {
		close(c)
		return c
	}
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

// Ack acknowledges the event.
func (s *Sink) Ack(ctx context.Context, e *Event) error {
	if err := e.Acker.XAck(ctx, e.streamKey, e.SinkName, e.ID).Err(); err != nil {
		s.logger.Error(err, "ack", e.ID, "stream", e.StreamName)
		return err
	}
	s.logger.Debug("acked", "event", e.ID, "stream", e.StreamName, "from-sink", e.SinkName)
	return nil
}

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink
// and returns ErrSinkClosed once shutdown starts.
func (s *Sink) AddStream(ctx context.Context, stream *Stream, opts ...options.AddStream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing.Load() {
		return ErrSinkClosed
	}
	addOptions := options.ParseAddStreamOptions(opts...)
	if err := validateAddStreamOptions(addOptions); err != nil {
		return err
	}
	if err := stream.verifyGeneration(ctx); err != nil {
		return err
	}
	for _, state := range s.streams {
		if state.stream.Name != stream.Name {
			continue
		}
		if state.stream.generation == stream.generation {
			return nil
		}
		return state.stream.verifyGeneration(ctx)
	}
	startID := s.startID
	if addOptions.LastEventID != "" {
		startID = addOptions.LastEventID
	}
	config := sinkConfiguration{
		filterKind:  s.filterKind,
		filterValue: s.filterValue,
		startID:     startID,
		noAck:       s.noAck,
		ackGrace:    s.ackGracePeriod,
	}
	state, err := attachSinkStream(ctx, stream, s.Name, config, s.logger)
	if err != nil {
		return err
	}
	if err := registerSinkConsumer(ctx, state, s.Name, s.consumer, s.lastKeepAlive); err != nil {
		closeSetupMembership(ctx, stream, state.consumers, err)
		state.keepAlives.Close()
		return err
	}
	s.streams[stream.key] = state
	s.logger.Info("added", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the sink. It is idempotent and returns
// ErrSinkClosed once shutdown starts. Removing the final stream returns
// ErrLastStream.
func (s *Sink) RemoveStream(ctx context.Context, stream *Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing.Load() {
		return ErrSinkClosed
	}
	var state *sinkStream
	for _, candidate := range s.streams {
		if candidate.stream == stream ||
			(stream.generation != "" &&
				candidate.stream.Name == stream.Name &&
				candidate.stream.generation == stream.generation) {
			state = candidate
			break
		}
	}
	if state == nil {
		return nil
	}
	if len(s.streams) == 1 {
		return ErrLastStream
	}
	if err := state.stream.verifyGeneration(ctx); err != nil {
		return err
	}
	if _, err := detachSinkConsumer(ctx, state, s.Name, s.consumer); err != nil {
		return fmt.Errorf("failed to detach consumer %s from stream %s: %w", s.consumer, stream.Name, err)
	}
	delete(s.streams, state.stream.key)
	state.close()
	s.logger.Info("removed", "stream", stream.Name)
	return nil
}

// Close stops event polling and detaches every distributed membership. Failed
// Redis cleanup is returned and may be retried with another context; the sink
// is closed only after all membership and keep-alive side effects complete.
func (s *Sink) Close(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.closing.Store(true)
		s.cancel()
		// Close donechan first, without holding the lock, so the signal
		// reaches the read loop even when it is parked on a fan-out send
		// to a stalled subscriber (which holds the lock). Otherwise Close
		// would deadlock acquiring the lock the read loop never releases.
		close(s.donechan)
	})
	s.wait.Wait()

	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	var cleanupErr error
	for streamKey, state := range s.streams {
		stream := state.stream
		err := stream.verifyGeneration(ctx)
		if errors.Is(err, ErrStreamDestroyed) || errors.Is(err, ErrDeadlineElapsed) {
			state.close()
			delete(s.streams, streamKey)
			continue
		}
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
				"failed to verify stream %s before detaching consumer %s: %w",
				streamKey,
				s.consumer,
				err,
			))
			continue
		}
		if _, err := detachSinkConsumer(ctx, state, s.Name, s.consumer); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
				"failed to detach consumer %s from stream %s: %w",
				s.consumer,
				streamKey,
				err,
			))
			continue
		}
		state.close()
		delete(s.streams, streamKey)
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	for _, c := range s.chans {
		close(c)
	}
	s.chans = nil
	s.closed = true
	s.logger.Info("closed")
	return nil
}

// IsClosed returns true if the sink was closed.
func (s *Sink) IsClosed() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.closed
}

// deleteStreamStaleConsumersWithLease deletes stale consumers for one attached
// stream only while this replica owns its fenced recovery lease.
func (s *Sink) deleteStreamStaleConsumersWithLease(ctx context.Context, state *sinkStream) error {
	duration := 2 * checkIdlePeriod
	lease, acquired, err := acquireSinkRecoveryLease(
		ctx,
		state.stream,
		state.leaseKey,
		state.leaseOwner,
		duration,
	)
	if err != nil || !acquired {
		return err
	}
	return s.deleteStreamStaleConsumers(ctx, state, lease, duration)
}

// deleteStreamStaleConsumers deletes stale consumers for a specific stream in
// one lease-fenced Redis operation.
// s.lock must be held once the sink is running.
func (s *Sink) deleteStreamStaleConsumers(
	ctx context.Context,
	state *sinkStream,
	lease sinkRecoveryLease,
	leaseDuration time.Duration,
) error {
	removed, malformed, err := cleanupStaleConsumers(
		ctx,
		state.stream,
		state.leaseKey,
		lease,
		leaseDuration,
		s.Name,
		s.ackGracePeriod,
	)
	if err != nil {
		return err
	}
	for _, consumer := range removed {
		s.logger.Info("cleaned up stale consumer", "consumer", consumer)
	}
	for _, consumer := range malformed {
		s.logger.Error(
			fmt.Errorf("invalid keep-alive timestamp"),
			"stream", state.stream.Name,
			"consumer", consumer,
		)
	}
	return nil
}

// newConsumer creates one replacement consumer across every owned stream. No
// stream observes local ownership unless all Redis consumers, memberships, and
// the shared keep-alive are established; failures roll back the full prefix.
func (s *Sink) newConsumer(ctx context.Context) (string, error) {
	consumer := ulid.Make().String()
	now, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return "", fmt.Errorf("failed to read Redis time for new consumer %s: %w", consumer, err)
	}
	keepAlive := now.UnixNano()
	registered := make([]*sinkStream, 0, len(s.streams))
	for _, state := range s.streams {
		registered = append(registered, state)
		if err := registerSinkConsumer(ctx, state, s.Name, consumer, keepAlive); err != nil {
			return "", errors.Join(err, s.rollbackConsumer(ctx, consumer, registered))
		}
	}
	s.lastKeepAlive = keepAlive
	return consumer, nil
}

// rollbackConsumer removes a replacement consumer from every stream that was
// registered before consumer creation failed.
func (s *Sink) rollbackConsumer(ctx context.Context, consumer string, states []*sinkStream) error {
	var rollbackErr error
	for i := len(states) - 1; i >= 0; i-- {
		state := states[i]
		stream := state.stream
		if _, err := detachSinkConsumer(ctx, state, s.Name, consumer); err != nil {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf(
				"failed to roll back consumer %s from stream %s: %w",
				consumer,
				stream.Name,
				err,
			))
		}
	}
	return rollbackErr
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read() {
	defer s.logger.Debug("read: exiting")
	defer s.wait.Done()
	var retry readRetry
	for {
		if err := s.ensureConsumer(s.ctx); err != nil {
			if !retry.wait(s.donechan, err, s.logger) {
				return
			}
			continue
		}
		snapshot, err := s.readSnapshot(s.ctx)
		if err != nil {
			if fatal := fatalReadError(err); fatal != nil {
				pulse.Go(s.logger, func() {
					if closeErr := s.Close(context.WithoutCancel(s.ctx)); closeErr != nil {
						s.logger.Error(fmt.Errorf("failed to close terminal sink: %w", closeErr))
					}
				})
				return
			}
			if !retry.wait(s.donechan, err, s.logger) {
				return
			}
			continue
		}

		s.logger.Debug("reading", "streams", snapshot.args, "max", s.maxPolled, "block", s.blockDuration)
		streams, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
			Group:    s.Name,
			Consumer: snapshot.consumer,
			Streams:  snapshot.args,
			Count:    s.maxPolled,
			Block:    s.blockDuration,
		}).Result()

		if s.closing.Load() {
			// Honor the Close contract and do not forward any more events.
			// Any events in the PEL will be claimed by another consumer.
			return
		}
		if err == nil {
			for _, events := range streams {
				owned, ok := snapshot.streams[events.Stream]
				if !ok {
					continue
				}
				stream := owned.stream
				if verifyErr := stream.verifyGeneration(s.ctx); verifyErr != nil {
					err = verifyErr
					break
				}
				s.lock.Lock()
				current := s.ownsStream(stream)
				if !current {
					s.lock.Unlock()
					if ackErr := acknowledgeMessages(s.ctx, stream, s.Name, events.Messages); ackErr != nil {
						err = ackErr
						break
					}
					continue
				}
				err = streamEvents(
					s.ctx,
					stream,
					s.Name,
					events.Messages,
					s.noAck,
					s.eventFilter,
					s.chans,
					s.donechan,
					s.rdb,
					s.logger,
				)
				s.lock.Unlock()
				if err != nil {
					break
				}
			}
		}
		if err != nil {
			if redis.HasErrorPrefix(err, "NOGROUP") {
				s.lock.Lock()
				err = s.recoverConsumerGroups(s.ctx)
				s.lock.Unlock()
			}
			fatal := fatalReadError(err)
			if fatal != nil {
				s.logger.Error(fmt.Errorf("fatal error while reading events: %w, stopping", fatal))
				pulse.Go(s.logger, func() {
					if err := s.Close(context.WithoutCancel(s.ctx)); err != nil {
						s.logger.Error(fmt.Errorf("failed to close terminal sink: %w", err))
					}
				})
				return
			}
			if err == nil || err == redis.Nil {
				retry.reset()
				continue
			}
			if !retry.wait(s.donechan, err, s.logger) {
				return
			}
			continue
		}
		retry.reset()
	}
}

// readSnapshot verifies and captures the exact stream capabilities and
// consumer used by one grouped read.
func (s *Sink) readSnapshot(ctx context.Context) (sinkSnapshot, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	snapshot := sinkSnapshot{
		streams:  make(map[string]*sinkStream, len(s.streams)),
		args:     make([]string, len(s.streams)*2),
		consumer: s.consumer,
	}
	keys := make([]string, 0, len(s.streams))
	for key := range s.streams {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		state := s.streams[key]
		if err := state.stream.verifyGeneration(ctx); err != nil {
			return sinkSnapshot{}, err
		}
		snapshot.streams[key] = state
		snapshot.args[i] = key
		snapshot.args[len(keys)+i] = ">"
	}
	return snapshot, nil
}

// ownsStream reports whether the sink still owns the exact capability used by
// an in-flight read.
func (s *Sink) ownsStream(candidate *Stream) bool {
	for _, state := range s.streams {
		if state.stream == candidate {
			return true
		}
	}
	return false
}

// acknowledgeMessages atomically advances recovery for messages returned by a
// read whose stream was concurrently removed.
func acknowledgeMessages(ctx context.Context, stream *Stream, group string, messages []redis.XMessage) error {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]string, len(messages))
	for i, message := range messages {
		ids[i] = message.ID
	}
	return (&recoveryAcker{stream: stream}).XAck(ctx, stream.key, group, ids...).Err()
}

// ensureConsumer ensures that the consumer is still alive.
func (s *Sink) ensureConsumer(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	now, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to read Redis time while checking consumer: %w", err)
	}
	if now.Sub(time.Unix(0, s.lastKeepAlive)) > 2*s.ackGracePeriod {
		s.logger.Debug("consumer stale, creating new one")
		consumer, err := s.newConsumer(ctx)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to create new consumer: %w", err))
			return err
		}
		s.consumer = consumer
	}
	return nil
}

// periodicKeepAlive updates this consumer keep-alive every half ack grace period.
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
				s.logger.Error(fmt.Errorf("failed to read Redis time for sink keep-alive: %w", err))
				s.lock.Unlock()
				continue
			}
			keepAlive := now.UnixNano()
			var updateErr error
			for _, state := range s.streams {
				err := setSinkKeepAlive(s.ctx, state, s.Name, s.consumer, keepAlive)
				switch {
				case err == nil:
				case errors.Is(err, ErrStreamDestroyed), errors.Is(err, ErrDeadlineElapsed):
					// The generation ended; the read loop owns detaching the
					// stream. Refreshing nothing is correct here.
					s.logger.Debug("keep-alive skipped", "stream", state.stream.Name, "reason", err)
				default:
					updateErr = errors.Join(updateErr, fmt.Errorf(
						"stream %s: %w",
						state.stream.Name,
						err,
					))
				}
			}
			if updateErr != nil {
				s.logger.Error(fmt.Errorf("failed to update sink keep-alive: %w", updateErr))
				s.lock.Unlock()
				continue
			}
			s.lastKeepAlive = now.UnixNano()
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// periodicIdleMessageCheck claims any idle message every check stale period.
// An idle message is one that has not been acked for more than the ack grace period.
// Once all idle messages are claimed, any stale consumer is deleted.
func (s *Sink) periodicIdleMessageCheck() {
	defer s.wait.Done()
	defer s.logger.Debug("periodicIdleMessageCheck: exiting")
	ticker := time.NewTicker(checkIdlePeriod)
	defer ticker.Stop()

	leaseDuration := 2 * checkIdlePeriod
	for {
		select {
		case <-ticker.C:
			s.lock.Lock()
			for _, state := range s.streams {
				lease, acquired, err := acquireSinkRecoveryLease(
					s.ctx,
					state.stream,
					state.leaseKey,
					state.leaseOwner,
					leaseDuration,
				)
				if err != nil {
					s.logger.Error(fmt.Errorf(
						"failed to acquire stale-recovery lease for stream %s: %w",
						state.stream.Name,
						err,
					))
					continue
				}
				if !acquired {
					continue
				}
				if err := s.claimIdleMessages(s.ctx, state, lease, leaseDuration); err != nil {
					s.logger.Error(fmt.Errorf(
						"failed to claim idle messages for stream %s: %w",
						state.stream.Name,
						err,
					))
					continue
				}
				if err := s.deleteStreamStaleConsumers(s.ctx, state, lease, leaseDuration); err != nil {
					s.logger.Error(fmt.Errorf(
						"failed to delete stale consumers for stream %s: %w",
						state.stream.Name,
						err,
					))
				}
			}
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// claimIdleMessages claims idle messages from one stream through an atomic
// exact-lease fence around every XAUTOCLAIM.
// s.lock must be held.
func (s *Sink) claimIdleMessages(
	ctx context.Context,
	state *sinkStream,
	lease sinkRecoveryLease,
	leaseDuration time.Duration,
) error {
	start, err := s.claim(ctx, state, lease, leaseDuration, "0-0")
	if err != nil {
		return err
	}
	for start != "0-0" {
		start, err = s.claim(ctx, state, lease, leaseDuration, start)
		if err != nil {
			return err
		}
	}
	return nil
}

// recoverConsumerGroups recreates groups removed outside Pulse for the streams
// the sink still owns. The caller holds s.lock, which serializes recovery with
// AddStream and RemoveStream so an in-flight stale read cannot resurrect a
// removed stream. BUSYGROUP means another sink instance already repaired the
// shared group and is therefore success.
func (s *Sink) recoverConsumerGroups(ctx context.Context) error {
	for _, state := range s.streams {
		stream := state.stream
		deleted, err := ensureConsumerGroup(ctx, stream, s.Name, state.startID)
		if err != nil {
			return err
		}
		if deleted {
			s.logger.Error(
				fmt.Errorf("stream data was deleted before consumer group recovery"),
				"stream", stream.Name,
				"group", s.Name,
				"data_loss", true,
			)
		}
		s.logger.Info("recovered consumer group", "stream", stream.Name, "start", state.startID)
	}
	return nil
}

// Helper function to claim messages from a stream used by claimIdleMessages.
func (s *Sink) claim(
	ctx context.Context,
	state *sinkStream,
	lease sinkRecoveryLease,
	leaseDuration time.Duration,
	start string,
) (string, error) {
	next, messages, err := fencedAutoClaim(
		ctx,
		state.stream,
		state.leaseKey,
		lease,
		leaseDuration,
		s.Name,
		s.consumer,
		s.ackGracePeriod,
		start,
		s.maxPolled,
	)
	if err != nil {
		return start, err
	}
	if len(messages) > 0 {
		s.logger.Info("claimed", "stream", state.stream.Name, "messages", len(messages))
		err = streamEvents(
			ctx,
			state.stream,
			s.Name,
			messages,
			s.noAck,
			s.eventFilter,
			s.chans,
			s.donechan,
			s.rdb,
			s.logger,
		)
	}
	return next, err
}

// attachSinkStream validates the shared contract before joining any group
// membership, then builds the complete per-stream coordination state.
func attachSinkStream(
	ctx context.Context,
	stream *Stream,
	name string,
	config sinkConfiguration,
	logger pulse.Logger,
) (*sinkStream, error) {
	if err := ensureSinkConfiguration(ctx, stream, name, config); err != nil {
		return nil, err
	}
	consumers, err := rmap.Join(
		ctx,
		consumersMapName(stream),
		stream.rdb,
		rmap.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink %s: %w", name, err)
	}
	keepAlives, err := rmap.Join(
		ctx,
		sinkKeepAliveMapName(stream, name),
		stream.rdb,
		rmap.WithLogger(logger),
	)
	if err != nil {
		consumers.Close()
		return nil, fmt.Errorf("failed to join replicated map for sink keep-alives %s: %w", name, err)
	}
	if _, err := ensureConsumerGroup(ctx, stream, name, config.startID); err != nil {
		closeSetupMembership(ctx, stream, consumers, err)
		keepAlives.Close()
		return nil, err
	}
	return &sinkStream{
		stream:     stream,
		startID:    config.startID,
		consumers:  consumers,
		keepAlives: keepAlives,
		leaseKey:   staleLockName(stream, name),
		leaseOwner: ulid.Make().String(),
	}, nil
}

// close releases both local rmap replicas owned by one attachment.
func (s *sinkStream) close() {
	s.consumers.Close()
	s.keepAlives.Close()
}

// consumersMapName is the name of the replicated map that backs a sink.
func consumersMapName(stream *Stream) string {
	return fmt.Sprintf("stream:%s:generation:%s:sinks", stream.Name, stream.generation)
}

// closeSetupMembership destroys an orphan generation-qualified map when the
// stream was destroyed during setup; otherwise it only releases this replica.
func closeSetupMembership(ctx context.Context, stream *Stream, membership *rmap.Map, setupErr error) {
	if errors.Is(setupErr, ErrStreamDestroyed) {
		membership.Close()
		if err := stream.rdb.Del(ctx, consumersMapContentKey(stream)).Err(); err != nil {
			stream.logger.Error(fmt.Errorf("failed to delete orphan sink membership: %w", err))
		}
		return
	}
	membership.Close()
}
