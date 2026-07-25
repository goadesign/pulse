package streaming

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/clue/log"
	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
)

type (
	// readRetry bounds the command rate while a long-lived reader or sink waits
	// for Redis to recover. Successful commands reset the exponential delay.
	readRetry struct {
		failures int
		jitter   func(int64) int64
	}

	// readerSnapshot is the exact immutable stream set used by one XREAD.
	readerSnapshot struct {
		streams map[string]*Stream
		args    []string
	}

	// Reader represents a stream reader.
	Reader struct {
		// closed is true if Close completed.
		closed bool
		// startID is the reader start event ID.
		startID string
		// lock is the reader mutex.
		lock sync.Mutex
		// streams are the streams the reader consumes events from.
		streams []*Stream
		// streamKeys is the stream names used to read events in
		// the same order as streamCursors
		streamKeys []string
		// streamCursors is the stream cursors used to read events in
		// the same order as streamNames
		streamCursors []string
		// blockDuration is the XREADBLOCK timeout.
		blockDuration time.Duration
		// maxPolled is the maximum number of events to read in one
		// XREADBLOCK call.
		maxPolled int64
		// buffer size of the reader channel.
		bufferSize int
		// channels to send notifications
		chans []chan *Event
		// startOnce is used to ensure the reader is started only once.
		startOnce sync.Once
		// closeOnce is used to ensure the reader is closed only once.
		closeOnce sync.Once
		// donechan is the reader donechan channel.
		donechan chan struct{}
		// ctx bounds blocking Redis reads for the reader lifetime.
		ctx context.Context
		// cancel interrupts a blocking XREAD when Close begins.
		cancel context.CancelFunc
		// wait is the reader cleanup wait group.
		wait sync.WaitGroup
		// closing is true from the instant Close begins.
		closing atomic.Bool
		// eventFilter is the event filter if any.
		eventFilter eventFilterFunc
		// logger is the logger used by the reader.
		logger pulse.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// Acker is the interface used by events to acknowledge themselves.
	Acker interface {
		XAck(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd
	}

	// Event is a stream event.
	Event struct {
		// ID is the unique event ID.
		ID string
		// StreamName is the name of the stream the event belongs to.
		StreamName string
		// StreamGeneration is the immutable stream incarnation that produced the
		// event. Events fetched before Destroy may still be in process, so
		// handlers that fence side effects can compare this token explicitly.
		StreamGeneration string
		// SinkName is the name of the sink the event belongs to.
		SinkName string
		// EventName is the producer-defined event name.
		EventName string
		// Topic is the producer-defined event topic if any, empty string if none.
		Topic string
		// Payload is the event payload.
		Payload []byte
		// Acker acknowledges events according to their sink recovery contract.
		Acker Acker
		// streamKey is the Redis key of the stream.
		streamKey string
	}
)

const (
	readRetryInitialDelay = 100 * time.Millisecond
	readRetryMaxDelay     = 5 * time.Second
)

var (
	// ErrReaderClosed is returned when stream ownership is changed after reader
	// shutdown begins.
	ErrReaderClosed = errors.New("pulse streaming: reader is closed")
	// ErrLastStream is returned when removing a stream would leave a reader or
	// sink without a valid Redis read set.
	ErrLastStream = errors.New("pulse streaming: cannot remove final stream")
)

// newReader creates a new reader.
func newReader(ctx context.Context, stream *Stream, opts ...options.Reader) (*Reader, error) {
	o := options.ParseReaderOptions(opts...)
	if err := validateReaderOptions(o); err != nil {
		return nil, err
	}
	var eventFilter eventFilterFunc
	if o.Topic != "" {
		eventFilter = func(e *Event) bool { return e.Topic == o.Topic }
	} else if o.TopicPattern != "" {
		topicPatternRegexp, err := regexp.Compile(o.TopicPattern)
		if err != nil {
			return nil, fmt.Errorf("topic pattern must be a valid regex: %w", err)
		}
		eventFilter = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
	}

	logCtx := context.Background()
	logCtx = log.WithContext(logCtx, ctx)
	runCtx, cancel := context.WithCancel(logCtx)
	reader := &Reader{
		startID:       o.LastEventID,
		streams:       []*Stream{stream},
		streamKeys:    []string{stream.key},
		streamCursors: []string{o.LastEventID},
		blockDuration: o.BlockDuration,
		maxPolled:     o.MaxPolled,
		bufferSize:    o.BufferSize,
		donechan:      make(chan struct{}),
		ctx:           runCtx,
		cancel:        cancel,
		eventFilter:   eventFilter,
		logger:        stream.rootLogger.WithPrefix("reader", stream.Name),
		rdb:           stream.rdb,
	}

	return reader, nil
}

// Subscribe returns a channel that receives events from the stream. The
// channel is closed when the reader closes; calls made after shutdown starts
// return an already closed channel.
func (r *Reader) Subscribe() <-chan *Event {
	c := make(chan *Event, r.bufferSize)
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closing.Load() {
		close(c)
		return c
	}
	r.chans = append(r.chans, c)
	r.start()
	return c
}

// Unsubscribe removes the channel from the reader subscribers and closes it.
func (r *Reader) Unsubscribe(c <-chan *Event) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for i, ch := range r.chans {
		if ch == c {
			close(ch)
			r.chans = append(r.chans[:i], r.chans[i+1:]...)
			return
		}
	}
}

// AddStream adds the stream to the reader. By default the stream cursor starts
// at the same timestamp as the main stream cursor. This can be overridden with
// opts. AddStream does nothing if the stream is already part of the reader and
// returns ErrReaderClosed once shutdown starts.
func (r *Reader) AddStream(ctx context.Context, stream *Stream, opts ...options.AddStream) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closing.Load() {
		return ErrReaderClosed
	}
	o := options.ParseAddStreamOptions(opts...)
	if err := validateAddStreamOptions(o); err != nil {
		return err
	}
	if err := stream.verifyExistingGeneration(ctx); err != nil {
		return err
	}
	for _, owned := range r.streams {
		if owned.Name != stream.Name {
			continue
		}
		if owned.generation == stream.generation {
			return nil
		}
		return owned.verifyGeneration(ctx)
	}
	startID := r.startID
	if o.LastEventID != "" {
		startID = o.LastEventID
	}
	r.streams = append(r.streams, stream)
	r.streamKeys = append(r.streamKeys, stream.key)
	r.streamCursors = append(r.streamCursors, startID)
	r.logger.Info("added", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the reader. It is idempotent and returns
// ErrReaderClosed once shutdown starts. Removing the final stream returns
// ErrLastStream.
func (r *Reader) RemoveStream(ctx context.Context, stream *Stream) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closing.Load() {
		return ErrReaderClosed
	}
	index := -1
	for i, st := range r.streams {
		if st == stream ||
			(stream.generation != "" &&
				st.Name == stream.Name &&
				st.generation == stream.generation) {
			index = i
			break
		}
	}
	if index == -1 {
		return nil
	}
	if len(r.streams) == 1 {
		return ErrLastStream
	}
	attached := r.streams[index]
	if err := attached.verifyExistingGeneration(ctx); err != nil {
		return err
	}
	r.streams = append(r.streams[:index], r.streams[index+1:]...)
	r.streamKeys = append(r.streamKeys[:index], r.streamKeys[index+1:]...)
	r.streamCursors = append(r.streamCursors[:index], r.streamCursors[index+1:]...)
	r.logger.Info("removed", "stream", stream.Name)
	return nil
}

// Close stops event polling and closes the reader channel. It is safe to call
// Close multiple times; concurrent callers block until the first Close
// completes. Close returns only once the read goroutine has stopped and its
// resources are released. The configured finite block duration bounds shutdown
// even when the Redis client does not interrupt a blocking read on cancellation.
func (r *Reader) Close() {
	r.closeOnce.Do(func() {
		r.closing.Store(true)
		r.cancel()
		// Close donechan first, without holding the lock, so the signal
		// reaches the read loop even when it is parked on a fan-out send
		// to a stalled subscriber (which holds the lock). Otherwise Close
		// would deadlock acquiring the lock the read loop never releases.
		close(r.donechan)
		// Synchronize with a Subscribe already inside the admission lock so its
		// wait-group Add completes before Wait begins. Future subscriptions see
		// closing and cannot start the read loop.
		r.lock.Lock()
		r.lock.Unlock()
		r.wait.Wait()
		r.lock.Lock()
		defer r.lock.Unlock()
		r.closed = true
		r.logger.Info("stopped")
	})
}

// IsClosed returns true if the reader is stopped.
func (r *Reader) IsClosed() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closed
}

// start starts the reader's read goroutine if it is not already running.
func (r *Reader) start() {
	r.startOnce.Do(func() {
		r.wait.Add(1)
		pulse.Go(r.logger, r.read)
	})
}

// xreadFn fetches the next batch of events for a reader. It is a package
// variable (rather than a struct field) so tests can simulate read errors
// without polluting Reader; it defaults to (*Reader).xread.
var xreadFn = (*Reader).xread

// read reads events from the streams and sends them to the reader channel.
func (r *Reader) read() {
	defer r.cleanup()
	var retry readRetry
	for {
		snapshot, err := r.readSnapshot(r.ctx)
		if err != nil {
			if fatal := fatalReadError(err); fatal != nil {
				pulse.Go(r.logger, r.Close)
				return
			}
			if !retry.wait(r.donechan, err, r.logger) {
				return
			}
			continue
		}
		streamsEvents, err := xreadFn(r, r.ctx, snapshot.args)
		if r.isClosing() {
			return
		}
		if err != nil {
			if err := fatalReadError(err); err != nil {
				r.logger.Error(fmt.Errorf("fatal error while reading events: %w, stopping", err))
				// Close waits on this goroutine via wait.Wait, so calling it
				// synchronously here would deadlock and leak the reader and its
				// Redis connection. Trigger the shutdown asynchronously and let
				// this goroutine return so cleanup can release the wait group.
				pulse.Go(r.logger, r.Close)
				return
			}
			if err == redis.Nil {
				retry.reset()
				continue
			}
			if !retry.wait(r.donechan, err, r.logger) {
				return
			}
			continue
		}
		retry.reset()

		for _, events := range streamsEvents {
			stream := snapshot.streams[events.Stream]
			if stream == nil {
				continue
			}
			if verifyErr := stream.verifyGeneration(r.ctx); verifyErr != nil {
				err = verifyErr
				break
			}
			r.lock.Lock()
			if !r.ownsStream(stream) {
				r.lock.Unlock()
				continue
			}
			if err := streamEvents(
				r.ctx,
				stream,
				"",
				events.Messages,
				false,
				r.eventFilter,
				r.chans,
				r.donechan,
				r.rdb,
				r.logger,
			); err != nil {
				r.logger.Error(fmt.Errorf("failed to stream reader events: %w", err))
				r.lock.Unlock()
				continue
			}
			for i := range r.streamKeys {
				if r.streamKeys[i] == events.Stream {
					r.streamCursors[i] = events.Messages[len(events.Messages)-1].ID
					break
				}
			}
			r.lock.Unlock()
		}
		if fatal := fatalReadError(err); fatal != nil {
			pulse.Go(r.logger, r.Close)
			return
		}
	}
}

// readSnapshot verifies and captures the exact stream capabilities used by one
// Redis read.
func (r *Reader) readSnapshot(ctx context.Context) (readerSnapshot, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	snapshot := readerSnapshot{
		streams: make(map[string]*Stream, len(r.streams)),
		args:    make([]string, 0, len(r.streamKeys)+len(r.streamCursors)),
	}
	for _, stream := range r.streams {
		if err := stream.verifyGeneration(ctx); err != nil {
			return readerSnapshot{}, err
		}
		snapshot.streams[stream.key] = stream
	}
	snapshot.args = append(snapshot.args, r.streamKeys...)
	snapshot.args = append(snapshot.args, r.streamCursors...)
	return snapshot, nil
}

func (r *Reader) xread(ctx context.Context, readStreams []string) ([]redis.XStream, error) {
	r.logger.Debug("reading", "streams", readStreams, "max", r.maxPolled, "block", r.blockDuration)
	return r.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: readStreams,
		Count:   r.maxPolled,
		Block:   r.blockDuration,
	}).Result()
}

// ownsStream reports whether the current reader still owns the exact snapshot
// capability after a concurrent RemoveStream.
func (r *Reader) ownsStream(candidate *Stream) bool {
	for _, stream := range r.streams {
		if stream == candidate {
			return true
		}
	}
	return false
}

// cleanup removes the consumer from the consumer groups and removes the reader
// from the readers map. This method is called automatically when the reader is
// stopped.
func (r *Reader) cleanup() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, c := range r.chans {
		close(c)
	}
	r.chans = nil
	r.wait.Done()
}

// isClosing returns true if the reader is stopping.
func (r *Reader) isClosing() bool {
	return r.closing.Load()
}

// CreatedAt returns the event creation time (millisecond precision).
func (e *Event) CreatedAt() time.Time {
	tss := e.ID[:strings.IndexByte(e.ID, '-')]
	ts, _ := strconv.ParseInt(tss, 10, 64)
	seconds := ts / 1000
	nanos := (ts % 1000) * 1_000_000
	return time.Unix(seconds, nanos).UTC()
}

// streamEvents filters and streams Redis messages. Sink events receive a
// recovery-aware Acker; auto-ack sinks acknowledge each message before it can
// be exposed to filters or subscribers.
func streamEvents(
	ctx context.Context,
	stream *Stream,
	sinkName string,
	msgs []redis.XMessage,
	autoAck bool,
	eventFilter eventFilterFunc,
	chans []chan *Event,
	done <-chan struct{},
	rdb *redis.Client,
	logger pulse.Logger,
) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, event := range msgs {
		name, topic, payload, err := decodeRedisEvent(event)
		if err != nil {
			if sinkName != "" {
				acker := &recoveryAcker{stream: stream}
				if ackErr := acker.XAck(ctx, stream.key, sinkName, event.ID).Err(); ackErr != nil {
					return errors.Join(err, fmt.Errorf("acknowledge malformed sink event %s: %w", event.ID, ackErr))
				}
			}
			logger.Error(err, "stream", stream.Name, "id", event.ID)
			continue
		}
		var acker Acker = rdb
		if sinkName != "" {
			acker = &recoveryAcker{stream: stream}
		}
		ev := &Event{
			ID:               event.ID,
			StreamName:       stream.Name,
			StreamGeneration: stream.generation,
			SinkName:         sinkName,
			EventName:        name,
			Topic:            topic,
			Payload:          payload,
			streamKey:        stream.key,
			Acker:            acker,
		}
		if autoAck {
			if err := ev.Acker.XAck(ctx, stream.key, sinkName, event.ID).Err(); err != nil {
				return err
			}
		}
		if eventFilter != nil && !eventFilter(ev) {
			if sinkName != "" && !autoAck {
				if err := ev.Acker.XAck(ctx, stream.key, sinkName, event.ID).Err(); err != nil {
					return fmt.Errorf("failed to acknowledge filtered sink event %s: %w", event.ID, err)
				}
			}
			logger.Debug("event filtered", "event", ev.EventName, "id", ev.ID, "stream", stream.Name)
			continue
		}
		logger.Debug("event", "stream", stream.Name, "event", ev.EventName, "id", ev.ID, "channels", len(chans))
		for _, c := range chans {
			select {
			case c <- ev:
			case <-done:
				// The reader/sink is closing; stop fanning out so the
				// read loop can return and release its resources instead
				// of blocking forever on a stalled subscriber. Any
				// remaining subscribers and messages in this batch are
				// abandoned: delivery is at-most-once and the reader/sink
				// is being torn down, so partial fan-out is acceptable.
				return nil
			}
		}
	}
	return nil
}

// decodeRedisEvent validates the externally writable Redis stream boundary.
func decodeRedisEvent(event redis.XMessage) (string, string, []byte, error) {
	name, ok := event.Values[nameKey].(string)
	if !ok || name == "" {
		return "", "", nil, fmt.Errorf(
			"pulse streaming: malformed event %s: required field %q must be a non-empty string",
			event.ID,
			nameKey,
		)
	}
	payload, ok := event.Values[payloadKey].(string)
	if !ok {
		return "", "", nil, fmt.Errorf(
			"pulse streaming: malformed event %s: required field %q must be a string",
			event.ID,
			payloadKey,
		)
	}
	var topic string
	if value, exists := event.Values[topicKey]; exists {
		var valid bool
		topic, valid = value.(string)
		if !valid {
			return "", "", nil, fmt.Errorf(
				"pulse streaming: malformed event %s: optional field %q must be a string",
				event.ID,
				topicKey,
			)
		}
	}
	return name, topic, []byte(payload), nil
}

// fatalReadError returns errors that permanently invalidate a reader. All
// other errors are transient and are retried through readRetry.
func fatalReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStreamDestroyed) || errors.Is(err, ErrDeadlineElapsed) {
		return err
	}
	return nil
}

// reset restores the retry delay after Redis processes a read or repairs a
// missing consumer group.
func (r *readRetry) reset() {
	r.failures = 0
}

// nextDelay returns a half-to-full-jitter exponential delay. The non-zero
// lower bound prevents a hot loop while randomization avoids fleet-wide retry
// synchronization.
func (r *readRetry) nextDelay() time.Duration {
	limit := readRetryInitialDelay
	for range r.failures {
		limit = min(limit*2, readRetryMaxDelay)
		if limit == readRetryMaxDelay {
			break
		}
	}
	if limit < readRetryMaxDelay {
		r.failures++
	}
	jitter := r.jitter
	if jitter == nil {
		jitter = rand.Int63n
	}
	floor := limit / 2
	return floor + time.Duration(jitter(int64(limit-floor)+1))
}

// wait applies the next bounded jittered delay. It returns false when shutdown
// interrupts the wait so Close does not wait for a retry timer.
func (r *readRetry) wait(done <-chan struct{}, err error, logger pulse.Logger) bool {
	delay := r.nextDelay()
	logger.Error(fmt.Errorf("failed to read events: %w", err), "retry_in", delay)

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	}
}
