package streaming

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/ponos/ponos"
)

type (
	// Reader represents a stream reader.
	Reader struct {
		// C is the reader event channel.
		C <-chan *Event
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
		// c is the reader event channel.
		c chan *Event
		// donechan is the reader donechan channel.
		donechan chan struct{}
		// streamschan notifies the reader when streams are added or
		// removed.
		streamschan chan struct{}
		// wait is the reader cleanup wait group.
		wait sync.WaitGroup
		// closing is true if Close was called.
		closing bool
		// eventMatcher is the event matcher if any.
		eventMatcher EventMatcherFunc
		// logger is the logger used by the reader.
		logger ponos.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// Event is a stream event.
	Event struct {
		// ID is the unique event ID.
		ID string
		// StreamName is the name of the stream the event belongs to.
		StreamName string
		// SinkName is the name of the sink the event belongs to.
		SinkName string
		// EventName is the producer-defined event name.
		EventName string
		// Topic is the producer-defined event topic if any, empty string if none.
		Topic string
		// Payload is the event payload.
		Payload []byte
		// streamKey is the Redis key of the stream.
		streamKey string
		// rdb is the redis client.
		rdb *redis.Client
	}
)

// newReader creates a new reader.
func newReader(ctx context.Context, stream *Stream, opts ...ReaderOption) (*Reader, error) {
	options := defaultReaderOptions()
	for _, option := range opts {
		option(&options)
	}
	c := make(chan *Event, options.BufferSize)
	eventMatcher := options.EventMatcher
	if eventMatcher == nil {
		if options.Topic != "" {
			eventMatcher = func(e *Event) bool { return e.Topic == options.Topic }
		} else if options.TopicPattern != "" {
			topicPatternRegexp := regexp.MustCompile(options.TopicPattern)
			eventMatcher = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
		}
	}

	reader := &Reader{
		C:             c,
		startID:       options.LastEventID,
		streams:       []*Stream{stream},
		streamKeys:    []string{stream.key},
		streamCursors: []string{options.LastEventID},
		blockDuration: options.BlockDuration,
		maxPolled:     options.MaxPolled,
		c:             c,
		donechan:      make(chan struct{}),
		streamschan:   make(chan struct{}),
		eventMatcher:  eventMatcher,
		logger:        stream.rootLogger,
		rdb:           stream.rdb,
	}

	reader.wait.Add(1)
	go reader.read()

	return reader, nil
}

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink.
func (r *Reader) AddStream(ctx context.Context, stream *Stream, opts ...AddStreamOption) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, name := range r.streamKeys {
		if name == stream.Name {
			return nil
		}
	}
	startID := r.startID
	options := defaultAddStreamOptions()
	for _, option := range opts {
		option(&options)
	}
	if options.LastEventID != "" {
		startID = options.LastEventID
	}
	r.streams = append(r.streams, stream)
	r.streamKeys = append(r.streamKeys, stream.key)
	r.streamCursors = append(r.streamCursors, startID)
	r.notifyStreamChange()
	r.logger.Info("added", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent.
func (r *Reader) RemoveStream(ctx context.Context, stream *Stream) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for i, st := range r.streams {
		if st == stream {
			r.streams = append(r.streams[:i], r.streams[i+1:]...)
			r.streamKeys = append(r.streamKeys[:i], r.streamKeys[i+1:]...)
			r.streamCursors = append(r.streamCursors[:i], r.streamCursors[i+1:]...)
			break
		}
	}
	r.notifyStreamChange()
	r.logger.Info("removed", "stream", stream.Name)
	return nil
}

// Close stops event polling and closes the reader channel. It is safe to call
// Close multiple times.
func (r *Reader) Close() {
	r.lock.Lock()
	if r.closing {
		return
	}
	r.closing = true
	close(r.donechan)
	close(r.streamschan)
	r.lock.Unlock()
	r.wait.Wait()
	r.lock.Lock()
	defer r.lock.Unlock()
	r.closed = true
	r.logger.Info("stopped")
}

// Closed returns true if the reader is stopped.
func (r *Reader) Closed() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closed
}

// read reads events from the streams and sends them to the reader channel.
func (r *Reader) read() {
	ctx := context.Background()
	defer r.cleanup()
	for {
		streamsEvents, err := readOnce(ctx, r.xread, r.streamschan, r.donechan, r.logger)
		if r.isClosing() {
			return
		}
		if err != nil {
			if err := handleReadError(err, r.logger); err != nil {
				r.logger.Error(fmt.Errorf("fatal error while reading events: %w, stopping", err))
				r.Close()
				return
			}
			continue
		}

		r.lock.Lock()
		for _, events := range streamsEvents {
			streamName := events.Stream[len(streamKeyPrefix):]
			streamEvents(streamName, events.Stream, "", events.Messages, r.eventMatcher, r.c, r.rdb, r.logger)
			for i := range r.streamKeys {
				if r.streamKeys[i] == events.Stream {
					r.streamCursors[i] = events.Messages[len(events.Messages)-1].ID
					break
				}
			}
		}
		r.lock.Unlock()
	}
}

func (r *Reader) xread(ctx context.Context) ([]redis.XStream, error) {
	r.lock.Lock()
	// force copy so no two goroutines can share the memory
	readStreams := make([]string, len(r.streamKeys))
	copy(readStreams, r.streamKeys)
	readStreams = append(readStreams, r.streamCursors...)
	r.lock.Unlock()

	if r.blockDuration > 4*time.Second {
		r.logger.Debug("reading", "streams", readStreams, "max", r.maxPolled, "block", r.blockDuration)
	}
	return r.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: readStreams,
		Count:   r.maxPolled,
		Block:   r.blockDuration,
	}).Result()
}

// notifyStreamChange notifies the reader that the streams have changed.
func (r *Reader) notifyStreamChange() {
	select {
	case r.streamschan <- struct{}{}:
	default:
	}
}

// cleanup removes the consumer from the consumer groups and removes the reader
// from the readers map. This method is called automatically when the reader is
// stopped.
func (r *Reader) cleanup() {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.c)
	r.wait.Done()
}

// isClosing returns true if the reader is stopping.
func (r *Reader) isClosing() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closing
}

// readOnce calls the provided readFn and returns the events or error.
// readOnce cancels the context if the reader is stopping or if the streams
// attached to the reader have changed.
// NOTE: the Redis client does not currently support context cancellation.
// See https://github.com/redis/go-redis/issues/2276. This means the read
// will block until the timeout is reached.
func readOnce(
	ctx context.Context,
	readFn func(context.Context) ([]redis.XStream, error),
	streamschan chan struct{},
	donechan chan struct{},
	logger ponos.Logger,
) ([]redis.XStream, error) {

	readchan := make(chan []redis.XStream)
	errchan := make(chan error)
	defer close(readchan)
	defer close(errchan)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		events, err := readFn(cctx)
		if err != nil {
			if cctx.Err() != nil {
				err = nil
			}
			errchan <- err
			return
		}
		readchan <- events
	}()
	select {
	case events := <-readchan:
		return events, nil
	case err := <-errchan:
		return nil, err
	case <-streamschan:
		cancel()
		logger.Debug("reading aborted", "reason", "streams changed")
	case <-donechan:
		cancel()
		logger.Debug("reading aborted", "reason", "reader stopped")
	}
	for {
		select {
		case events := <-readchan:
			return events, nil
		case err := <-errchan:
			return nil, err
		}
	}
}

// streamEvents filters and streams the Redis messages as events to c.
// The caller is responsible for locking c.
func streamEvents(
	streamName string,
	streamKey string,
	sinkName string,
	msgs []redis.XMessage,
	eventMatcher EventMatcherFunc,
	c chan *Event,
	rdb *redis.Client,
	logger ponos.Logger,
) {
	if len(msgs) == 0 {
		return
	}
	for _, event := range msgs {
		var topic string
		if t, ok := event.Values[topicKey]; ok {
			topic = t.(string)
		}
		ev := &Event{
			ID:         event.ID,
			StreamName: streamName,
			SinkName:   sinkName,
			EventName:  event.Values[nameKey].(string),
			Topic:      topic,
			Payload:    []byte(event.Values[payloadKey].(string)),
			streamKey:  streamKey,
			rdb:        rdb,
		}
		if eventMatcher != nil && !eventMatcher(ev) {
			logger.Debug("event did not match event matcher", "stream", streamName, "event", ev.ID)
			continue
		}
		logger.Debug("event", "stream", streamName, "event", ev.ID)
		c <- ev
	}
}

// handleReadError retries retryable read errors and ignores non-retryable.
func handleReadError(err error, logger ponos.Logger) error {
	if strings.Contains(err.Error(), "stream key no longer exists") {
		return err // Fatal error
	}
	if err == redis.Nil {
		return nil // No event at this time, just loop
	}
	if strings.Contains(err.Error(), "NOGROUP") {
		return nil // Consumer group was removed with RemoveStream, just loop (s.streamCursors will be updated)
	}
	// Retryable error, sleep and loop
	d := time.Duration(rand.Intn(maxJitterMs)) * time.Millisecond
	logger.Error(fmt.Errorf("failed to read events: %w, retrying in %v", err, d))
	time.Sleep(d)
	return nil
}
