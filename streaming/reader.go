package streaming

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
)

type (
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
		// donechan is the reader donechan channel.
		donechan chan struct{}
		// streamschan notifies the reader when streams are added or
		// removed.
		streamschan chan struct{}
		// wait is the reader cleanup wait group.
		wait sync.WaitGroup
		// closing is true if Close was called.
		closing bool
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
		// SinkName is the name of the sink the event belongs to.
		SinkName string
		// EventName is the producer-defined event name.
		EventName string
		// Topic is the producer-defined event topic if any, empty string if none.
		Topic string
		// Payload is the event payload.
		Payload []byte
		// Acker is the redis client used to acknowledge events.
		Acker Acker
		// streamKey is the Redis key of the stream.
		streamKey string
	}
)

// newReader creates a new reader.
func newReader(ctx context.Context, stream *Stream, opts ...options.Reader) (*Reader, error) {
	o := options.ParseReaderOptions(opts...)
	var eventFilter eventFilterFunc
	if o.Topic != "" {
		eventFilter = func(e *Event) bool { return e.Topic == o.Topic }
	} else if o.TopicPattern != "" {
		topicPatternRegexp := regexp.MustCompile(o.TopicPattern)
		eventFilter = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
	}

	reader := &Reader{
		startID:       o.LastEventID,
		streams:       []*Stream{stream},
		streamKeys:    []string{stream.key},
		streamCursors: []string{o.LastEventID},
		blockDuration: o.BlockDuration,
		maxPolled:     o.MaxPolled,
		bufferSize:    o.BufferSize,
		donechan:      make(chan struct{}),
		streamschan:   make(chan struct{}),
		eventFilter:   eventFilter,
		logger:        stream.rootLogger.WithPrefix("reader", stream.Name),
		rdb:           stream.rdb,
	}

	reader.wait.Add(1)
	pulse.Go(ctx, reader.read)

	return reader, nil
}

// Subscribe returns a channel that receives events from the stream.
// The channel is closed when the reader is closed.
func (r *Reader) Subscribe() <-chan *Event {
	c := make(chan *Event, r.bufferSize)
	r.lock.Lock()
	defer r.lock.Unlock()
	r.chans = append(r.chans, c)
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

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink.
func (r *Reader) AddStream(ctx context.Context, stream *Stream, opts ...options.AddStream) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, name := range r.streamKeys {
		if name == stream.Name {
			return nil
		}
	}
	startID := r.startID
	o := options.ParseAddStreamOptions(opts...)
	if o.LastEventID != "" {
		startID = o.LastEventID
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

// IsClosed returns true if the reader is stopped.
func (r *Reader) IsClosed() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closed
}

// read reads events from the streams and sends them to the reader channel.
func (r *Reader) read() {
	ctx := context.Background()
	defer r.cleanup()
	for {
		streamsEvents, err := r.xread(ctx)
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
			streamEvents(streamName, events.Stream, "", events.Messages, r.eventFilter, r.chans, r.rdb, r.logger)
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
	// copy so no two goroutines can share the memory
	r.lock.Lock()
	readStreams := make([]string, len(r.streamKeys))
	copy(readStreams, r.streamKeys)
	readStreams = append(readStreams, r.streamCursors...)
	r.lock.Unlock()

	r.logger.Debug("reading", "streams", readStreams, "max", r.maxPolled, "block", r.blockDuration)
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
	for _, c := range r.chans {
		close(c)
	}
	r.wait.Done()
}

// isClosing returns true if the reader is stopping.
func (r *Reader) isClosing() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closing
}

// CreatedAt returns the event creation time (millisecond precision).
func (e *Event) CreatedAt() time.Time {
	tss := e.ID[:strings.IndexByte(e.ID, '-')]
	ts, _ := strconv.ParseInt(tss, 10, 64)
	seconds := ts / 1000
	nanos := (ts % 1000) * 1_000_000
	return time.Unix(seconds, nanos).UTC()
}

// streamEvents filters and streams the Redis messages as events to c.
// The caller is responsible for locking c.
func streamEvents(
	streamName string,
	streamKey string,
	sinkName string,
	msgs []redis.XMessage,
	eventFilter eventFilterFunc,
	chans []chan *Event,
	rdb *redis.Client,
	logger pulse.Logger,
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
			Acker:      rdb,
		}
		if eventFilter != nil && !eventFilter(ev) {
			logger.Debug("event filtered", "event", ev.EventName, "id", ev.ID, "stream", streamName)
			continue
		}
		logger.Debug("event", "stream", streamName, "event", ev.EventName, "id", ev.ID, "channels", len(chans))
		for _, c := range chans {
			c <- ev
		}
	}
}

// handleReadError retries retryable read errors and ignores non-retryable.
func handleReadError(err error, logger pulse.Logger) error {
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
