package streaming

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
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
		// stopped is true if Stop completed.
		stopped bool
		// startID is the reader start event ID.
		startID string
		// lock is the reader mutex.
		lock sync.Mutex
		// streams are the streams the reader consumes events from.
		streams []*Stream
		// streamNames is the stream names used to read events in
		// the same order as streamCursors
		streamNames []string
		// streamCursors is the stream cursors used to read events in
		// the same order as streamNames
		streamCursors []string
		// readchan is used to communicate the results of XREADBLOCK
		readchan chan []redis.XStream
		// errchan is used to communicate errors from XREADGROUP.
		errchan chan error
		// c is the reader event channel.
		c chan *Event
		// done is the reader done channel.
		done chan struct{}
		// wait is the reader cleanup wait group.
		wait sync.WaitGroup
		// stopping is true if Stop was called.
		stopping bool
		// eventMatcher is the event matcher if any.
		eventMatcher EventMatcherFunc
		// logger is the logger used by the reader.
		logger ponos.Logger
		// rdb is the redis connection.
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
		streamNames:   []string{stream.Name},
		streamCursors: []string{options.LastEventID},
		readchan:      make(chan []redis.XStream),
		errchan:       make(chan error),
		c:             c,
		done:          make(chan struct{}),
		eventMatcher:  eventMatcher,
		logger:        stream.logger,
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
	for _, name := range r.streamNames {
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
	r.streamNames = append(r.streamNames, stream.Name)
	r.streamCursors = append(r.streamCursors, startID)
	r.logger.Info("added stream to reader", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent.
func (r *Reader) RemoveStream(ctx context.Context, stream *Stream) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for i, st := range r.streams {
		if st == stream {
			r.streams = append(r.streams[:i], r.streams[i+1:]...)
			r.streamNames = append(r.streamNames[:i], r.streamNames[i+1:]...)
			r.streamCursors = append(r.streamCursors[:i], r.streamCursors[i+1:]...)
			break
		}
	}
	r.logger.Info("removed stream from reader", "stream", stream.Name)
	return nil
}

// Stop stops event polling and closes the reader channel, it is idempotent.
func (r *Reader) Stop() {
	r.lock.Lock()
	if r.stopping {
		return
	}
	r.stopping = true
	close(r.done)
	r.lock.Unlock()
	r.wait.Wait()
	r.lock.Lock()
	defer r.lock.Unlock()
	r.stopped = true
	r.logger.Info("stopped")
}

// Stopped returns true if the reader is stopped.
func (r *Reader) Stopped() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.stopped
}

// read reads events from the streams and sends them to the reader channel.
func (r *Reader) read() {
	defer r.cleanup()
	for {
		events, err := r.readOnce()
		if r.isStopping() {
			return
		}
		if err != nil {
			if err == redis.Nil {
				continue // No event at this time, just loop
			}
			d := time.Duration(rand.Intn(maxJitterMs)) * time.Millisecond
			r.logger.Error(fmt.Errorf("reader failed to read events: %w, retrying in %v", err, d))
			time.Sleep(d)
			continue // Full jitter eternal retry
		}

		var evs []*Event
		for _, streamEvents := range events {
			if len(streamEvents.Messages) == 0 {
				continue
			}
			for _, event := range streamEvents.Messages {
				var topic string
				if t, ok := event.Values["topic"]; ok {
					topic = t.(string)
				}
				ev := newEvent(
					r.rdb,
					streamEvents.Stream,
					"",
					event.ID,
					event.Values[nameKey].(string),
					topic,
					[]byte(event.Values[payloadKey].(string)),
				)
				if r.eventMatcher != nil && !r.eventMatcher(ev) {
					r.logger.Debug("event did not match event matcher", "stream", streamEvents.Stream, "event", ev.ID)
					continue
				}
				r.logger.Debug("event received", "stream", streamEvents.Stream, "event", ev.ID)
				evs = append(evs, ev)
			}
		}

		r.lock.Lock()
		for _, ev := range evs {
			r.c <- ev
		}
		r.lock.Unlock()
	}
}

// readOnce reads events from the streams using the backing consumer group.  It
// blocks up to blockMs milliseconds or until the reader is stopped if no events
// are available.
func (r *Reader) readOnce() ([]redis.XStream, error) {
	ctx := context.Background()

	go func() {
		events, err := r.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: append(r.streamNames, r.streamCursors...),
			Count:   maxMessages,
			Block:   pollDuration,
		}).Result()
		func() {
			r.lock.Lock()
			defer r.lock.Unlock()
			if r.stopping {
				return
			}
			if err != nil {
				r.errchan <- err
				return
			}
			r.readchan <- events
		}()
	}()

	select {
	case events := <-r.readchan:
		return events, nil
	case err := <-r.errchan:
		return nil, err
	case <-r.done:
		return nil, nil
	}
}

// cleanup removes the consumer from the consumer groups and removes the reader
// from the readers map. This method is called automatically when the reader is
// stopped.
func (r *Reader) cleanup() {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.readchan)
	close(r.errchan)
	close(r.c)
	r.wait.Done()
}

// isStopping returns true if the reader is stopping.
func (r *Reader) isStopping() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.stopping
}
