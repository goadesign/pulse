// Replay readers check a retained position and copy its successors atomically.
// They never establish or repair lifecycle state, and expose terminal errors
// instead of silently skipping malformed entries or retrying failed reads.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type (
	// ReplayReaderOptions requires explicit positive replay budgets.
	ReplayReaderOptions struct {
		// MaxEvents limits the complete events copied in one batch.
		MaxEvents int64
		// MaxBytes limits the sum of IDs, names, topics and payloads in a
		// batch. The opening anchor has a separate equal allowance. This
		// does not bound raw entry materialization or a wakeup response.
		MaxBytes int64
		// BlockDuration bounds each tail wait, at millisecond precision.
		// It must be at least one millisecond; there is no default.
		BlockDuration time.Duration
	}

	// ReplayReader observes one existing stream after a retained Redis ID.
	// It supports append and prefix trimming, not arbitrary interior deletion.
	// Its caller context owns both opening and the entire observation lifetime.
	ReplayReader struct {
		options      ReplayReaderOptions
		name         string
		lifecycleKey string
		baseKey      string
		identity     replayIdentity
		position     string
		anchor       SnapshotEvent
		hasAnchor    bool
		events       chan *Event
		done         chan struct{}
		ctx          context.Context
		cancel       context.CancelCauseFunc
		rdb          *redis.Client
		lock         sync.Mutex
		err          error
	}

	// replayIdentity is immutable once opening has checked the saved lifecycle.
	replayIdentity struct {
		generation string
		physical   string
		deadline   string
		retention  string
	}
)

var (
	// ErrInvalidReplayPosition means the supplied position is not a canonical
	// pair of unsigned 64-bit Redis ID components.
	ErrInvalidReplayPosition = errors.New("pulse streaming: invalid replay position")
	// ErrReplayPositionUnavailable means the exact position cannot be resumed.
	// It may wrap ErrStreamDestroyed or ErrDeadlineElapsed.
	ErrReplayPositionUnavailable = errors.New("pulse streaming: replay position unavailable")
	// ErrReplayEventTooLarge means the opening anchor or first available event
	// exceeds MaxBytes. No part of that event is delivered or skipped.
	ErrReplayEventTooLarge = errors.New("pulse streaming: replay event exceeds byte budget")

	errReplayClosed = errors.New("pulse streaming: replay deliberately closed")
)

// NewReplayReader checks the existing lifecycle, retained anchor and first batch
// synchronously. "0-0" starts at available history without claiming earlier
// completeness; any other position must identify an exact retained entry.
// No Redis data is written. The Stream handle binds to the checked generation.
func (s *Stream) NewReplayReader(ctx context.Context, afterID string, opts ReplayReaderOptions) (*ReplayReader, error) {
	if !validReplayID(afterID) {
		return nil, ErrInvalidReplayPosition
	}
	if opts.MaxEvents <= 0 || opts.MaxBytes <= 0 {
		return nil, fmt.Errorf("pulse streaming: replay MaxEvents and MaxBytes must be positive")
	}
	if opts.BlockDuration < time.Millisecond {
		return nil, fmt.Errorf("pulse streaming: replay BlockDuration must be at least one millisecond")
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	r := &ReplayReader{
		options:      opts,
		name:         s.Name,
		lifecycleKey: s.lifecycleKey,
		baseKey:      streamKey(s.Name),
		position:     afterID,
		events:       make(chan *Event),
		done:         make(chan struct{}),
		ctx:          runCtx,
		cancel:       cancel,
		rdb:          s.rdb,
	}
	batch, err := r.open(s)
	if err != nil {
		err = r.contextError(err)
		cancel(errReplayClosed)
		return nil, err
	}
	go r.read(batch)
	return r, nil
}

// Anchor returns the independently owned opening entry, or false for "0-0".
// It is not automatically sent on Subscribe. SnapshotEvent.Payload returns a copy.
func (r *ReplayReader) Anchor() (SnapshotEvent, bool) {
	return r.anchor, r.hasAnchor
}

// Subscribe returns the reader's single unbuffered channel. Repeated calls
// return that same channel, not independent subscriptions.
func (r *ReplayReader) Subscribe() <-chan *Event {
	return r.events
}

// Done closes after the reader records Err, closes Subscribe, and finishes its
// current Redis command and read-loop work. It lets a caller observe completion
// without consuming events or initiating Close. Closure does not mean success.
func (r *ReplayReader) Done() <-chan struct{} {
	return r.done
}

// Err returns the first terminal cause, recorded before Subscribe closes.
// Deliberate Close has no error unless a failure already terminated the reader.
func (r *ReplayReader) Err() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.err
}

// Close stops observation and joins the read loop and its current Redis command.
// It does not close the caller's Redis client, modify data, or stop producers.
// An already admitted send may complete. Blocking commands remain subject to
// the configured client transport behavior and finite BlockDuration.
func (r *ReplayReader) Close() {
	r.cancel(errReplayClosed)
	<-r.done
}

// validReplayID preserves exact Redis uint64 components without normalization.
func validReplayID(id string) bool {
	left, right, ok := strings.Cut(id, "-")
	if !ok {
		return false
	}
	for _, part := range []string{left, right} {
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != part {
			return false
		}
	}
	return true
}

// open shares the existing handle's binding lock but does not use its mutating
// generation verification helpers. The reader then owns an immutable identity.
func (r *ReplayReader) open(s *Stream) ([]*Event, error) {
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	bound := s.generation != ""
	if bound {
		r.identity.generation = s.generation
		r.identity.physical = s.key
	}
	if bound || s.retentionExplicit {
		r.identity.retention = s.retention
	}
	batch, err := r.fetch(true)
	if err != nil {
		return nil, err
	}
	deadline, err := parseDeadline(r.identity.deadline)
	if err != nil {
		return nil, fmt.Errorf("%w: replay deadline: %w", ErrStreamConfigMismatch, err)
	}
	if !bound {
		if err := s.applyRetentionConfig(r.identity.retention); err != nil {
			return nil, fmt.Errorf("%w: replay retention: %w", ErrStreamConfigMismatch, err)
		}
		s.generation = r.identity.generation
		s.key = r.identity.physical
		s.deadline = deadline
	}
	return batch, nil
}

// read drains each checked prefix before performing another read. XREAD data
// is never delivered; after any wakeup, fetch must check the retained position.
func (r *ReplayReader) read(batch []*Event) {
	var terminal error
	defer func() {
		r.lock.Lock()
		r.err = terminal
		r.lock.Unlock()
		r.cancel(errReplayClosed)
		close(r.events)
		close(r.done)
	}()
	for {
		for i, event := range batch {
			if r.ctx.Err() != nil {
				terminal = r.contextError(nil)
				return
			}
			// Capture the next position before transferring ownership: a
			// consumer may mutate Event as soon as its receive completes.
			next := event.ID
			select {
			case <-r.ctx.Done():
				terminal = r.contextError(nil)
				return
			case r.events <- event:
				r.position = next
				batch[i] = nil
			}
		}
		if len(batch) == 0 {
			if err := r.wake(); err != nil && !errors.Is(err, redis.Nil) {
				terminal = r.contextError(err)
				return
			}
		}
		var err error
		batch, err = r.fetch(false)
		if err != nil {
			terminal = r.contextError(err)
			return
		}
	}
}

// wake allows one complete Redis entry to arrive, but retains none of its data.
// This entry's pre-admission materialization cost is not bounded by MaxBytes.
func (r *ReplayReader) wake() error {
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}
	return r.rdb.XRead(r.ctx, &redis.XReadArgs{
		Streams: []string{r.identity.physical, r.position},
		Count:   1,
		Block:   r.options.BlockDuration,
	}).Err()
}

// fetch is the only replay delivery source. The script validates structure and
// applies budgets before returning raw fields; existing decoders own Go values.
func (r *ReplayReader) fetch(opening bool) ([]*Event, error) {
	if r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}
	result, err := replayReadScript.Run(r.ctx, r.rdb, []string{r.lifecycleKey},
		r.baseKey, r.identity.generation, r.identity.physical, r.identity.retention,
		r.position, boolString(opening),
		r.options.MaxEvents>>32, r.options.MaxEvents&0xffffffff,
		r.options.MaxBytes>>32, r.options.MaxBytes&0xffffffff,
		streamFormatVersion,
	).Slice()
	if err != nil {
		return nil, replayReadError(err, r.identity.generation != "", r.position)
	}
	if r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}
	if len(result) != 6 {
		return nil, fmt.Errorf("pulse streaming: malformed replay result")
	}
	generation, physical, deadline, retention, err := parseLifecycleIdentity(result[:4])
	if err != nil {
		return nil, fmt.Errorf("%w: replay identity: %w", ErrStreamConfigMismatch, err)
	}
	anchor, ok := result[4].([]any)
	if !ok {
		return nil, fmt.Errorf("pulse streaming: malformed replay anchor")
	}
	raw, ok := result[5].([]any)
	if !ok || int64(len(raw)) > r.options.MaxEvents {
		return nil, fmt.Errorf("pulse streaming: malformed replay batch")
	}
	events, err := decodeReplayEvents(raw, r.name, generation, r.options.MaxBytes)
	if err != nil {
		return nil, err
	}
	if opening && r.position != "0-0" {
		entries, err := decodeReplayEvents(anchor, r.name, generation, r.options.MaxBytes)
		if err != nil {
			return nil, err
		}
		if len(entries) != 1 || entries[0].ID != r.position {
			return nil, fmt.Errorf("pulse streaming: malformed replay anchor identity")
		}
		entry := entries[0]
		r.anchor = SnapshotEvent{
			id:         entry.ID,
			streamName: r.name,
			generation: generation,
			name:       entry.EventName,
			topic:      entry.Topic,
			payload:    entry.Payload,
		}
		r.hasAnchor = true
	} else if len(anchor) != 0 {
		return nil, fmt.Errorf("pulse streaming: unexpected replay anchor")
	}
	r.identity = replayIdentity{generation, physical, deadline, retention}
	return events, nil
}

// decodeReplayEvents preserves the strict field set before the shared range
// decoder builds a map, which would otherwise hide duplicate field names.
func decodeReplayEvents(raw []any, name, generation string, remaining int64) ([]*Event, error) {
	for _, value := range raw {
		entry, ok := value.([]any)
		if !ok || len(entry) != 2 {
			return nil, fmt.Errorf("pulse streaming: malformed replay entry")
		}
		id, ok := entry[0].(string)
		if !ok || !validReplayID(id) || id == "0-0" {
			return nil, fmt.Errorf("pulse streaming: malformed replay event ID")
		}
		fields, ok := entry[1].([]any)
		if !ok || (len(fields) != 4 && len(fields) != 6) {
			return nil, fmt.Errorf("pulse streaming: malformed replay event fields")
		}
		seen := make(map[string]bool, 3)
		for i := 0; i < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok || (key != nameKey && key != payloadKey && key != topicKey) || seen[key] {
				return nil, fmt.Errorf("pulse streaming: malformed replay event fields")
			}
			seen[key] = true
			text, ok := fields[i+1].(string)
			if !ok {
				return nil, fmt.Errorf("pulse streaming: malformed replay event value")
			}
			if int64(len(text)) > remaining {
				return nil, fmt.Errorf("pulse streaming: replay result violated byte admission")
			}
			remaining -= int64(len(text))
		}
		if int64(len(id)) > remaining {
			return nil, fmt.Errorf("pulse streaming: replay result violated byte admission")
		}
		remaining -= int64(len(id))
	}
	messages, err := decodeSnapshotRange(raw)
	if err != nil {
		return nil, err
	}
	events := make([]*Event, len(messages))
	for i, message := range messages {
		eventName, topic, payload, err := decodeRedisEvent(message)
		if err != nil {
			return nil, err
		}
		events[i] = &Event{
			ID:               message.ID,
			StreamName:       name,
			StreamGeneration: generation,
			EventName:        eventName,
			Topic:            topic,
			Payload:          payload,
		}
	}
	return events, nil
}

// replayReadError distinguishes missing continuity from malformed metadata and
// dependency failures. No dependency error is reclassified as normal EOF.
func replayReadError(err error, bound bool, position string) error {
	switch {
	case redis.HasErrorPrefix(err, "REPLAYPOSITIONUNAVAILABLE"):
		return ErrReplayPositionUnavailable
	case redis.HasErrorPrefix(err, "REPLAYEVENTTOOLARGE"):
		return ErrReplayEventTooLarge
	case redis.HasErrorPrefix(err, streamConfigMissingError):
		return fmt.Errorf("%w: missing replay lifecycle configuration", ErrStreamConfigMismatch)
	case redis.HasErrorPrefix(err, "REPLAYMALFORMED"):
		return fmt.Errorf("pulse streaming: malformed replay event")
	}
	mapped := streamLifecycleBoundaryError(err)
	switch {
	case errors.Is(mapped, ErrStreamNotFound):
		if bound || position != "0-0" {
			return fmt.Errorf("%w: %w", ErrReplayPositionUnavailable, ErrStreamNotFound)
		}
	case errors.Is(mapped, ErrStreamDestroyed), errors.Is(mapped, ErrDeadlineElapsed):
		return fmt.Errorf("%w: %w", ErrReplayPositionUnavailable, mapped)
	}
	return mapped
}

// contextError preserves both the context category and a custom caller cause.
// A deliberate Close wins only if no earlier cause canceled this reader.
func (r *ReplayReader) contextError(fallback error) error {
	cause := context.Cause(r.ctx)
	if cause == errReplayClosed {
		return nil
	}
	if cause == nil {
		return fallback
	}
	if errors.Is(cause, r.ctx.Err()) {
		return cause
	}
	return errors.Join(r.ctx.Err(), cause)
}
