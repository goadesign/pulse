// Package streaming adopts pre-generation event data in place, then assigns a
// distinct physical Redis key to every explicitly recreated incarnation.
// Stream handles bind lazily to the lifecycle-selected generation and key.
package streaming

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const (
	streamStateActive            = "active"
	streamStateDestroyed         = "destroyed"
	streamDestroyedError         = "STREAMDESTROYED"
	streamPhysicalKey            = "physical_key"
	streamDeadlineKey            = "deadline_ms"
	streamTTLOwnedKey            = "ttl_owned"
	streamDeadlineError          = "DEADLINEELAPSED"
	streamConflictError          = "STREAMDEADLINECONFLICT"
	streamConfigKey              = "retention_config"
	streamConfigError            = "STREAMCONFIGMISMATCH"
	streamNotFoundError          = "STREAMNOTFOUND"
	streamDeadlineRequiredError  = "STREAMDEADLINEREQUIRED"
	streamConfigMissingError     = "STREAMCONFIGMISSING"
	streamSnapshotUnboundedError = "SNAPSHOTUNBOUNDED"
	streamSnapshotBoundError     = "SNAPSHOTBOUNDVIOLATION"
	streamFormatVersion          = "2"
)

var (
	// establishStreamScript adopts an existing flat stream for generation one.
	// Explicit recreation receives a distinct physical key.
	establishStreamScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
local physical = redis.call("HGET", KEYS[1], ARGV[2])
local deadline = redis.call("HGET", KEYS[1], ARGV[6])
local ttl_owned = redis.call("HGET", KEYS[1], ARGV[7])
local retention = redis.call("HGET", KEYS[1], ARGV[8])
local physical_missing = not physical
if ARGV[4] ~= "" and ARGV[5] == "1" then
    return redis.error_reply("STREAMDEADLINECONFLICT")
end
if ARGV[4] ~= "" then
    local clock = redis.call("TIME")
    local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
    if now >= tonumber(ARGV[4]) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end
if not generation and ARGV[10] == "1" and tonumber(ARGV[11]) > 0 then
    redis.call("XTRIM", ARGV[3], "MAXLEN", "=", ARGV[11])
end
if not generation then
    generation = "1"
    physical = ARGV[3]
    redis.call("HSET", KEYS[1], "generation", generation, "state", ARGV[1], ARGV[2], physical, ARGV[8], ARGV[9])
    if ARGV[4] ~= "" then
        deadline = ARGV[4]
        redis.call("HSET", KEYS[1], ARGV[6], deadline)
    elseif ARGV[5] == "1" then
        ttl_owned = "1"
        redis.call("HSET", KEYS[1], ARGV[7], ttl_owned)
    end
elseif state ~= ARGV[1] then
    generation = tostring(redis.call("HINCRBY", KEYS[1], "generation", 1))
    physical = ARGV[3] .. ":generation:" .. generation
    redis.call("HSET", KEYS[1], "state", ARGV[1], ARGV[2], physical, ARGV[8], ARGV[9])
    redis.call("HDEL", KEYS[1], ARGV[6], ARGV[7])
    deadline = false
    ttl_owned = false
    if ARGV[4] ~= "" then
        deadline = ARGV[4]
        redis.call("HSET", KEYS[1], ARGV[6], deadline)
    elseif ARGV[5] == "1" then
        ttl_owned = "1"
        redis.call("HSET", KEYS[1], ARGV[7], ttl_owned)
    end
elseif not physical then
    physical = ARGV[3]
end
if state == ARGV[1] then
    if retention and ARGV[10] == "1" and retention ~= ARGV[9] then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
    if deadline then
        if (ARGV[4] ~= "" and ARGV[4] ~= deadline) or ARGV[5] == "1" then
            return redis.error_reply("STREAMDEADLINECONFLICT")
        end
    elseif ttl_owned == "1" then
        if ARGV[4] ~= "" then
            return redis.error_reply("STREAMDEADLINECONFLICT")
        end
    end
    if physical_missing then
        redis.call("HSET", KEYS[1], ARGV[2], physical)
    end
    if not retention then
        if ARGV[10] == "1" and tonumber(ARGV[11]) > 0 and physical == ARGV[3] then
            redis.call("XTRIM", physical, "MAXLEN", "=", ARGV[11])
        end
        redis.call("HSET", KEYS[1], ARGV[8], ARGV[9])
        retention = ARGV[9]
    end
    if not deadline and ttl_owned ~= "1" then
        if ARGV[4] ~= "" then
            deadline = ARGV[4]
            redis.call("HSET", KEYS[1], ARGV[6], deadline)
        elseif ARGV[5] == "1" then
            redis.call("HSET", KEYS[1], ARGV[7], "1")
        end
    end
end
return {generation, physical, deadline or "", retention or ARGV[9]}
`)

	// loadCurrentGenerationScript binds an unbound non-creating operation to
	// the current generation without advancing a destroyed lifecycle.
	loadCurrentGenerationScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
local physical = redis.call("HGET", KEYS[1], ARGV[2])
local deadline = redis.call("HGET", KEYS[1], ARGV[6])
local ttl_owned = redis.call("HGET", KEYS[1], ARGV[7])
local retention = redis.call("HGET", KEYS[1], ARGV[8])
if not generation then
    return redis.error_reply("STREAMNOTFOUND")
end
if state ~= ARGV[1] then
    return redis.error_reply("STREAMDESTROYED")
end
if not physical then
    physical = ARGV[3]
end
if retention then
    if ARGV[10] == "1" and retention ~= ARGV[9] then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
end
if deadline then
    if (ARGV[4] ~= "" and ARGV[4] ~= deadline) or ARGV[5] == "1" then
        return redis.error_reply("STREAMDEADLINECONFLICT")
    end
elseif ttl_owned == "1" then
    if ARGV[4] ~= "" then
        return redis.error_reply("STREAMDEADLINECONFLICT")
    end
elseif ARGV[4] ~= "" or ARGV[5] == "1" then
    return redis.error_reply("STREAMDEADLINECONFLICT")
end
if not retention then
    retention = ARGV[9]
    redis.call("HSET", KEYS[1], ARGV[8], retention)
end
return {generation, physical, deadline or "", retention}
`)

	// verifyStreamScript is used by constructors whose subsequent Redis
	// operations cannot recreate stream state.
	verifyStreamScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], ARGV[3]) ~= ARGV[4] then
    return redis.error_reply("STREAMDESTROYED")
end
if redis.call("HGET", KEYS[1], ARGV[6]) ~= ARGV[7] then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
local deadline = redis.call("HGET", KEYS[1], ARGV[5])
if deadline then
    local now = redis.call("TIME")
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if now_ms >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end
return 1
`)

	// addStreamEventScript verifies the generation, appends one event, and
	// applies stream/recovery retention in one Redis operation.
	addStreamEventScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], ARGV[11]) ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
if redis.call("HGET", KEYS[1], ARGV[13]) ~= ARGV[14] then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
local deadline = redis.call("HGET", KEYS[1], ARGV[12])
if deadline then
    local now = redis.call("TIME")
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if now_ms >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
    if tonumber(ARGV[9]) > 0 then
        return redis.error_reply("STREAMDEADLINECONFLICT")
    end
end
if ARGV[6] == "1" and redis.call("EXISTS", KEYS[2]) == 0 then
    return {0}
end

local id
if ARGV[3] == "0" and ARGV[7] == "1" then
    if ARGV[6] == "1" then
        id = redis.call("XADD", KEYS[2], "NOMKSTREAM", "*", "n", ARGV[4], "p", ARGV[5], "t", ARGV[8])
    else
        id = redis.call("XADD", KEYS[2], "*", "n", ARGV[4], "p", ARGV[5], "t", ARGV[8])
    end
elseif ARGV[3] == "0" then
    if ARGV[6] == "1" then
        id = redis.call("XADD", KEYS[2], "NOMKSTREAM", "*", "n", ARGV[4], "p", ARGV[5])
    else
        id = redis.call("XADD", KEYS[2], "*", "n", ARGV[4], "p", ARGV[5])
    end
elseif ARGV[7] == "1" then
    if ARGV[6] == "1" then
        id = redis.call("XADD", KEYS[2], "NOMKSTREAM", "MAXLEN", "=", ARGV[3], "*",
            "n", ARGV[4], "p", ARGV[5], "t", ARGV[8])
    else
        id = redis.call("XADD", KEYS[2], "MAXLEN", "=", ARGV[3], "*",
            "n", ARGV[4], "p", ARGV[5], "t", ARGV[8])
    end
else
    if ARGV[6] == "1" then
        id = redis.call("XADD", KEYS[2], "NOMKSTREAM", "MAXLEN", "=", ARGV[3], "*",
            "n", ARGV[4], "p", ARGV[5])
    else
        id = redis.call("XADD", KEYS[2], "MAXLEN", "=", ARGV[3], "*",
            "n", ARGV[4], "p", ARGV[5])
    end
end

local ttl = tonumber(ARGV[9])
if deadline then
    redis.call("HSET", KEYS[3], "=deadline", deadline)
    redis.call("SADD", KEYS[4], KEYS[3])
    redis.call("PEXPIREAT", KEYS[2], deadline)
    redis.call("PEXPIREAT", KEYS[3], deadline)
    redis.call("PEXPIREAT", KEYS[4], deadline)
elseif ttl > 0 then
    if ARGV[10] == "1" then
        redis.call("PEXPIRE", KEYS[2], ttl)
    elseif redis.call("PTTL", KEYS[2]) == -1 then
        redis.call("PEXPIRE", KEYS[2], ttl)
    end
end
return {1, id}
`)

	// removeStreamEventsScript prevents stale Stream values from deleting
	// events in a newer incarnation.
	removeStreamEventsScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], ARGV[3]) ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
return redis.call("XDEL", KEYS[2], unpack(ARGV, 4))
`)
)

// establishStreamGeneration linearizes the first caller-context operation
// against Destroy and claims or adopts the generation retention contract.
func establishStreamGeneration(
	ctx context.Context,
	rdb *redis.Client,
	name, requestedDeadline, retention string,
	maxLen int,
	ttlOwned, retentionExplicit bool,
) (string, string, string, string, error) {
	result, err := establishStreamScript.Run(
		ctx,
		rdb,
		[]string{streamLifecycleKey(name)},
		streamStateActive,
		streamPhysicalKey,
		streamKey(name),
		requestedDeadline,
		boolString(ttlOwned),
		streamDeadlineKey,
		streamTTLOwnedKey,
		streamConfigKey,
		retention,
		boolString(retentionExplicit),
		strconv.Itoa(maxLen),
	).Slice()
	if err != nil {
		return "", "", "", "", streamLifecycleBoundaryError(
			fmt.Errorf("pulse stream: establish generation for %q: %w", name, err),
		)
	}
	generation, physical, deadline, canonicalRetention, err := parseLifecycleIdentity(result)
	if err != nil {
		return "", "", "", "", fmt.Errorf("pulse stream: establish generation for %q: %w", name, err)
	}
	return generation, physical, deadline, canonicalRetention, nil
}

// ensureGeneration binds s to the active Redis generation exactly once.
// Failed boundary I/O leaves the handle unbound so a later caller may retry
// with its own context.
func (s *Stream) ensureGeneration(ctx context.Context) error {
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	if s.generation != "" {
		return nil
	}
	generation, physical, deadline, retention, err := establishStreamGeneration(
		ctx,
		s.rdb,
		s.Name,
		s.requestedDeadline(),
		requestedRetention(s),
		s.maxLen,
		s.ttl > 0,
		s.retentionExplicit,
	)
	if err != nil {
		return err
	}
	s.generation = generation
	s.key = physical
	if err := s.applyRetentionConfig(retention); err != nil {
		s.generation = ""
		s.key = streamKey(s.Name)
		return fmt.Errorf("pulse stream: establish generation for %q: %w", s.Name, err)
	}
	s.deadline, err = parseDeadline(deadline)
	if err != nil {
		s.generation = ""
		s.key = streamKey(s.Name)
		return fmt.Errorf("pulse stream: establish generation for %q: %w", s.Name, err)
	}
	return nil
}

// loadExistingGeneration binds an unbound non-creating operation to the
// existing lifecycle generation without allocating a destroyed successor.
func (s *Stream) loadExistingGeneration(ctx context.Context) error {
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	if s.generation != "" {
		return nil
	}
	result, err := loadCurrentGenerationScript.Run(
		ctx,
		s.rdb,
		[]string{s.lifecycleKey},
		streamStateActive,
		streamPhysicalKey,
		streamKey(s.Name),
		s.requestedDeadline(),
		boolString(s.ttl > 0),
		streamDeadlineKey,
		streamTTLOwnedKey,
		streamConfigKey,
		requestedRetention(s),
		boolString(s.retentionExplicit),
	).Slice()
	if err != nil {
		return streamLifecycleBoundaryError(
			fmt.Errorf("pulse stream: load current generation for %q: %w", s.Name, err),
		)
	}
	generation, physical, deadline, retention, err := parseLifecycleIdentity(result)
	if err != nil {
		return fmt.Errorf("pulse stream: load current generation for %q: %w", s.Name, err)
	}
	s.generation = generation
	s.key = physical
	if err := s.applyRetentionConfig(retention); err != nil {
		s.generation = ""
		s.key = streamKey(s.Name)
		return fmt.Errorf("pulse stream: load current generation for %q: %w", s.Name, err)
	}
	s.deadline, err = parseDeadline(deadline)
	if err != nil {
		s.generation = ""
		s.key = streamKey(s.Name)
		return fmt.Errorf("pulse stream: load current generation for %q: %w", s.Name, err)
	}
	return nil
}

// verifyGeneration establishes an unbound handle and rejects a stale bound
// capability.
func (s *Stream) verifyGeneration(ctx context.Context) error {
	if err := s.ensureGeneration(ctx); err != nil {
		return err
	}
	err := verifyStreamScript.Run(
		ctx,
		s.rdb,
		[]string{s.lifecycleKey},
		streamStateActive,
		s.generation,
		streamPhysicalKey,
		s.key,
		streamDeadlineKey,
		streamConfigKey,
		s.retention,
	).Err()
	return s.lifecycleError(err)
}

// verifyExistingGeneration loads without creating, then verifies the exact
// active generation and its adopted retention contract.
func (s *Stream) verifyExistingGeneration(ctx context.Context) error {
	if err := s.loadExistingGeneration(ctx); err != nil {
		return err
	}
	return s.verifyGeneration(ctx)
}

// addEvent atomically verifies this generation and appends one event.
func (s *Stream) addEvent(
	ctx context.Context,
	name string,
	payload []byte,
	onlyIfExists bool,
	topic string,
) (string, error) {
	var err error
	if onlyIfExists {
		err = s.loadExistingGeneration(ctx)
	} else {
		err = s.ensureGeneration(ctx)
	}
	if err != nil {
		return "", err
	}
	topicPresent := topic != ""
	result, err := addStreamEventScript.Run(
		ctx,
		s.rdb,
		[]string{
			s.lifecycleKey,
			s.key,
			recoveryCursorKey(s),
			streamResourceRegistryKey(s),
		},
		streamStateActive,
		s.generation,
		strconv.Itoa(s.maxLen),
		name,
		payload,
		boolString(onlyIfExists),
		boolString(topicPresent),
		topic,
		strconv.FormatInt(s.ttl.Milliseconds(), 10),
		boolString(s.ttlSliding),
		streamPhysicalKey,
		streamDeadlineKey,
		streamConfigKey,
		s.retention,
	).Slice()
	if err != nil {
		return "", s.lifecycleError(err)
	}
	if len(result) == 0 {
		return "", fmt.Errorf("add stream event script returned no status")
	}
	status, ok := result[0].(int64)
	if !ok {
		return "", fmt.Errorf("add stream event script returned invalid status %T", result[0])
	}
	if status == 0 {
		return "", nil
	}
	if len(result) != 2 {
		return "", fmt.Errorf("add stream event script returned %d values for successful add", len(result))
	}
	id, ok := result[1].(string)
	if !ok {
		return "", fmt.Errorf("add stream event script returned invalid event ID %T", result[1])
	}
	return id, nil
}

// removeEvents atomically verifies this generation and deletes event IDs.
func (s *Stream) removeEvents(ctx context.Context, ids []string) error {
	if err := s.ensureGeneration(ctx); err != nil {
		return err
	}
	args := make([]any, 0, len(ids)+3)
	args = append(args, streamStateActive, s.generation, streamPhysicalKey)
	for _, id := range ids {
		args = append(args, id)
	}
	err := removeStreamEventsScript.Run(
		ctx,
		s.rdb,
		[]string{s.lifecycleKey, s.key},
		args...,
	).Err()
	return s.lifecycleError(err)
}


// lifecycleError maps Redis's generation mismatch to the public sentinel.
func (s *Stream) lifecycleError(err error) error {
	if err == nil {
		return nil
	}
	if redis.HasErrorPrefix(err, streamDestroyedError) {
		return fmt.Errorf(
			"%w: stream %q generation %s is no longer active",
			ErrStreamDestroyed,
			s.Name,
			s.generation,
		)
	}
	if redis.HasErrorPrefix(err, streamDeadlineError) {
		return fmt.Errorf("%w: stream %q", ErrDeadlineElapsed, s.Name)
	}
	if redis.HasErrorPrefix(err, streamConflictError) {
		return fmt.Errorf(
			"%w: stream %q deadline conflicts with active generation",
			ErrStreamConfigMismatch,
			s.Name,
		)
	}
	if redis.HasErrorPrefix(err, streamConfigError) {
		return fmt.Errorf("%w: stream %q", ErrStreamConfigMismatch, s.Name)
	}
	if redis.HasErrorPrefix(err, streamDeadlineRequiredError) {
		return fmt.Errorf("%w: stream %q requires deadline retention", ErrStreamConfigMismatch, s.Name)
	}
	if redis.HasErrorPrefix(err, streamConfigMissingError) {
		return fmt.Errorf("%w: stream %q has no adopted retention configuration", ErrStreamConfigMismatch, s.Name)
	}
	if redis.HasErrorPrefix(err, streamSnapshotUnboundedError) {
		return fmt.Errorf("%w: stream %q", ErrSnapshotUnbounded, s.Name)
	}
	if redis.HasErrorPrefix(err, streamSnapshotBoundError) {
		return fmt.Errorf("%w: stream %q", ErrSnapshotBoundExceeded, s.Name)
	}
	if redis.HasErrorPrefix(err, streamNotFoundError) {
		return fmt.Errorf("%w: stream %q", ErrStreamNotFound, s.Name)
	}
	return err
}

// streamLifecycleKey identifies the canonical generation record for a logical
// stream name.
func streamLifecycleKey(name string) string {
	return fmt.Sprintf("%s%s:lifecycle", streamKeyPrefix, name)
}

// streamKey identifies the flat physical key adopted by generation one.
func streamKey(name string) string {
	return streamKeyPrefix + name
}

// parseStreamIdentity decodes the generation and physical key selected by the
// lifecycle scripts.
func parseStreamIdentity(result []any) (string, string, string, error) {
	if len(result) != 3 {
		return "", "", "", fmt.Errorf("stream lifecycle returned %d values", len(result))
	}
	generation, ok := result[0].(string)
	if !ok {
		return "", "", "", fmt.Errorf("stream lifecycle returned generation %T", result[0])
	}
	physical, ok := result[1].(string)
	if !ok {
		return "", "", "", fmt.Errorf("stream lifecycle returned physical key %T", result[1])
	}
	deadline, ok := result[2].(string)
	if !ok {
		return "", "", "", fmt.Errorf("stream lifecycle returned deadline %T", result[2])
	}
	return generation, physical, deadline, nil
}

// parseLifecycleIdentity also returns the canonical retention configuration
// selected by the lifecycle owner.
func parseLifecycleIdentity(result []any) (string, string, string, string, error) {
	if len(result) != 4 {
		return "", "", "", "", fmt.Errorf("stream lifecycle returned %d values", len(result))
	}
	generation, physical, deadline, err := parseStreamIdentity(result[:3])
	if err != nil {
		return "", "", "", "", err
	}
	retention, ok := result[3].(string)
	if !ok || retention == "" {
		return "", "", "", "", fmt.Errorf("stream lifecycle returned retention %T", result[3])
	}
	return generation, physical, deadline, retention, nil
}

// requestedDeadline returns the construction-time absolute deadline in Redis
// millisecond form, or empty when this handle adopts the generation contract.
func (s *Stream) requestedDeadline() string {
	if !s.deadlineConfigured {
		return ""
	}
	return strconv.FormatInt(s.deadline.UnixMilli(), 10)
}

// parseDeadline decodes the canonical lifecycle deadline already validated by
// the owning Redis script.
func parseDeadline(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid Redis-owned stream deadline %q: %w", value, err)
	}
	return time.UnixMilli(milliseconds), nil
}

// streamLifecycleBoundaryError maps lifecycle configuration errors returned
// before a Stream handle has bound its generation.
func streamLifecycleBoundaryError(err error) error {
	if redis.HasErrorPrefix(err, streamDestroyedError) {
		return ErrStreamDestroyed
	}
	if redis.HasErrorPrefix(err, streamDeadlineError) {
		return ErrDeadlineElapsed
	}
	if redis.HasErrorPrefix(err, streamConflictError) {
		return fmt.Errorf(
			"%w: stream deadline conflicts with active generation",
			ErrStreamConfigMismatch,
		)
	}
	if redis.HasErrorPrefix(err, streamConfigError) {
		return ErrStreamConfigMismatch
	}
	if redis.HasErrorPrefix(err, streamNotFoundError) {
		return ErrStreamNotFound
	}
	return err
}

// retentionConfig serializes the typed immutable retention contract compared
// by every generation-opening Redis operation.
func (s *Stream) retentionConfig() string {
	mode := "none"
	value := int64(0)
	sliding := false
	switch {
	case s.deadlineConfigured:
		mode = "deadline"
		value = s.deadline.UnixMilli()
	case s.ttl > 0:
		mode = "ttl"
		value = s.ttl.Milliseconds()
		sliding = s.ttlSliding
	}
	return fmt.Sprintf(
		"v=%s|max=%d|mode=%s|value=%d|sliding=%t",
		streamFormatVersion,
		s.maxLen,
		mode,
		value,
		sliding,
	)
}

// requestedRetention returns the explicit configuration or the documented
// default used only when a writer creates the first generation.
func requestedRetention(s *Stream) string {
	return s.retention
}

// applyRetentionConfig adopts the Redis-owned immutable configuration into an
// unconfigured local handle.
func (s *Stream) applyRetentionConfig(config string) error {
	parts := strings.Split(config, "|")
	if len(parts) != 5 || parts[0] != "v="+streamFormatVersion {
		return fmt.Errorf("invalid stream retention configuration %q", config)
	}
	maxLen, err := strconv.Atoi(strings.TrimPrefix(parts[1], "max="))
	if err != nil || maxLen < 0 {
		return fmt.Errorf("invalid stream retention maximum in %q", config)
	}
	mode := strings.TrimPrefix(parts[2], "mode=")
	value, err := strconv.ParseInt(strings.TrimPrefix(parts[3], "value="), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid stream retention value in %q", config)
	}
	sliding, err := strconv.ParseBool(strings.TrimPrefix(parts[4], "sliding="))
	if err != nil {
		return fmt.Errorf("invalid stream retention sliding flag in %q", config)
	}
	switch mode {
	case "none":
		if value != 0 || sliding {
			return fmt.Errorf("invalid none retention configuration %q", config)
		}
		s.ttl = 0
		s.ttlSliding = false
	case "ttl":
		if value <= 0 {
			return fmt.Errorf("invalid ttl retention configuration %q", config)
		}
		s.ttl = time.Duration(value) * time.Millisecond
		s.ttlSliding = sliding
	case "deadline":
		if value <= 0 || sliding {
			return fmt.Errorf("invalid deadline retention configuration %q", config)
		}
		s.ttl = 0
	default:
		return fmt.Errorf("invalid stream retention mode %q", mode)
	}
	s.maxLen = maxLen
	s.MaxLen = maxLen
	s.retention = config
	return nil
}

// boolString encodes booleans for Lua arguments.
func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
