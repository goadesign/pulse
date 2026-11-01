// Package rmapwire owns the replicated map's Redis protocol: how a Lua script
// numbers a change to a map's hash and publishes it to the map's replicas.
// Package rmap and package pool both write map hashes from their own Lua
// scripts; building every such script with NewScript gives them one shared
// definition, so the published format cannot drift between writers.
//
// Each change carries the map's generation and revision. A generation starts
// whenever a script finds the map's hash missing (never written, or removed by
// its TTL): stamp stores a fresh random id under "=gen" and revisions restart
// at 1. Reset and destroy keep the generation. A replica compares revisions
// for changes of its own generation and reloads the map for any other.
//
// Payloads published on the map's channel:
//
//	set:<pack(key, value, rev, gen)>
//	del:<pack(key, rev, gen)>
//	reset:<gen>:<rev>
//	destroy:<gen>:<rev>
//
// where pack writes each string as a little-endian uint32 length followed by
// its bytes (Lua struct.pack "ic0").
package rmapwire

import (
	"context"
	"crypto/rand"

	"github.com/redis/go-redis/v9"
)

type (
	// Script is a Lua script that may write replicated-map hashes. Its Run
	// always supplies the fresh generation id that the shared prelude
	// consumes, so a script built here can never run without it.
	Script struct {
		script *redis.Script
	}
)

// prelude is prepended to every script. Its first statement removes the
// generation id that Run appends, before any of the script's own code reads
// ARGV, so the script sees exactly the arguments its caller passed (ARGV[#ARGV]
// keeps meaning the caller's last argument).
const prelude = `
local rmapNewGen = table.remove(ARGV)

-- stamp returns the generation of the map stored in hash and its next
-- revision, starting a generation when the hash is missing. Only the publish
-- helpers below call it, each publishing exactly one change, so revisions
-- advance by exactly one per published change and replicas can detect a lost
-- notification as a skipped revision.
local function stamp(hash)
   local gen = redis.call("HGET", hash, "=gen")
   if not gen then
      gen = rmapNewGen
      redis.call("HSET", hash, "=gen", gen)
   end
   return gen, tostring(redis.call("HINCRBY", hash, "=rev", 1))
end

-- publishSet records that key now holds value in the map stored in hash,
-- publishes the change on channel, and returns its revision. The caller has
-- already written the value.
local function publishSet(hash, channel, key, value)
   local gen, rev = stamp(hash)
   redis.call("HSET", hash, "=kind", "set")
   redis.call("PUBLISH", channel, "set:" .. struct.pack("ic0ic0ic0ic0",
      string.len(key), key, string.len(value), value, string.len(rev), rev, string.len(gen), gen))
   return rev
end

-- publishDel records that key was removed from the map stored in hash,
-- publishes the change on channel, and returns its revision. The caller has
-- already removed the field.
local function publishDel(hash, channel, key)
   local gen, rev = stamp(hash)
   redis.call("HSET", hash, "=kind", "del")
   redis.call("PUBLISH", channel, "del:" .. struct.pack("ic0ic0ic0",
      string.len(key), key, string.len(rev), rev, string.len(gen), gen))
   return rev
end

-- publishClear empties the map stored in hash, keeping its generation and
-- revision, publishes kind ("reset" or "destroy") on channel, and returns the
-- revision.
local function publishClear(hash, channel, kind)
   local gen, rev = stamp(hash)
   redis.call("DEL", hash)
   redis.call("HSET", hash, "=gen", gen, "=rev", rev, "=kind", kind)
   redis.call("PUBLISH", channel, kind .. ":" .. gen .. ":" .. rev)
   return rev
end

-- deleteMap removes the map stored in hash entirely, leaving no hash behind,
-- and publishes "destroy" on channel so replicas empty their copies.
local function deleteMap(hash, channel)
   local gen, rev = stamp(hash)
   redis.call("DEL", hash)
   redis.call("PUBLISH", channel, "destroy:" .. gen .. ":" .. rev)
end
`

// NewScript builds a Script from Lua source that may call publishSet,
// publishDel, publishClear, and deleteMap.
func NewScript(src string) *Script {
	return &Script{script: redis.NewScript(prelude + src)}
}

// Run runs the script with keys and args, appending the generation id the
// prelude consumes. It has the same contract as redis.Script.Run.
func (s *Script) Run(ctx context.Context, c redis.Scripter, keys []string, args ...any) *redis.Cmd {
	// Copy on append so the caller's args slice is never modified.
	return s.script.Run(ctx, c, keys, append(args[:len(args):len(args)], rand.Text())...)
}

// Hash returns the SHA-1 Redis uses to identify the script, including the
// prelude.
func (s *Script) Hash() string {
	return s.script.Hash()
}

// Load caches the script in Redis.
func (s *Script) Load(ctx context.Context, c redis.Scripter) *redis.StringCmd {
	return s.script.Load(ctx, c)
}
