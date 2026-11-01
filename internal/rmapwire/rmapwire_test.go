package rmapwire

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

var (
	// scriptRE matches a Lua script built with redis.NewScript or
	// rmapwire.NewScript from a raw string literal.
	scriptRE = regexp.MustCompile("(?s)\\b(redis|rmapwire)\\.NewScript\\(`(.*?)`\\)")
	// helperRE matches a call to one of the prelude's helpers.
	helperRE = regexp.MustCompile(`\b(publishSet|publishDel|publishClear|deleteMap)\(`)
	// protocolRE matches hand-written map protocol: bumping "=rev", packing a
	// change, or publishing a map payload.
	// A direct stamp call would advance the revision without publishing.
	protocolRE = regexp.MustCompile(`"=rev"|"=kind"|struct\.pack|"(set|del|reset|destroy):"|\bstamp\(`)
)

// TestScriptsUseSharedProtocol keeps this package the only definition of the
// replicated map's Redis protocol. Every Lua script in the module that changes
// a map must call the prelude's helpers and be built with NewScript, and no
// script may hand-write the protocol, so the published format cannot drift
// between rmap, pool, and streaming.
func TestScriptsUseSharedProtocol(t *testing.T) {
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	var checked int
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.Dir(path) == filepath.Join(root, "internal", "rmapwire") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for _, m := range scriptRE.FindAllStringSubmatch(string(src), -1) {
			checked++
			body := m[2]
			assert.False(t, protocolRE.MatchString(body),
				"%s: a script hand-writes the map protocol; use the rmapwire helpers", rel)
			if helperRE.MatchString(body) {
				assert.Equal(t, "rmapwire", m[1],
					"%s: a script calls rmapwire helpers but is not built with rmapwire.NewScript", rel)
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Greater(t, checked, 50, "the scan found too few scripts; check scriptRE")
}

// TestRunHidesGenerationArgument runs a script against Redis and checks that
// the script's own code sees exactly the caller's arguments (the generation id
// Run appends is removed first, so ARGV[#ARGV] is the caller's last argument)
// and that Run never writes into the caller's args slice.
func TestRunHidesGenerationArgument(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	script := NewScript(`return {#ARGV, ARGV[#ARGV]}`)
	backing := make([]any, 2, 4)
	backing[0], backing[1] = "first", "last"
	res, err := script.Run(context.Background(), rdb, []string{"k"}, backing[:2]...).Slice()
	require.NoError(t, err)
	assert.Equal(t, []any{int64(2), "last"}, res)
	assert.Nil(t, backing[:3][2], "Run must not write past the caller's arguments")
}
