package testing

// Hasher is a mock hasher for testing
// It can be used in two ways:
// 1. Use a static index to pick a bucket
// 2. Use a function to customize the hashing behavior
type Hasher struct {
	Index     int64
	IndexFunc func(key string, numBuckets int64) int64
}

// Hash returns the bucket index for the given key
func (h *Hasher) Hash(key string, numBuckets int64) int64 {
	if h.IndexFunc != nil {
		return h.IndexFunc(key, numBuckets)
	}
	return h.Index
}
