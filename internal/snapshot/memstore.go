package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"sync"
)

// MemStore is a Store in memory. It exists for tests, in this package and in
// others that need a Store without a bucket.
type MemStore struct {
	// PutErr, when set, is returned by every PutSnapshot.
	PutErr error

	mu   sync.Mutex
	objs map[string]memObject
	puts int
}

type memObject struct {
	body []byte
	meta map[string]string
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{objs: map[string]memObject{}} }

func (m *MemStore) StatSnapshot(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return 0, fmt.Errorf("snapshot: %s: %w", key, fs.ErrNotExist)
	}
	return int64(len(o.body)), nil
}

func (m *MemStore) PutSnapshot(_ context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error {
	m.mu.Lock()
	m.puts++
	putErr := m.PutErr
	m.mu.Unlock()
	if putErr != nil {
		return putErr
	}
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		return fmt.Errorf("snapshot: put %s: read %d bytes, want %d", key, len(b), size)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = memObject{body: b, meta: maps.Clone(meta)}
	return nil
}

func (m *MemStore) OpenSnapshot(_ context.Context, key string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("snapshot: %s: %w", key, fs.ErrNotExist)
	}
	return nopCloser{bytes.NewReader(o.body)}, nil
}

// Keys returns every stored key, sorted.
func (m *MemStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.objs))
}

// Object returns the body and metadata stored at key.
func (m *MemStore) Object(key string) ([]byte, map[string]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	return o.body, o.meta, ok
}

// PutCount returns how many times PutSnapshot ran, failed calls included.
func (m *MemStore) PutCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
