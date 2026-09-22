package lfs

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func mustLock(t *testing.T, s *Store, path, owner string) Lock {
	t.Helper()
	l, created, err := s.CreateLock(context.Background(), testRepo, path, owner)
	if err != nil {
		t.Fatalf("CreateLock(%q): %v", path, err)
	}
	if !created {
		t.Fatalf("CreateLock(%q) did not create a lock", path)
	}
	return l
}

func TestCreateLock(t *testing.T) {
	s := newTestStore(t, newFakeS3())

	l := mustLock(t, s, "big.bin", "alice")

	if l.Path != "big.bin" {
		t.Errorf("Path = %q, want %q", l.Path, "big.bin")
	}
	if l.Owner.Name != "alice" {
		t.Errorf("Owner = %q, want %q", l.Owner.Name, "alice")
	}
	if l.ID == "" {
		t.Error("lock has no ID")
	}
	if l.LockedAt.IsZero() {
		t.Error("lock has no timestamp")
	}
}

// TestLockIDsAreServerGenerated matters because the ID comes back to the server
// as a path segment on unlock. A client-chosen ID would be caller-controlled
// text in a route.
func TestLockIDsAreServerGenerated(t *testing.T) {
	s := newTestStore(t, newFakeS3())

	a := mustLock(t, s, "one.bin", "alice")
	b := mustLock(t, s, "two.bin", "alice")

	if a.ID == b.ID {
		t.Fatal("two locks share an ID")
	}
	for _, id := range []string{a.ID, b.ID} {
		if strings.Trim(id, "0123456789abcdef") != "" {
			t.Errorf("lock ID %q is not lowercase hex", id)
		}
	}
}

func TestCreateLockOnAHeldPathConflicts(t *testing.T) {
	s := newTestStore(t, newFakeS3())
	first := mustLock(t, s, "big.bin", "alice")

	got, created, err := s.CreateLock(context.Background(), testRepo, "big.bin", "bob")
	if err != nil {
		t.Fatalf("CreateLock: %v", err)
	}
	if created {
		t.Fatal("a held path must not be locked twice")
	}
	// The conflict response has to name the existing holder, so the client can
	// tell the user who to ask.
	if got.ID != first.ID {
		t.Errorf("conflict returned lock %q, want the holder %q", got.ID, first.ID)
	}
	if got.Owner.Name != "alice" {
		t.Errorf("conflict owner = %q, want alice", got.Owner.Name)
	}
}

func TestListLocks(t *testing.T) {
	s := newTestStore(t, newFakeS3())
	one := mustLock(t, s, "one.bin", "alice")
	mustLock(t, s, "two.bin", "bob")

	for _, tt := range []struct {
		name   string
		filter LockFilter
		want   int
	}{
		{name: "no filter returns every lock", want: 2},
		{name: "by path", filter: LockFilter{Path: "one.bin"}, want: 1},
		{name: "by id", filter: LockFilter{ID: one.ID}, want: 1},
		{name: "by unknown path", filter: LockFilter{Path: "nope.bin"}, want: 0},
		{name: "limit", filter: LockFilter{Limit: 1}, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := s.ListLocks(context.Background(), testRepo, tt.filter)
			if err != nil {
				t.Fatalf("ListLocks: %v", err)
			}
			if len(got) != tt.want {
				t.Fatalf("got %d locks, want %d", len(got), tt.want)
			}
		})
	}
}

func TestListLocksOnAnUnlockedRepository(t *testing.T) {
	s := newTestStore(t, newFakeS3())

	got, _, err := s.ListLocks(context.Background(), testRepo, LockFilter{})
	if err != nil {
		t.Fatalf("ListLocks: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d locks, want 0", len(got))
	}
}

func TestDeleteLock(t *testing.T) {
	for _, tt := range []struct {
		name    string
		owner   string
		force   bool
		wantErr error
	}{
		{name: "the owner may unlock", owner: "alice"},
		{name: "another user may not", owner: "bob", wantErr: ErrLockHeldByAnother},
		{name: "another user may force", owner: "bob", force: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t, newFakeS3())
			held := mustLock(t, s, "big.bin", "alice")

			got, err := s.DeleteLock(context.Background(), testRepo, held.ID, tt.owner, tt.force)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("DeleteLock error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if got.ID != held.ID {
				t.Errorf("DeleteLock returned %q, want %q", got.ID, held.ID)
			}

			remaining, _, err := s.ListLocks(context.Background(), testRepo, LockFilter{})
			if err != nil {
				t.Fatalf("ListLocks: %v", err)
			}
			if len(remaining) != 0 {
				t.Errorf("got %d locks after unlock, want 0", len(remaining))
			}
		})
	}
}

func TestDeleteUnknownLock(t *testing.T) {
	s := newTestStore(t, newFakeS3())

	_, err := s.DeleteLock(context.Background(), testRepo, "deadbeef", "alice", false)
	if !errors.Is(err, ErrLockNotFound) {
		t.Fatalf("DeleteLock error = %v, want ErrLockNotFound", err)
	}
}

// TestCreateLockRetriesALostRace drives the compare-and-swap the way two
// concurrent clients would. The hook lands a competing lock between this
// caller's read and its write, so the first conditional write is refused and
// the loop has to re-read and try again.
func TestCreateLockRetriesALostRace(t *testing.T) {
	f := newFakeS3()
	s := newTestStore(t, f)

	var raced bool
	f.putHook = func(key string) {
		if raced || key != LocksKey(testRepo) {
			return
		}
		raced = true
		// A competing writer commits first, invalidating the ETag this caller
		// is about to write against.
		f.set(key, fakeObject{body: []byte(`{"locks":[{"id":"aa","path":"other.bin","owner":{"name":"bob"}}]}`)})
	}

	l := mustLock(t, s, "big.bin", "alice")

	if !raced {
		t.Fatal("the racing write never ran, so no retry was exercised")
	}

	// Both locks must survive: the retry re-read the competitor's document
	// rather than overwriting it.
	got, _, err := s.ListLocks(context.Background(), testRepo, LockFilter{})
	if err != nil {
		t.Fatalf("ListLocks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d locks, want 2 (the retry dropped a concurrent write)", len(got))
	}
	if _, _, err := s.ListLocks(context.Background(), testRepo, LockFilter{ID: l.ID}); err != nil {
		t.Fatalf("locking own lock back: %v", err)
	}
}
