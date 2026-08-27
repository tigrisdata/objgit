package bundler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBundler(t *testing.T) {
	input := []int{1, 2, 3, 4}
	done := false
	b := New[int](func(_ context.Context, data []int) {
		if len(data) != 4 {
			t.Errorf("Wanted len(data) == %d, got: %d", len(input), len(data))
		}

		sum := 0
		const wantSum = 10
		for _, i := range data {
			sum += i
		}

		if sum != wantSum {
			t.Errorf("wanted sum of inputs to be %d, got: %d", wantSum, sum)
		}
		done = true
	})

	for _, i := range input {
		b.Add(i, 1)
	}

	b.Flush()

	if !done {
		t.Fatal("function wasn't called")
	}
}

// TestAddWaitParksOnOversizedItem pins the behavior callers have to defend
// against: with BundleByteLimit unset there is no ErrOversizedItem check, so an
// item weighing more than BufferedByteLimit reaches a semaphore that can never
// admit it. Acquire does not fail such a weight — it waits for ctx. A caller
// that hands AddWait an unbounded size therefore blocks for as long as its
// context lives. See sizeHint in internal/storage/tigris/upload.go, which
// clamps for exactly this reason.
func TestAddWaitParksOnOversizedItem(t *testing.T) {
	t.Parallel()

	handled := 0
	b := New[int](func(_ context.Context, data []int) { handled += len(data) })
	b.BufferedByteLimit = 100

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := b.AddWait(ctx, 1, 1000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AddWait of an oversized item = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("AddWait returned after %v, want it to have waited out the context", elapsed)
	}

	b.Flush()
	if handled != 0 {
		t.Errorf("handler saw %d items, want 0: the oversized item was never admitted", handled)
	}

	// A weight the limit can hold still goes through, so the park above is
	// about the size and not about the bundler being wedged.
	if err := b.AddWait(context.Background(), 2, 10); err != nil {
		t.Fatalf("AddWait of an item within the limit: %v", err)
	}
	b.Flush()
	if handled != 1 {
		t.Errorf("handler saw %d items, want 1", handled)
	}
}
