package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveSnapshot(t *testing.T) {
	tests := []struct {
		result string
	}{
		{"built"},
		{"exists"},
		{"error"},
	}
	for _, tt := range tests {
		t.Run(tt.result, func(t *testing.T) {
			before := testutil.ToFloat64(snapshotBuilds.WithLabelValues(tt.result))
			ObserveSnapshot(tt.result, time.Second, 1<<20)
			if got := testutil.ToFloat64(snapshotBuilds.WithLabelValues(tt.result)); got != before+1 {
				t.Errorf("builds_total{%s} = %v, want %v", tt.result, got, before+1)
			}
		})
	}
}

func TestObserveSnapshotCache(t *testing.T) {
	tests := []struct {
		event string
		vec   string
		label string
	}{
		{"hit", "opens", "hit"},
		{"miss", "opens", "miss"},
		{"evict_budget", "evictions", "budget"},
		{"evict_idle", "evictions", "idle"},
	}
	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			c := snapshotCacheOpens.WithLabelValues(tt.label)
			if tt.vec == "evictions" {
				c = snapshotCacheEvictions.WithLabelValues(tt.label)
			}
			before := testutil.ToFloat64(c)
			ObserveSnapshotCache(tt.event)
			if got := testutil.ToFloat64(c); got != before+1 {
				t.Errorf("%s{%s} = %v, want %v", tt.vec, tt.label, got, before+1)
			}
		})
	}
}
