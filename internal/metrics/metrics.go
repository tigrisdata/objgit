// Package metrics defines objgitd's Prometheus instrumentation. Every metric
// registers against the default registry via promauto, so the Go-runtime and
// process collectors that client_golang installs there are exported alongside
// them by promhttp.Handler.
//
// The exported helpers keep call sites in the transports and s3fs tiny and free
// of label plumbing: each transport reports a git operation, each Authorize
// call reports a decision, and s3fs reports an S3 round-trip through the
// observer signature ObserveS3 satisfies.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tigrisdata/objgit/internal/auth"
)

const namespace = "objgit"

var (
	s3Requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "s3",
		Name:      "requests_total",
		Help:      "Total S3/Tigris API calls by operation and outcome.",
	}, []string{"operation", "status"})

	s3Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "s3",
		Name:      "request_duration_seconds",
		Help:      "Latency of S3/Tigris API calls by operation.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation"})

	gitRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "git",
		Name:      "requests_total",
		Help:      "Total git operations by protocol, service, and outcome.",
	}, []string{"protocol", "service", "status"})

	gitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "git",
		Name:      "request_duration_seconds",
		Help:      "Latency of git operations by protocol and service.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"protocol", "service"})

	gitInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "git",
		Name:      "requests_in_flight",
		Help:      "Git operations currently being served, by protocol.",
	}, []string{"protocol"})

	authRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "auth",
		Name:      "requests_total",
		Help:      "Authorization decisions by transport, operation, and decision.",
	}, []string{"transport", "operation", "decision"})

	authDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "auth",
		Name:      "request_duration_seconds",
		Help:      "Latency of authorization decisions by transport.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"transport"})

	hookRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "hook",
		Name:      "runs_total",
		Help:      "Push-hook executions by outcome (ok, error, timeout).",
	}, []string{"status"})

	hookDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "hook",
		Name:      "run_duration_seconds",
		Help:      "Latency of push-hook executions.",
		Buckets:   prometheus.DefBuckets,
	})

	reposCreated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "repos_created_total",
		Help:      "Repositories auto-created on first push.",
	})

	packPayloads = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "pack",
		Name:      "payloads_total",
		Help:      "Objects written into pack containers by payload codec (raw, zstd).",
	}, []string{"codec"})

	packPayloadBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "pack",
		Name:      "payload_bytes_total",
		Help:      "Object bytes written into pack containers by codec, counted both before and after compression.",
	}, []string{"codec", "stage"})

	packPayloadRatio = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "pack",
		Name:      "payload_stored_ratio",
		Help:      "Stored size as a fraction of raw size for compressed pack payloads; 1.0 means compression saved nothing.",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
	})

	pushSlotsHeld = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "push",
		Name:      "slots_held",
		Help:      "Pushes holding a concurrency slot, so unpacking a packfile right now.",
	})

	pushQueueWaiting = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "push",
		Name:      "queue_waiting",
		Help:      "Pushes waiting for a concurrency slot. A value that stays above zero means -max-concurrent-pushes is below the offered load.",
	})

	pushQueueWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "push",
		Name:      "queue_wait_seconds",
		Help:      "Time a push spent waiting for a concurrency slot.",
		Buckets:   []float64{0.001, 0.01, 0.1, 1, 5, 15, 30, 60, 120, 300},
	})

	pushQueueOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "push",
		Name:      "queue_outcomes_total",
		Help:      "Pushes that asked for a concurrency slot, by outcome (admitted, timeout, canceled).",
	}, []string{"outcome"})

	refCASRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "ref",
		Name:      "cas_retries_total",
		Help:      "Packed-refs compare-and-swap attempts that lost a race and were retried.",
	})
)

// ObserveS3 records one S3/Tigris API call. Its signature matches the observer
// internal/s3fs expects, so main wires it via s3fs.SetMetricsObserver.
func ObserveS3(operation string, dur time.Duration, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	s3Requests.WithLabelValues(operation, status).Inc()
	s3Duration.WithLabelValues(operation).Observe(dur.Seconds())
}

// TrackInFlight increments the in-flight gauge for protocol and returns a
// closure that decrements it; call the result with defer.
func TrackInFlight(protocol string) func() {
	gitInFlight.WithLabelValues(protocol).Inc()
	return func() { gitInFlight.WithLabelValues(protocol).Dec() }
}

// Push-slot outcomes, as passed to ObservePushWait.
const (
	PushAdmitted = "admitted"
	PushTimeout  = "timeout"
	PushCanceled = "canceled"
)

// TrackPushWait increments the gauge of pushes waiting for a concurrency slot
// and returns a closure that decrements it; call the result with defer.
func TrackPushWait() func() {
	pushQueueWaiting.Inc()
	return func() { pushQueueWaiting.Dec() }
}

// TrackPushSlot increments the gauge of pushes holding a concurrency slot and
// returns a closure that decrements it; call the result with defer.
func TrackPushSlot() func() {
	pushSlotsHeld.Inc()
	return func() { pushSlotsHeld.Dec() }
}

// ObservePushWait records one wait for a concurrency slot. outcome is
// PushAdmitted, PushTimeout, or PushCanceled, which keeps a push that gave up
// waiting distinct from every other kind of push failure. start is when the
// wait began.
func ObservePushWait(outcome string, start time.Time) {
	pushQueueOutcomes.WithLabelValues(outcome).Inc()
	pushQueueWait.Observe(time.Since(start).Seconds())
}

// ObserveGitOp records a completed git operation: status is "ok", "error", or
// "denied". start is when the handler began serving it.
func ObserveGitOp(protocol, service, status string, start time.Time) {
	gitRequests.WithLabelValues(protocol, service, status).Inc()
	gitDuration.WithLabelValues(protocol, service).Observe(time.Since(start).Seconds())
}

// ObserveAuth records an authorization decision, mapping the auth enums to
// stable label strings so transports stay free of label plumbing.
func ObserveAuth(transport string, op auth.Operation, d auth.Decision, start time.Time) {
	authRequests.WithLabelValues(transport, operationLabel(op), decisionLabel(d)).Inc()
	authDuration.WithLabelValues(transport).Observe(time.Since(start).Seconds())
}

func operationLabel(op auth.Operation) string {
	if op == auth.Write {
		return "write"
	}
	return "read"
}

func decisionLabel(d auth.Decision) string {
	switch d {
	case auth.Allow:
		return "allow"
	case auth.Unauthenticated:
		return "unauthenticated"
	default:
		return "deny"
	}
}

// ObserveHook records a push-hook execution and its latency. status is "ok",
// "error", or "timeout".
func ObserveHook(status string, dur time.Duration) {
	hookRuns.WithLabelValues(status).Inc()
	hookDuration.Observe(dur.Seconds())
}

// ReposCreated counts a repository auto-created on first push.
func ReposCreated() {
	reposCreated.Inc()
}

// ObservePackPayload records one object written into a pack container. Its
// signature matches the observer internal/storage/tigris expects, so main
// wires it via tigris.WithPayloadObserver — which is what keeps that package
// free of any Prometheus import.
//
// Expect codec="raw" to dominate by count on a normal repository: objects
// below the compression floor are never compressed, and most git objects are
// small. Judge the feature by payload_bytes_total, not by payloads_total.
func ObservePackPayload(codec string, raw, stored int64) {
	packPayloads.WithLabelValues(codec).Inc()
	packPayloadBytes.WithLabelValues(codec, "raw").Add(float64(raw))
	packPayloadBytes.WithLabelValues(codec, "stored").Add(float64(stored))
	if codec != "raw" && raw > 0 {
		packPayloadRatio.Observe(float64(stored) / float64(raw))
	}
}

// ObserveRefCASRetry records one retried packed-refs compare-and-swap. Wire it
// into tigris.WithRefCASObserver.
//
// A rising rate here is the one quiet way the packed-ref write path degrades:
// every retry re-reads and re-writes the whole object, so sustained contention
// shows up as push latency long before it shows up as an error.
func ObserveRefCASRetry() {
	refCASRetries.Inc()
}

// ListingCacheStats is a flat snapshot of the s3fs directory-listing cache's
// counters. It mirrors s3fs.CacheStats but is defined here so s3fs stays free of
// any Prometheus import; main bridges the two.
type ListingCacheStats struct {
	Hits, Misses                          int64
	ListingItems, SubtreeItems, HeadItems int64
}

// RegisterListingCache installs a Prometheus collector that reports the
// directory-listing cache's counters under objgit_s3_listing_cache_*. provider
// is polled at scrape time. Call once at startup when the cache is enabled.
func RegisterListingCache(provider func() ListingCacheStats) {
	prometheus.MustRegister(&listingCacheCollector{provider: provider})
}

type listingCacheCollector struct {
	provider func() ListingCacheStats
}

var (
	lcHits   = prometheus.NewDesc("objgit_s3_listing_cache_hits_total", "Listing/subtree-cache lookups served from cache.", nil, nil)
	lcMisses = prometheus.NewDesc("objgit_s3_listing_cache_misses_total", "Listing/subtree-cache lookups that fell through to S3.", nil, nil)
	lcItems  = prometheus.NewDesc("objgit_s3_listing_cache_items", "Resident cache entries by kind.", []string{"kind"}, nil)
)

func (c *listingCacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lcHits
	ch <- lcMisses
	ch <- lcItems
}

func (c *listingCacheCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.provider()
	ch <- prometheus.MustNewConstMetric(lcHits, prometheus.CounterValue, float64(s.Hits))
	ch <- prometheus.MustNewConstMetric(lcMisses, prometheus.CounterValue, float64(s.Misses))
	ch <- prometheus.MustNewConstMetric(lcItems, prometheus.GaugeValue, float64(s.ListingItems), "listing")
	ch <- prometheus.MustNewConstMetric(lcItems, prometheus.GaugeValue, float64(s.SubtreeItems), "subtree")
	ch <- prometheus.MustNewConstMetric(lcItems, prometheus.GaugeValue, float64(s.HeadItems), "head")
}
