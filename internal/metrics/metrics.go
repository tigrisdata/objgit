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
