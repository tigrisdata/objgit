package s3fs

import "time"

// metricsObserver, when non-nil, is invoked after every S3 API round-trip with
// the operation name, its wall-clock duration, and any error. It is
// process-global because the S3 client and the Prometheus registry it feeds
// both are; SetMetricsObserver wires it from main. This keeps s3fs free of any
// Prometheus import — the observer is an opaque callback.
var metricsObserver func(operation string, dur time.Duration, err error)

// SetMetricsObserver installs a process-wide observer invoked after each S3 API
// call. Pass nil to disable. Call it during startup, before the filesystem is
// in use; it is not safe to change concurrently with active operations.
func SetMetricsObserver(fn func(operation string, dur time.Duration, err error)) {
	metricsObserver = fn
}

// observeS3 reports one S3 API call to the metrics observer if one is
// installed. operation is the S3 API name (e.g. "GetObject"); start is taken
// immediately before the call.
func observeS3(operation string, start time.Time, err error) {
	if metricsObserver != nil {
		metricsObserver(operation, time.Since(start), err)
	}
}
