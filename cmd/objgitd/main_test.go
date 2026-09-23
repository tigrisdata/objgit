package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMetricsMuxHealthz covers the health endpoint of the metrics listener.
// GET /healthz answers 200 with an ok body for probes. Other methods are
// rejected. /metrics keeps serving the scrape endpoint beside it.
func TestMetricsMuxHealthz(t *testing.T) {
	ts := httptest.NewServer(newMetricsMux())
	t.Cleanup(ts.Close)

	for _, tt := range []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "GET /healthz answers 200 ok",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
			wantBody:   "ok\n",
		},
		{
			name:       "POST /healthz is not allowed",
			method:     http.MethodPost,
			path:       "/healthz",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "GET /metrics still serves the scrape endpoint",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusOK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, ts.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantBody == "" {
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(body) != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
		})
	}
}
