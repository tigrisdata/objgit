package main

import (
	"strings"
	"testing"
)

// A trimmed /metrics body. The histogram bucket and count lines are here on
// purpose: they share a prefix with the duration sum, and a parser that keys on
// the prefix instead of the whole name silently triples the recorded time.
const sampleExposition = `# HELP objgit_s3_requests_total Tigris API calls.
# TYPE objgit_s3_requests_total counter
objgit_s3_requests_total{operation="GetObject",status="ok"} 8500
objgit_s3_requests_total{operation="GetObject",status="error"} 3
objgit_s3_requests_total{operation="PutObject",status="ok"} 20000
objgit_s3_requests_total{operation="ListObjectsV2",status="ok"} 12
# TYPE objgit_s3_request_duration_seconds histogram
objgit_s3_request_duration_seconds_bucket{operation="GetObject",le="0.1"} 4000
objgit_s3_request_duration_seconds_sum{operation="GetObject"} 412.5
objgit_s3_request_duration_seconds_count{operation="GetObject"} 8503
objgit_s3_request_duration_seconds_sum{operation="PutObject"} 900.25
go_goroutines 42
`

func TestParseS3Metrics(t *testing.T) {
	t.Parallel()

	got, err := parseS3Metrics(strings.NewReader(sampleExposition))
	if err != nil {
		t.Fatalf("parseS3Metrics: %v", err)
	}

	for _, tt := range []struct {
		name string
		op   string
		want uint64
	}{
		{"both statuses sum into one operation", "GetObject", 8503},
		{"single status", "PutObject", 20000},
		{"listing", "ListObjectsV2", 12},
		{"an operation the daemon never made", "HeadObject", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got.Requests[tt.op] != tt.want {
				t.Logf("want: %d", tt.want)
				t.Logf("got:  %d", got.Requests[tt.op])
				t.Errorf("%s request count is wrong", tt.op)
			}
		})
	}

	if got.Errors["GetObject"] != 3 {
		t.Errorf("want 3 GetObject errors, got %d", got.Errors["GetObject"])
	}
	if got.Errors["PutObject"] != 0 {
		t.Errorf("a status=ok sample was counted as an error: %d", got.Errors["PutObject"])
	}

	if got.Total() != 28515 {
		t.Errorf("want 28515 total requests, got %d", got.Total())
	}

	// 412.5 + 900.25. If the bucket or count lines leaked in, this is larger.
	if want := 1312.75; got.TotalSeconds() != want {
		t.Logf("want: %v", want)
		t.Logf("got:  %v", got.TotalSeconds())
		t.Error("histogram bucket or count lines leaked into the duration sum")
	}
}

func TestParseSample(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		line      string
		wantName  string
		wantValue float64
		wantOp    string
		wantErr   bool
	}{
		{
			name:      "labelled counter",
			line:      `objgit_s3_requests_total{operation="GetObject",status="ok"} 8500`,
			wantName:  "objgit_s3_requests_total",
			wantValue: 8500,
			wantOp:    "GetObject",
		},
		{
			name:      "no labels",
			line:      `go_goroutines 42`,
			wantName:  "go_goroutines",
			wantValue: 42,
		},
		{
			name:      "value carries a timestamp",
			line:      `objgit_s3_requests_total{operation="PutObject",status="ok"} 7 1756651200000`,
			wantName:  "objgit_s3_requests_total",
			wantValue: 7,
			wantOp:    "PutObject",
		},
		{
			name:      "scientific notation",
			line:      `objgit_s3_request_duration_seconds_sum{operation="GetObject"} 1.5e+02`,
			wantName:  "objgit_s3_request_duration_seconds_sum",
			wantValue: 150,
			wantOp:    "GetObject",
		},
		{
			name:      "escaped quote in a label value",
			line:      `objgit_s3_requests_total{operation="Get\"Object",status="ok"} 1`,
			wantName:  "objgit_s3_requests_total",
			wantValue: 1,
			wantOp:    `Get"Object`,
		},
		{
			name:    "no value at all",
			line:    `objgit_s3_requests_total`,
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, labels, value, err := parseSample(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Fatal("wanted an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSample: %v", err)
			}

			if name != tt.wantName {
				t.Errorf("want name %q, got %q", tt.wantName, name)
			}
			if value != tt.wantValue {
				t.Errorf("want value %v, got %v", tt.wantValue, value)
			}
			if labels["operation"] != tt.wantOp {
				t.Errorf("want operation %q, got %q", tt.wantOp, labels["operation"])
			}
		})
	}
}

func TestS3CountersSub(t *testing.T) {
	t.Parallel()

	before := newS3Counters()
	before.Requests["GetObject"] = 100
	before.Requests["PutObject"] = 5
	before.Seconds["GetObject"] = 10

	after := newS3Counters()
	after.Requests["GetObject"] = 8600
	after.Requests["PutObject"] = 5
	after.Requests["HeadObject"] = 7
	after.Seconds["GetObject"] = 412.5

	got := after.sub(before)

	for _, tt := range []struct {
		name string
		op   string
		want uint64
	}{
		{"counter that moved", "GetObject", 8500},
		{"counter that did not move", "PutObject", 0},
		{"operation absent from the earlier reading", "HeadObject", 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got.Requests[tt.op] != tt.want {
				t.Logf("want: %d", tt.want)
				t.Logf("got:  %d", got.Requests[tt.op])
				t.Errorf("%s delta is wrong", tt.op)
			}
		})
	}

	if got.Seconds["GetObject"] != 402.5 {
		t.Errorf("want 402.5 seconds, got %v", got.Seconds["GetObject"])
	}
}

// A daemon restart resets every counter to zero. The later reading is then
// smaller than the earlier one, and the whole later value is the delta: it all
// belongs to the operation being measured.
func TestS3CountersSubAfterRestart(t *testing.T) {
	t.Parallel()

	before := newS3Counters()
	before.Requests["GetObject"] = 9000

	after := newS3Counters()
	after.Requests["GetObject"] = 12

	if got := after.sub(before).Requests["GetObject"]; got != 12 {
		t.Logf("want: 12")
		t.Logf("got:  %d", got)
		t.Error("a counter reset was wrapped instead of taken at face value")
	}
}

func TestOperationsAreSorted(t *testing.T) {
	t.Parallel()

	c := newS3Counters()
	c.Requests["PutObject"] = 1
	c.Requests["GetObject"] = 1
	c.Seconds["ListObjectsV2"] = 1

	got := c.Operations()
	want := []string{"GetObject", "ListObjectsV2", "PutObject"}

	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}
