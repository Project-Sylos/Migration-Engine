package db

import "testing"

func TestDeterministicProgressPercent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                        string
		pending, successful, failed int64
		retryMode                   bool
		want                        float64
	}{
		{name: "empty", want: 0},
		{name: "all pending", pending: 10, want: 0},
		{name: "mid run", pending: 50, successful: 40, failed: 10, want: 50},
		{name: "all successful", successful: 100, want: 100},
		{name: "successful and failed no pending", successful: 80, failed: 20, want: 100},
		{name: "pending remaining", pending: 25, successful: 75, want: 75},
		{name: "only failed", failed: 5, want: 100},
		// Retry: already-successful baseline; failed (+ pending) is remaining work.
		{name: "retry start from baseline", successful: 90, failed: 10, retryMode: true, want: 90},
		{name: "retry mid", pending: 0, successful: 95, failed: 5, retryMode: true, want: 95},
		{name: "retry with marked pending", pending: 10, successful: 90, failed: 0, retryMode: true, want: 90},
		{name: "retry all done", successful: 100, retryMode: true, want: 100},
		{name: "retry only failed left", failed: 10, retryMode: true, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := DeterministicProgressPercent(tt.pending, tt.successful, tt.failed, tt.retryMode)
			if got != tt.want {
				t.Fatalf("DeterministicProgressPercent(%d,%d,%d,retry=%v)=%v want %v",
					tt.pending, tt.successful, tt.failed, tt.retryMode, got, tt.want)
			}
			counts := PhaseProgressCounts{
				Pending:    tt.pending,
				Successful: tt.successful,
				Failed:     tt.failed,
			}
			gotCounts := counts.ProgressPercent()
			if tt.retryMode {
				gotCounts = counts.ProgressPercentRetry()
			}
			if gotCounts != tt.want {
				t.Fatalf("PhaseProgressCounts percent=%v want %v", gotCounts, tt.want)
			}
		})
	}
}
