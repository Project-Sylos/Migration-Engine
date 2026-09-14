// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestEnqueueAuditSnapshotLatestWinsWithoutBlocking(t *testing.T) {
	o := NewQueueObserver(nil, time.Hour)
	defer o.updateTicker.Stop()

	first := map[string]ExternalQueueMetrics{
		"src": {Round: 1, RoundCompleted: 10},
	}
	second := map[string]ExternalQueueMetrics{
		"src": {Round: 1, RoundCompleted: 99},
	}
	queues := map[string]*queue.Queue{}

	done := make(chan struct{})
	go func() {
		o.enqueueAuditSnapshot(first, queues)
		o.enqueueAuditSnapshot(second, queues)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueueAuditSnapshot blocked while audit channel was full")
	}

	snap := <-o.auditCh
	if len(snap.rows) != 1 {
		t.Fatalf("rows=%d want 1", len(snap.rows))
	}
	if snap.rows[0].key != "src-traversal" {
		t.Fatalf("key=%q", snap.rows[0].key)
	}
	if !strings.Contains(snap.rows[0].json, `"round_completed":`+strconv.Itoa(99)) {
		t.Fatalf("expected latest snapshot with round_completed=99, got %s", snap.rows[0].json)
	}
	select {
	case <-o.auditCh:
		t.Fatal("expected only one buffered audit snapshot")
	default:
	}
}
