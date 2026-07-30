// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

// EnsureLeaseBatchSizeAtLeast raises the DuckDB→pendingBuff pull size when below n
// (used when the active FS adapter supports batch mutations).
func (q *Queue) EnsureLeaseBatchSizeAtLeast(n int) {
	if q == nil || n <= 0 {
		return
	}
	if q.EffectiveLeaseBatchSize() >= n {
		return
	}
	if n > maxLeaseBatchSize {
		n = maxLeaseBatchSize
	}
	q.SetLeaseBatchSize(n)
}
