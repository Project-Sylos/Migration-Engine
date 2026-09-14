// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sort"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// DBOpAgg is one rolled-up timing row for an op name.
type DBOpAgg struct {
	Op              string `json:"op"`
	Count           int    `json:"count"`
	TotalDurationNs int64  `json:"totalDurationNs"`
	AvgDurationNs   int64  `json:"avgDurationNs"`
	MaxDurationNs   int64  `json:"maxDurationNs"`
	TotalRows       int64  `json:"totalRows"`
}

// DBOpSample is one recent timing sample for API/CLI export.
type DBOpSample struct {
	Op          string    `json:"op"`
	SQL         string    `json:"sql,omitempty"`
	Rows        int64     `json:"rows,omitempty"`
	DurationNs  int64     `json:"durationNs"`
	DurationMs  float64   `json:"durationMs"`
	Err         string    `json:"err,omitempty"`
	At          time.Time `json:"at"`
}

// DBOpsReport is seal buffer gauges plus optional op aggregates and raw samples.
type DBOpsReport struct {
	Seal    SealBufferTelemetry `json:"seal"`
	Summary []DBOpAgg           `json:"summary"`
	Samples []DBOpSample        `json:"samples,omitempty"`
}

// DBOpsReportOptions controls ListDBOps fetch and response shape.
type DBOpsReportOptions struct {
	OpFilter       string
	SampleLimit    int
	IncludeSamples bool
}

// DBOpsReport reads recent timing samples from the ops store and rolls them up by op name.
func (db *DB) DBOpsReport(opts DBOpsReportOptions) (DBOpsReport, error) {
	out := DBOpsReport{Seal: db.SealBufferTelemetry()}
	if db == nil || db.Ops() == nil {
		return out, fmt.Errorf("ops store not open")
	}
	limit := opts.SampleLimit
	if limit <= 0 {
		limit = 200
	}
	if limit > 10000 {
		limit = 10000
	}
	recs, err := db.Ops().ListDBOps(opts.OpFilter, limit)
	if err != nil {
		return out, err
	}
	out.Summary = SummarizeDBOpRecords(recs)
	if opts.IncludeSamples {
		out.Samples = make([]DBOpSample, len(recs))
		for i, r := range recs {
			out.Samples[i] = dbOpSampleFromRecord(r)
		}
	}
	return out, nil
}

// SummarizeDBOpRecords aggregates samples by op name (total/avg/max duration, row sum).
func SummarizeDBOpRecords(recs []opsdb.DBOpRecord) []DBOpAgg {
	byOp := map[string]*DBOpAgg{}
	for _, r := range recs {
		a := byOp[r.Op]
		if a == nil {
			a = &DBOpAgg{Op: r.Op}
			byOp[r.Op] = a
		}
		a.Count++
		a.TotalDurationNs += r.DurationNs
		a.TotalRows += r.Rows
		if r.DurationNs > a.MaxDurationNs {
			a.MaxDurationNs = r.DurationNs
		}
	}
	out := make([]DBOpAgg, 0, len(byOp))
	for _, a := range byOp {
		if a.Count > 0 {
			a.AvgDurationNs = a.TotalDurationNs / int64(a.Count)
		}
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalDurationNs == out[j].TotalDurationNs {
			return out[i].Op < out[j].Op
		}
		return out[i].TotalDurationNs > out[j].TotalDurationNs
	})
	return out
}

func dbOpSampleFromRecord(r opsdb.DBOpRecord) DBOpSample {
	return DBOpSample{
		Op:         r.Op,
		SQL:        r.SQL,
		Rows:       r.Rows,
		DurationNs: r.DurationNs,
		DurationMs: float64(r.DurationNs) / 1e6,
		Err:        r.Err,
		At:         r.At,
	}
}
