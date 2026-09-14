// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "encoding/json"

func encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodeNode(b []byte) (NodeRecord, error) {
	var n NodeRecord
	err := json.Unmarshal(b, &n)
	return n, err
}

func decodeStatus(b []byte) (StatusRecord, error) {
	var s StatusRecord
	err := json.Unmarshal(b, &s)
	return s, err
}

func decodeIDMap(b []byte) (IDMapRecord, error) {
	var m IDMapRecord
	err := json.Unmarshal(b, &m)
	return m, err
}

func decodeKids(b []byte) ([]KidRecord, error) {
	var kids []KidRecord
	err := json.Unmarshal(b, &kids)
	return kids, err
}

func decodeLog(b []byte) (LogRecord, error) {
	var rec LogRecord
	err := json.Unmarshal(b, &rec)
	return rec, err
}

func decodeQueueStats(b []byte) (QueueStatsRecord, error) {
	var rec QueueStatsRecord
	err := json.Unmarshal(b, &rec)
	return rec, err
}

func decodeDBOp(b []byte) (DBOpRecord, error) {
	var rec DBOpRecord
	err := json.Unmarshal(b, &rec)
	return rec, err
}

func decodeInto(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
