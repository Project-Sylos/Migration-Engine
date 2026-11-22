// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "testing"

func TestAutoscalerPrefsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	minV, maxV := 1, 32
	in := AutoscalerPrefs{
		Knobs: []AutoscalerKnobPref{{
			Queue: "src",
			Knob:  "WorkerCount",
			Min:   &minV,
			Max:   &maxV,
		}},
		LastViews: []AutoscalerKnobView{{
			Queue: "src",
			Knob:  "WorkerCount",
			Value: 8,
			Min:   1,
			Max:   32,
		}},
	}
	if err := store.PutAutoscalerPrefs(in); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.GetAutoscalerPrefs()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected prefs")
	}
	if len(got.Knobs) != 1 || got.Knobs[0].Queue != "src" || got.Knobs[0].Max == nil || *got.Knobs[0].Max != 32 {
		t.Fatalf("knobs=%+v", got.Knobs)
	}
	if len(got.LastViews) != 1 || got.LastViews[0].Value != 8 {
		t.Fatalf("views=%+v", got.LastViews)
	}
}
