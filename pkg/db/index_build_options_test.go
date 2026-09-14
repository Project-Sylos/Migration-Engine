package db

import "testing"

func TestClampIndexBuildThreads(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, 1},
		{-3, 1},
		{1, 1},
		{2, 2},
		{4, 4},
		{5, 4},
		{99, 4},
	}
	for _, tc := range cases {
		if got := ClampIndexBuildThreads(tc.in); got != tc.want {
			t.Fatalf("ClampIndexBuildThreads(%d)=%d want %d", tc.in, got, tc.want)
		}
	}
}
