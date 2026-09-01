package catbird

import (
	"testing"
	"time"
)

func mustParseSchedule(t *testing.T, text string) schedule {
	t.Helper()
	s, err := parseSchedule(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return s
}

func at(y int, m time.Month, d, hh, mm int) time.Time {
	return time.Date(y, m, d, hh, mm, 0, 0, time.UTC)
}

func TestScheduleMatches(t *testing.T) {
	// 2026-09-01 is a Tuesday, 2026-09-06 a Sunday, 2026-09-07 a Monday.
	if w := at(2026, 9, 1, 0, 0).Weekday(); w != time.Tuesday {
		t.Fatalf("2026-09-01 is a %v, the cases below assume Tuesday", w)
	}
	cases := []struct {
		schedule string
		time     time.Time
		want     bool
	}{
		{"30 9 * * *", at(2026, 9, 1, 9, 30), true},
		{"30 9 * * *", at(2026, 9, 1, 9, 31), false},
		{"30 9 * * *", at(2026, 9, 1, 10, 30), false},
		{"*/15 * * * *", at(2026, 9, 1, 3, 45), true},
		{"*/15 * * * *", at(2026, 9, 1, 3, 10), false},
		{"0,30 8-17 * * *", at(2026, 9, 1, 17, 30), true},
		{"0,30 8-17 * * *", at(2026, 9, 1, 18, 0), false},
		{"0 0 * * 1", at(2026, 9, 7, 0, 0), true},  // a Monday
		{"0 0 * * 1", at(2026, 9, 1, 0, 0), false}, // a Tuesday
		{"0 0 * * 7", at(2026, 9, 6, 0, 0), true},  // 7 is Sunday, like 0
		{"0 0 * * 0", at(2026, 9, 6, 0, 0), true},
		{"0 0 1 * *", at(2026, 9, 1, 0, 0), true},
		{"0 0 1 * *", at(2026, 9, 2, 0, 0), false},
		// Both day fields restricted: a day matching either one counts.
		{"0 0 1 * 1", at(2026, 9, 1, 0, 0), true},  // the 1st, a Tuesday
		{"0 0 1 * 1", at(2026, 9, 7, 0, 0), true},  // a Monday, not the 1st
		{"0 0 1 * 1", at(2026, 9, 8, 0, 0), false}, // neither
		{"0 12 * 2 *", at(2026, 2, 10, 12, 0), true},
		{"0 12 * 2 *", at(2026, 3, 10, 12, 0), false},
		{"1/10 * * * *", at(2026, 9, 1, 0, 41), true}, // a step widens a value to "from here on"
		{"1/10 * * * *", at(2026, 9, 1, 0, 40), false},
	}
	for _, c := range cases {
		s := mustParseSchedule(t, c.schedule)
		if got := s.matches(c.time); got != c.want {
			t.Errorf("%q matches %v = %v, want %v", c.schedule, c.time, got, c.want)
		}
	}
}

func TestScheduleParseErrors(t *testing.T) {
	for _, text := range []string{
		"",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * 32 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"a * * * *",
		"5-1 * * * *",
		"1-60 * * * *",
		"1,,2 * * * *",
	} {
		if _, err := parseSchedule(text); err == nil {
			t.Errorf("parse %q: expected an error", text)
		}
	}
}

func TestScheduleNext(t *testing.T) {
	cases := []struct {
		schedule string
		after    time.Time
		want     time.Time
	}{
		{"30 9 * * *", at(2026, 9, 1, 9, 0), at(2026, 9, 1, 9, 30)},
		// next is strictly after: from a matching minute it moves on.
		{"30 9 * * *", at(2026, 9, 1, 9, 30), at(2026, 9, 2, 9, 30)},
		{"*/5 * * * *", at(2026, 9, 1, 12, 3), at(2026, 9, 1, 12, 5)},
		{"0 0 1 * *", at(2026, 9, 2, 0, 0), at(2026, 10, 1, 0, 0)},
		{"0 12 * 2 *", at(2026, 3, 1, 0, 0), at(2027, 2, 1, 12, 0)},
		{"0 0 29 2 *", at(2026, 3, 1, 0, 0), at(2028, 2, 29, 0, 0)},
	}
	for _, c := range cases {
		s := mustParseSchedule(t, c.schedule)
		if got := s.next(c.after); !got.Equal(c.want) {
			t.Errorf("%q next after %v = %v, want %v", c.schedule, c.after, got, c.want)
		}
	}

	// A schedule that names no real day finds nothing and reports it as the
	// zero time, which NewJobType turns into a panic at declaration.
	s := mustParseSchedule(t, "0 0 31 2 *")
	if got := s.next(at(2026, 1, 1, 0, 0)); !got.IsZero() {
		t.Errorf("%q next = %v, want the zero time", "0 0 31 2 *", got)
	}
}
