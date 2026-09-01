package catbird

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// schedule is a parsed JobTypeOptions.Schedule: for each field, a bit per
// value it names. A minute matches when its minute, hour and month bits are
// set and the day rule below passes.
type schedule struct {
	minutes  uint64 // bit n set = minute n
	hours    uint32 // bit n set = hour n
	days     uint32 // bit n set = day of month n
	months   uint16 // bit n set = month n
	weekdays uint8  // bit n set = weekday n, Sunday 0
	// Whether the day fields were written as anything but "*" (a step on "*"
	// still counts as "*"). As in cron, when both are restricted a day
	// matching either one counts, so "0 0 1 * 1" runs on the first and on
	// Mondays; with one restricted the other says nothing.
	daysRestricted     bool
	weekdaysRestricted bool
}

// parseSchedule reads five fields separated by spaces: minute (0-59), hour
// (0-23), day of month (1-31), month (1-12), day of week (0-6, Sunday is 0
// and 7 also means Sunday). Each field is "*", a number, a range "8-17", a
// list "0,30", or a step "*/5", "8-17/2". Numbers only, no month or weekday
// names, and every process evaluates the schedule in UTC.
func parseSchedule(text string) (schedule, error) {
	fields := strings.Fields(text)
	if len(fields) != 5 {
		return schedule{}, fmt.Errorf("catbird: schedule %q: expected five fields: minute hour day-of-month month day-of-week", text)
	}
	var s schedule
	var masks [5]uint64
	bounds := [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	for i, field := range fields {
		mask, err := parseScheduleField(field, bounds[i][0], bounds[i][1])
		if err != nil {
			return schedule{}, fmt.Errorf("catbird: schedule %q: %w", text, err)
		}
		masks[i] = mask
	}
	// 7 is Sunday like 0, so both spellings set the same bit.
	if masks[4]>>7&1 == 1 {
		masks[4] |= 1
		masks[4] &^= 1 << 7
	}
	s.minutes = masks[0]
	s.hours = uint32(masks[1])
	s.days = uint32(masks[2])
	s.months = uint16(masks[3])
	s.weekdays = uint8(masks[4])
	s.daysRestricted = !strings.HasPrefix(fields[2], "*")
	s.weekdaysRestricted = !strings.HasPrefix(fields[4], "*")
	return s, nil
}

// parseScheduleField turns one field into a bitmask of the values between lo
// and hi it names.
func parseScheduleField(field string, lo, hi int) (uint64, error) {
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		values, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("bad step %q", part)
			}
			step = n
		}
		from, to := lo, hi
		switch fromText, toText, isRange := strings.Cut(values, "-"); {
		case values == "*":
		case isRange:
			a, errA := strconv.Atoi(fromText)
			b, errB := strconv.Atoi(toText)
			if errA != nil || errB != nil {
				return 0, fmt.Errorf("bad range %q", part)
			}
			from, to = a, b
		default:
			n, err := strconv.Atoi(values)
			if err != nil {
				return 0, fmt.Errorf("bad value %q", part)
			}
			from = n
			if !hasStep {
				to = n // a step widens a single value to "from here on", as in cron
			}
		}
		if from < lo || to > hi || from > to {
			return 0, fmt.Errorf("%q is outside %d-%d", part, lo, hi)
		}
		for n := from; n <= to; n += step {
			mask |= 1 << n
		}
	}
	if mask == 0 {
		return 0, fmt.Errorf("%q names no value", field)
	}
	return mask, nil
}

func (s schedule) matches(t time.Time) bool {
	return s.minutes>>t.Minute()&1 == 1 &&
		s.hours>>t.Hour()&1 == 1 &&
		s.months>>int(t.Month())&1 == 1 &&
		s.matchesDay(t)
}

func (s schedule) matchesDay(t time.Time) bool {
	day := s.days>>t.Day()&1 == 1
	weekday := s.weekdays>>int(t.Weekday())&1 == 1
	if s.daysRestricted && s.weekdaysRestricted {
		return day || weekday
	}
	return day && weekday
}

// next returns the first matching minute after t, or the zero time when the
// next ten years hold none. It steps by field — a wrong month jumps to the
// next month, a wrong day to the next midnight — so even a February 29
// schedule is a handful of iterations. Ten years because that schedule's
// longest legitimate wait is eight: the leap day skips a century year.
func (s schedule) next(t time.Time) time.Time {
	t = t.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(10, 0, 0)
	for t.Before(limit) {
		switch {
		case s.months>>int(t.Month())&1 == 0:
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
		case !s.matchesDay(t):
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
		case s.hours>>t.Hour()&1 == 0:
			t = t.Truncate(time.Hour).Add(time.Hour)
		case s.minutes>>t.Minute()&1 == 0:
			t = t.Add(time.Minute)
		default:
			return t
		}
	}
	return time.Time{}
}

// periodic enqueues a scheduled job type on its matching minutes. Handle
// declares one for every scheduled type it registers, so the processes that
// can run the type are the ones that tick it, and every one of them ticks:
// there is no leader, and the statement's two guards keep the result single.
// See enqueuePeriodic in client.go.
type periodic struct {
	runtime *Runtime
	jobType *JobType
	logger  *slog.Logger
}

// start ticks until ctx is canceled. The minute is computed from the clock at
// every wake-up, never carried across the sleep, so a process that was
// suspended resumes into the current minute and cannot enqueue ticks the
// schedule has moved past. A failed insert is retried while its minute lasts
// — the deduplication key makes a repeat free, and without the retry a
// single-process deployment loses the tick to one dropped connection.
func (p *periodic) start(ctx context.Context) {
	for ctx.Err() == nil {
		minute := time.Now().UTC().Truncate(time.Minute)
		var wait time.Duration
		if p.jobType.schedule.matches(minute) {
			if err := enqueuePeriodic(ctx, p.runtime.pool, p.jobType, minute); err != nil {
				if ctx.Err() == nil {
					p.logger.Error("catbird: periodic enqueue failed", "job_type", p.jobType.name, "err", err)
				}
				wait = time.Second
			}
		}
		if wait == 0 {
			wait = time.Until(p.jobType.schedule.next(minute))
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}
