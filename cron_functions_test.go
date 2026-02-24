package catbird

import (
	"testing"
	"time"
)

func TestCronNextTick(t *testing.T) {
	client := getTestClient(t)

	tests := []struct {
		name string
		spec string
		from time.Time
		want time.Time
	}{
		{
			name: "hourly_keyword",
			spec: "@hourly",
			from: time.Date(2026, 2, 24, 10, 15, 30, 0, time.UTC),
			want: time.Date(2026, 2, 24, 11, 0, 0, 0, time.UTC),
		},
		{
			name: "daily_keyword",
			spec: "@daily",
			from: time.Date(2026, 2, 24, 23, 59, 59, 0, time.UTC),
			want: time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "every_fifteen_minutes",
			spec: "*/15 * * * *",
			from: time.Date(2026, 2, 24, 10, 14, 0, 0, time.UTC),
			want: time.Date(2026, 2, 24, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "weekday_specific",
			spec: "0 9 * * 1",
			from: time.Date(2026, 2, 24, 10, 0, 0, 0, time.UTC), // Tuesday
			want: time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC),
		},
		{
			name: "dom_dow_or_semantics",
			spec: "0 9 1 * 1",
			from: time.Date(2026, 2, 2, 10, 0, 0, 0, time.UTC), // Monday after 09:00
			want: time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got time.Time
			err := client.Conn.QueryRow(t.Context(), `SELECT cb_next_cron_tick($1, $2::timestamptz)`, tt.spec, tt.from).Scan(&got)
			if err != nil {
				t.Fatalf("cb_next_cron_tick failed: %v", err)
			}

			if !got.Equal(tt.want) {
				t.Fatalf("cb_next_cron_tick(%q, %s) = %s, want %s", tt.spec, tt.from.Format(time.RFC3339), got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
		})
	}
}

func TestCronNextTickInvalidSpec(t *testing.T) {
	client := getTestClient(t)

	var got time.Time
	err := client.Conn.QueryRow(
		t.Context(),
		`SELECT cb_next_cron_tick($1, $2::timestamptz)`,
		"61 * * * *",
		time.Date(2026, 2, 24, 10, 0, 0, 0, time.UTC),
	).Scan(&got)
	if err == nil {
		t.Fatal("expected error for invalid cron spec, got nil")
	}
}
