package scheduler

import (
	"testing"
	"time"
)

func TestParseCron(t *testing.T) {
	valid := []string{"* * * * *", "*/5 * * * *", "0 3 * * 1-5", "30 2 1 */2 *", "@hourly", "0 0 1 jan mon"}
	for _, expr := range valid {
		if err := ParseCron(expr); err != nil {
			t.Errorf("ParseCron(%q) = %v, want nil", expr, err)
		}
	}
	invalid := []string{"", "* * * *", "61 * * * *", "* * * * * *", "not cron", "0 25 * * *"}
	for _, expr := range invalid {
		if err := ParseCron(expr); err == nil {
			t.Errorf("ParseCron(%q) accepted", expr)
		}
	}
}

func TestNextAfter(t *testing.T) {
	base := time.Date(2026, 7, 19, 10, 30, 45, 0, time.UTC)

	cases := []struct {
		expr string
		want time.Time
	}{
		{"* * * * *", time.Date(2026, 7, 19, 10, 31, 0, 0, time.UTC)},
		{"0 * * * *", time.Date(2026, 7, 19, 11, 0, 0, 0, time.UTC)},
		{"0 3 * * *", time.Date(2026, 7, 20, 3, 0, 0, 0, time.UTC)},
		// 2026-07-19 is a Sunday; next weekday is Monday the 20th.
		{"0 9 * * 1-5", time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)},
		// DOM/DOW union rule: day 1 OR Monday — the 20th (Mon) precedes
		// Aug 1.
		{"0 0 1 * 1", time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		got, err := NextAfter(c.expr, base)
		if err != nil {
			t.Errorf("NextAfter(%q): %v", c.expr, err)
			continue
		}
		if !got.Equal(c.want) {
			t.Errorf("NextAfter(%q, %s) = %s, want %s", c.expr, base, got, c.want)
		}
	}

	// Non-UTC input is computed in UTC.
	syd := time.FixedZone("AEST", 10*3600)
	got, err := NextAfter("0 3 * * *", time.Date(2026, 7, 19, 20, 0, 0, 0, syd)) // 10:00 UTC
	if err != nil || !got.Equal(time.Date(2026, 7, 20, 3, 0, 0, 0, time.UTC)) {
		t.Errorf("UTC normalization: got %s, %v", got, err)
	}

	if _, err := NextAfter("garbage", base); err == nil {
		t.Error("NextAfter accepted garbage")
	}
}
