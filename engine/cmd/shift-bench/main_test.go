package main

import (
	"context"
	"io"
	"testing"
)

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"64MiB":    64 << 20,
		"1GiB":     1 << 30,
		"2KiB":     2 << 10,
		"100B":     100,
		"1.5\tGiB": (3 << 30) / 2,
		"5MB":      5_000_000,
		"123":      123,
	}
	for in, want := range cases {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "MiB", "x1GiB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) should fail", bad)
		}
	}
}

func TestGeneratorDeterministicAndBounded(t *testing.T) {
	read := func() ([]byte, int64) {
		g := newGenerator("ndjson", 1<<20, 100)
		data, err := io.ReadAll(g)
		if err != nil {
			t.Fatal(err)
		}
		return data, g.records
	}
	a, na := read()
	b, nb := read()
	if string(a) != string(b) || na != nb {
		t.Fatal("generator is not deterministic")
	}
	if len(a) < 1<<20 || len(a) > (1<<20)+(70<<10) {
		t.Fatalf("generator size %d not near 1MiB limit", len(a))
	}
}

func TestScenariosSmoke(t *testing.T) {
	r := runner{size: 1 << 20, watermark: 256 << 10, groups: 5000, spillDir: t.TempDir()}
	for _, sc := range []string{"transform", "csv", "aggregate", "baseline"} {
		res, err := r.run(sc)
		if err != nil {
			t.Fatalf("%s: %v", sc, err)
		}
		if res.report.RecordsOut == 0 {
			t.Errorf("%s: no output records", sc)
		}
	}
	// aggregate at tiny watermark must have spilled
	rep, spilled, err := r.aggregate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if spilled == 0 {
		t.Error("aggregate at 256KiB watermark should spill")
	}
	// ~4.8k records over 5000 possible keys: expect most-but-not-all groups.
	if rep.RecordsOut < 2000 || rep.RecordsOut > 5000 {
		t.Errorf("aggregate groups out = %d, want 2000..5000", rep.RecordsOut)
	}
}
