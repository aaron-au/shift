package main

import (
	"fmt"
	"io"
)

// generator produces deterministic synthetic data as an io.Reader, so
// benchmarks measure the engine, not disk I/O, and runs are reproducible.
// A fixed-seed LCG drives field variation.
type generator struct {
	format    string // "ndjson" or "csv"
	limit     int64  // total bytes to emit
	emitted   int64
	records   int64
	groups    int64
	buf       []byte
	off       int
	rng       uint64
	wroteHead bool
}

func newGenerator(format string, limit, groups int64) *generator {
	return &generator{format: format, limit: limit, groups: groups, rng: 0x5DEECE66D}
}

func (g *generator) next() uint64 {
	g.rng = g.rng*6364136223846793005 + 1442695040888963407
	return g.rng
}

func (g *generator) Read(p []byte) (int, error) {
	if g.off >= len(g.buf) {
		if g.emitted >= g.limit {
			return 0, io.EOF
		}
		g.fill()
	}
	n := copy(p, g.buf[g.off:])
	g.off += n
	return n, nil
}

func (g *generator) fill() {
	g.buf = g.buf[:0]
	g.off = 0
	if g.format == "csv" && !g.wroteHead {
		g.buf = append(g.buf, "id,name,email,amount,active,region,city,postcode\n"...)
		g.wroteHead = true
	}
	// Emit ~64KiB per fill.
	for len(g.buf) < 64<<10 && g.emitted+int64(len(g.buf)) < g.limit {
		r := g.next()
		id := g.records
		region := r % uint64(g.groups) //nolint:gosec // groups is validated positive at startup
		amount := float64(r%1_000_000) / 100.0
		active := r&1 == 0
		city := cities[r%uint64(len(cities))]
		switch g.format {
		case "csv":
			g.buf = fmt.Appendf(g.buf, "%d,customer-%06d,user%d@example.com,%.2f,%t,g%05d,%s,%04d\n",
				id, id%1_000_000, id, amount, active, region, city, 3000+r%8000)
		default:
			g.buf = fmt.Appendf(g.buf,
				`{"id":%d,"name":"customer-%06d","email":"user%d@example.com","amount":%.2f,"active":%t,"region":"g%05d","tags":["retail","au"],"address":{"street":"%d Collins St","city":"%s","postcode":"%04d"}}`+"\n",
				id, id%1_000_000, id, amount, active, region, id%400+1, city, 3000+r%8000)
		}
		g.records++
	}
	g.emitted += int64(len(g.buf))
}

var cities = []string{"Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide", "Hobart", "Darwin", "Canberra"}
