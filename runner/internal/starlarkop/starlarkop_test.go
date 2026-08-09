package starlarkop

import (
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

func yes() *bool { b := true; return &b }

func compile(t *testing.T, script string) *Program {
	t.Helper()
	p, err := Compile(Options{Script: script, StepID: "s1", Allowed: yes()})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return p
}

// runOne applies a script to one record and returns the result rendered as
// "kind:text" per field, so a test asserts the KIND as well as the value —
// a decimal that silently became a float is the defect most worth catching.
func runOne(t *testing.T, prog *Program, build func(*record.Builder)) (map[string]string, bool, error) {
	t.Helper()
	src := record.NewBatch()
	bld := src.Builder()
	bld.BeginMap()
	build(bld)
	bld.EndMap()
	rec := bld.Finish()

	dst := record.NewBatch()
	out, keep, err := prog.Run(context.Background(), dst, rec)
	if err != nil || !keep {
		return nil, keep, err
	}
	got := make(map[string]string, out.Len())
	for i := range out.Len() {
		v := out.Index(i)
		text := v.Text()
		switch v.Kind() {
		case record.KindString, record.KindBytes:
			text = v.String()
		case record.KindFloat:
			text = "float"
		case record.KindBool:
			text = "false"
			if v.Bool() {
				text = "true"
			}
		case record.KindNull:
			text = "null"
		}
		got[string(out.KeyAt(i))] = v.Kind().String() + ":" + text
	}
	return got, true, nil
}

// TestTheGapThisStepExists ToClose: arithmetic, string work and a conditional,
// none of which the declarative mapper can express.
func TestTheGapThisStepExistsToClose(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    total = rec.qty * rec.price
    return {
        "name":  rec.first.strip() + " " + rec.last.strip(),
        "total": total,
        "band":  "high" if total > decimal("100.00") else "low",
    }
`)
	got, keep, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("first")
		b.StringLiteral("  Ada ")
		b.KeyLiteral("last")
		b.StringLiteral("Lovelace  ")
		b.KeyLiteral("qty")
		b.Int(3)
		b.KeyLiteral("price")
		b.Decimal(1010, 2) // 10.10
	})
	if err != nil || !keep {
		t.Fatalf("run: err=%v keep=%v", err, keep)
	}
	if got["name"] != "string:Ada Lovelace" {
		t.Errorf("name = %s", got["name"])
	}
	// 3 × 10.10 = 30.30 EXACTLY, and still a decimal.
	if got["total"] != "decimal:30.30" {
		t.Errorf("total = %s, want decimal:30.30", got["total"])
	}
	if got["band"] != "string:low" {
		t.Errorf("band = %s", got["band"])
	}
}

// TestMoneyArithmeticIsExact is the reason Decimal is its own Starlark type:
// the same sum through float64 does not land on the cent.
func TestMoneyArithmeticIsExact(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    total = decimal("0.00")
    for i in range(1000):
        total = total + decimal("0.10")
    return {"total": total}
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got["total"] != "decimal:100.00" {
		t.Errorf("total = %s, want decimal:100.00", got["total"])
	}
}

// TestADecimalRefusesToMixWithAFloat — silently producing an inexact result is
// exactly what the type exists to prevent, so the refusal has to be loud.
func TestADecimalRefusesToMixWithAFloat(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {"bad": rec.price * 1.5}
`)
	_, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("price")
		b.Decimal(1010, 2)
	})
	if err == nil {
		t.Fatal("decimal × float was accepted")
	}
	if !strings.Contains(err.Error(), "inexact") {
		t.Errorf("error = %v, want it to explain the inexactness", err)
	}
}

func TestDecimalDivisionPointsAtRescale(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {"bad": rec.price / rec.price}
`)
	_, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("price")
		b.Decimal(1010, 2)
	})
	if err == nil {
		t.Fatal("decimal division was accepted")
	}
	if !strings.Contains(err.Error(), "rescale") {
		t.Errorf("error = %v, want it to point at the explicit rounding call", err)
	}
}

func TestRescaleRoundsHalfAwayFromZero(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {"up": rec.a.rescale(2), "down": rec.b.rescale(2), "neg": rec.c.rescale(2)}
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("a")
		b.Decimal(10105, 3) // 10.105 → 10.11
		b.KeyLiteral("b")
		b.Decimal(10104, 3) // 10.104 → 10.10
		b.KeyLiteral("c")
		b.Decimal(-10105, 3) // -10.105 → -10.11
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for field, want := range map[string]string{
		"up": "decimal:10.11", "down": "decimal:10.10", "neg": "decimal:-10.11",
	} {
		if got[field] != want {
			t.Errorf("%s = %s, want %s", field, got[field], want)
		}
	}
}

// TestReturningNoneDropsTheRecord — a script is also a filter.
func TestReturningNoneDropsTheRecord(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    if rec.drop:
        return None
    return {"kept": True}
`)
	_, keep, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("drop")
		b.Bool(true)
	})
	if err != nil {
		t.Fatal(err)
	}
	if keep {
		t.Error("returning None must drop the record")
	}
	_, keep, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("drop")
		b.Bool(false)
	})
	if err != nil || !keep {
		t.Errorf("a returned record must be kept: keep=%v err=%v", keep, err)
	}
}

// TestListsAreReadable closes the other half of the gap: the declarative
// mapper builds nested maps only and cannot touch a repeating group.
func TestListsAreReadable(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    total = decimal("0.00")
    skus = []
    for line in rec.lines:
        total = total + line.amount
        skus.append(line.sku)
    return {"total": total, "skus": skus, "count": len(rec.lines)}
`)
	src := record.NewBatch()
	bld := src.Builder()
	bld.BeginMap()
	bld.KeyLiteral("lines")
	bld.BeginList()
	for i, amt := range []int64{1050, 2500} {
		bld.BeginMap()
		bld.KeyLiteral("sku")
		bld.StringLiteral([]string{"A", "B"}[i])
		bld.KeyLiteral("amount")
		bld.Decimal(amt, 2)
		bld.EndMap()
	}
	bld.EndList()
	bld.EndMap()
	rec := bld.Finish()

	dst := record.NewBatch()
	out, keep, err := prog.Run(context.Background(), dst, rec)
	if err != nil || !keep {
		t.Fatalf("run: err=%v keep=%v", err, keep)
	}
	total, _ := out.Field("total")
	if total.Text() != "35.50" {
		t.Errorf("total = %q, want 35.50", total.Text())
	}
	count, _ := out.Field("count")
	if count.Int() != 2 {
		t.Errorf("count = %d", count.Int())
	}
	skus, _ := out.Field("skus")
	if skus.Kind() != record.KindList || skus.Len() != 2 || skus.Index(0).String() != "A" {
		t.Errorf("skus = %v len=%d", skus.Kind(), skus.Len())
	}
}

func TestAnUntouchedFieldKeepsItsExactKind(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    return {"at": rec.at, "amount": rec.amount, "note": "x"}
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("at")
		b.Date(20673)
		b.KeyLiteral("amount")
		b.Decimal(1010, 2)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["at"] != "date:2026-08-08" {
		t.Errorf("at = %s, want the date unchanged", got["at"])
	}
	if got["amount"] != "decimal:10.10" {
		t.Errorf("amount = %s, want the decimal unchanged", got["amount"])
	}
}
