package csvf

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

func TestReadTyped(t *testing.T) {
	in := "id,name,amount,active,notes\n" +
		"1,ada,10.5,true,hello\n" +
		"2,\"quoted, name\",0.25,false,\"multi\nline\"\n" +
		"3,empty,,,\n"
	r := NewReader(strings.NewReader(in), ReaderOptions{
		Types: map[string]ColumnType{"id": TypeInt, "amount": TypeFloat, "active": TypeBool},
	})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 3 {
		t.Fatalf("len = %d", b.Len())
	}
	r0 := b.Record(0)
	if v, _ := r0.Field("id"); v.Kind() != record.KindInt || v.Int() != 1 {
		t.Errorf("id = %v", v)
	}
	if v, _ := r0.Field("amount"); v.Float() != 10.5 {
		t.Errorf("amount = %v", v.Float())
	}
	if v, _ := r0.Field("active"); !v.Bool() {
		t.Error("active should be true")
	}
	r1 := b.Record(1)
	if v, _ := r1.Field("name"); v.String() != "quoted, name" {
		t.Errorf("name = %q", v.String())
	}
	if v, _ := r1.Field("notes"); v.String() != "multi\nline" {
		t.Errorf("notes = %q", v.String())
	}
	// empty typed cells → null
	r2 := b.Record(2)
	if v, _ := r2.Field("amount"); !v.IsNull() {
		t.Errorf("empty amount = %v, want null", v.Kind())
	}
	if v, _ := r2.Field("active"); !v.IsNull() {
		t.Errorf("empty active = %v, want null", v.Kind())
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestReadNoHeader(t *testing.T) {
	r := NewReader(strings.NewReader("a,b\nc,d\n"), ReaderOptions{NoHeader: true})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != 2 {
		t.Fatalf("len = %d", b.Len())
	}
	if v, ok := b.Record(0).Field("col0"); !ok || v.String() != "a" {
		t.Errorf("col0 = %v", v)
	}
	if v, ok := b.Record(1).Field("col1"); !ok || v.String() != "d" {
		t.Errorf("col1 = %v", v)
	}
}

func TestTypeErrors(t *testing.T) {
	r := NewReader(strings.NewReader("id\nnope\n"), ReaderOptions{
		Types: map[string]ColumnType{"id": TypeInt},
	})
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "row 2") {
		t.Fatalf("err = %v, want row 2 int error", err)
	}
}

func TestEmptyInput(t *testing.T) {
	r := NewReader(strings.NewReader(""), ReaderOptions{})
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestWriteRoundTrip(t *testing.T) {
	in := "id,name,amount,active\n1,ada,10.5,true\n2,bob,,false\n"
	r := NewReader(strings.NewReader(in), ReaderOptions{
		Types: map[string]ColumnType{"id": TypeInt, "amount": TypeFloat, "active": TypeBool},
	})
	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{})
	ctx := context.Background()
	for {
		b, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Write(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	want := "id,name,amount,active\n1,ada,10.5,true\n2,bob,,false\n"
	if got != want {
		t.Errorf("round trip:\n got %q\nwant %q", got, want)
	}
}

func TestWriteMissingAndPinnedColumns(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.Int(1)
	bld.KeyLiteral("b")
	bld.StringLiteral("x")
	bld.EndMap()
	batch.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out, WriterOptions{Columns: []string{"b", "missing"}})
	if err := w.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "b,missing\nx,\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteRejectsContainers(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("nested")
	bld.BeginList()
	bld.Int(1)
	bld.EndList()
	bld.EndMap()
	batch.Append(bld.Finish())

	w := NewWriter(io.Discard, WriterOptions{})
	if err := w.Write(context.Background(), batch); err == nil {
		t.Fatal("expected container error")
	}
}

func BenchmarkRead(b *testing.B) {
	var input strings.Builder
	input.WriteString("id,name,email,amount,active\n")
	for i := range 1000 {
		input.WriteString("12345,customer-name,someone@example.com,1234.56,true\n")
		_ = i
	}
	data := input.String()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	ctx := context.Background()
	opts := ReaderOptions{Types: map[string]ColumnType{"id": TypeInt, "amount": TypeFloat, "active": TypeBool}}
	for b.Loop() {
		r := NewReader(strings.NewReader(data), opts)
		for {
			if _, err := r.Next(ctx); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}
