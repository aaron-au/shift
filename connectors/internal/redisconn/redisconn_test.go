package redisconn

import (
	"context"
	"errors"
	"io"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// --- in-memory fake redisClient -------------------------------------------

type setCall struct {
	key, val string
	ttl      time.Duration
}

// fakeRedis is a deterministic in-memory stand-in for a Redis server. Scan
// paginates over ALL keys in pageSize chunks and filters each page by the glob
// (mirroring real SCAN, where COUNT bounds the table walk and MATCH filters
// after) so an early page can come back empty with a non-zero cursor.
type fakeRedis struct {
	types    map[string]string
	strs     map[string]string
	hashes   map[string]map[string]string
	lists    map[string][]string
	pageSize int

	sets    []setCall
	deleted []string
	closed  bool

	scanErr, typeErr, getErr, hErr, lErr, setErr, delErr error
}

func newFake() *fakeRedis {
	return &fakeRedis{
		types:  map[string]string{},
		strs:   map[string]string{},
		hashes: map[string]map[string]string{},
		lists:  map[string][]string{},
	}
}

func (f *fakeRedis) putString(k, v string)                 { f.types[k] = "string"; f.strs[k] = v }
func (f *fakeRedis) putHash(k string, h map[string]string) { f.types[k] = "hash"; f.hashes[k] = h }
func (f *fakeRedis) putList(k string, l []string)          { f.types[k] = "list"; f.lists[k] = l }
func (f *fakeRedis) putType(k, typ string)                 { f.types[k] = typ }

func (f *fakeRedis) sortedKeys() []string {
	keys := make([]string, 0, len(f.types))
	for k := range f.types {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (f *fakeRedis) Scan(_ context.Context, cursor uint64, match string, _ int64) ([]string, uint64, error) {
	if f.scanErr != nil {
		return nil, 0, f.scanErr
	}
	keys := f.sortedKeys()
	n := uint64(len(keys))
	if cursor >= n {
		return nil, 0, nil
	}
	page := n + 1 // default: one page holds everything
	if f.pageSize > 0 {
		page = uint64(f.pageSize)
	}
	end := cursor + page
	if end > n {
		end = n
	}
	var out []string
	for _, k := range keys[cursor:end] { // slice indices accept any integer type
		ok, err := path.Match(match, k)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			out = append(out, k)
		}
	}
	next := end
	if end >= n {
		next = 0
	}
	return out, next, nil
}

func (f *fakeRedis) Type(_ context.Context, key string) (string, error) {
	if f.typeErr != nil {
		return "", f.typeErr
	}
	if t, ok := f.types[key]; ok {
		return t, nil
	}
	return "none", nil
}

func (f *fakeRedis) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.strs[key], nil
}

func (f *fakeRedis) HGetAll(_ context.Context, key string) (map[string]string, error) {
	if f.hErr != nil {
		return nil, f.hErr
	}
	return f.hashes[key], nil
}

func (f *fakeRedis) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	if f.lErr != nil {
		return nil, f.lErr
	}
	l := f.lists[key]
	if start < 0 {
		start = 0
	}
	if stop >= int64(len(l)) {
		stop = int64(len(l)) - 1
	}
	if len(l) == 0 || start > stop {
		return nil, nil
	}
	return l[start : stop+1], nil
}

func (f *fakeRedis) Set(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.sets = append(f.sets, setCall{key: key, val: value, ttl: ttl})
	f.types[key] = "string"
	f.strs[key] = value
	return nil
}

func (f *fakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	if f.delErr != nil {
		return 0, f.delErr
	}
	var n int64
	for _, k := range keys {
		f.deleted = append(f.deleted, k)
		if _, ok := f.types[k]; ok {
			delete(f.types, k)
			delete(f.strs, k)
			delete(f.hashes, k)
			delete(f.lists, k)
			n++
		}
	}
	return n, nil
}

func (f *fakeRedis) Close() error { f.closed = true; return nil }

// injects f as the client for any verb.
func inject(f *fakeRedis) func(*config) (redisClient, error) {
	return func(*config) (redisClient, error) { return f, nil }
}

func baseConfig(extra string) []byte {
	return []byte(`{"addr":"redis.example.com:6379","allow_local":true` + extra + `}`)
}

// --- get ------------------------------------------------------------------

func drainGet(t *testing.T, s *getSource) []record.Value {
	t.Helper()
	ctx := context.Background()
	var out []record.Value
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		// Records are valid only until the next Next: copy out via CopyValue.
		for _, rec := range b.Records() {
			out = append(out, record.CopyValue(record.NewBatch(), rec))
		}
	}
	return out
}

func TestGetScanPaginationAndDecode(t *testing.T) {
	f := newFake()
	f.pageSize = 2 // force multiple SCAN pages
	f.putString("s:1", "hello")
	f.putString("s:2", "world")
	f.putHash("h:1", map[string]string{"b": "2", "a": "1"})
	f.putList("l:1", []string{"x", "y", "z"})
	f.putType("set:1", "set") // unsupported type → null value

	s := &getSource{open: inject(f)}
	if err := s.Open(context.Background(), baseConfig(`,"pattern":"*"`)); err != nil {
		t.Fatalf("open: %v", err)
	}

	recs := drainGet(t, s)
	if len(recs) != 5 {
		t.Fatalf("got %d records, want 5", len(recs))
	}
	byKey := map[string]record.Value{}
	for _, r := range recs {
		k, _ := r.Field("key")
		byKey[k.String()] = r
	}

	// string
	if v, _ := byKey["s:1"].Field("value"); v.String() != "hello" {
		t.Errorf("s:1 value = %q", v.String())
	}
	if ty, _ := byKey["s:1"].Field("type"); ty.String() != "string" {
		t.Errorf("s:1 type = %q", ty.String())
	}
	// hash → map with sorted fields
	hv, _ := byKey["h:1"].Field("value")
	if hv.Kind() != record.KindMap || hv.Len() != 2 {
		t.Fatalf("h:1 value kind=%s len=%d", hv.Kind(), hv.Len())
	}
	if a, _ := hv.Field("a"); a.String() != "1" {
		t.Errorf("h:1.a = %q", a.String())
	}
	// list
	lv, _ := byKey["l:1"].Field("value")
	if lv.Kind() != record.KindList || lv.Len() != 3 || lv.Index(2).String() != "z" {
		t.Errorf("l:1 value = kind %s len %d", lv.Kind(), lv.Len())
	}
	// unsupported type → null value, type recorded
	sv, _ := byKey["set:1"].Field("value")
	if !sv.IsNull() {
		t.Errorf("set:1 value not null: %v", sv)
	}
	if ty, _ := byKey["set:1"].Field("type"); ty.String() != "set" {
		t.Errorf("set:1 type = %q", ty.String())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !f.closed {
		t.Error("Close did not close the client")
	}
}

func TestGetPatternFilterAndEmptyPages(t *testing.T) {
	f := newFake()
	f.pageSize = 1 // each page holds one key; non-matching pages come back empty
	f.putString("user:1", "a")
	f.putString("other:1", "b")
	f.putString("user:2", "c")
	f.putString("misc:1", "d")

	s := &getSource{open: inject(f)}
	if err := s.Open(context.Background(), baseConfig(`,"pattern":"user:*"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	recs := drainGet(t, s)
	got := map[string]bool{}
	for _, r := range recs {
		k, _ := r.Field("key")
		got[k.String()] = true
	}
	if len(got) != 2 || !got["user:1"] || !got["user:2"] {
		t.Fatalf("pattern filter yielded %v, want user:1 + user:2", got)
	}
}

func TestGetEmptyKeyspace(t *testing.T) {
	s := &getSource{open: inject(newFake())}
	if err := s.Open(context.Background(), baseConfig("")); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("empty keyspace Next = %v, want EOF", err)
	}
	// default pattern applied
	if s.cfg.Pattern != "*" {
		t.Errorf("default pattern = %q, want *", s.cfg.Pattern)
	}
}

func TestGetScanError(t *testing.T) {
	f := newFake()
	f.scanErr = errors.New("boom")
	s := &getSource{open: inject(f)}
	_ = s.Open(context.Background(), baseConfig(""))
	if _, err := s.Next(context.Background()); err == nil {
		t.Fatal("expected scan error to propagate")
	}
}

// --- set ------------------------------------------------------------------

func writeBatch(t *testing.T, s *setSink, build func(*record.Builder, *record.Batch)) error {
	t.Helper()
	b := record.NewBatch()
	build(b.Builder(), b)
	return s.Write(context.Background(), b)
}

func TestSetStaticKeyAndTTL(t *testing.T) {
	f := newFake()
	s := &setSink{open: inject(f)}
	if err := s.Open(context.Background(), baseConfig(`,"key":"greeting","ttl_seconds":60`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	err := writeBatch(t, s, func(bld *record.Builder, b *record.Batch) {
		bld.BeginMap()
		bld.KeyLiteral("value")
		bld.StringLiteral("hi")
		bld.EndMap()
		b.Append(bld.Finish())
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(f.sets) != 1 {
		t.Fatalf("got %d SETs, want 1", len(f.sets))
	}
	got := f.sets[0]
	if got.key != "greeting" || got.val != "hi" || got.ttl != 60*time.Second {
		t.Fatalf("SET = %+v, want greeting/hi/60s", got)
	}
	if err := s.Close(); err != nil || !f.closed {
		t.Fatalf("close err=%v closed=%v", err, f.closed)
	}
}

func TestSetPerRecordKeyAndValueField(t *testing.T) {
	f := newFake()
	s := &setSink{open: inject(f)}
	if err := s.Open(context.Background(), baseConfig(`,"value_field":"payload"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	if s.cfg.ValueField != "payload" {
		t.Fatalf("value_field = %q", s.cfg.ValueField)
	}
	err := writeBatch(t, s, func(bld *record.Builder, b *record.Batch) {
		for i, k := range []string{"a", "b"} {
			bld.BeginMap()
			bld.KeyLiteral("key")
			bld.StringLiteral(k)
			bld.KeyLiteral("payload")
			bld.Int(int64(i + 1)) // non-string value → stringified
			bld.EndMap()
			b.Append(bld.Finish())
		}
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(f.sets) != 2 || f.sets[0] != (setCall{key: "a", val: "1"}) || f.sets[1] != (setCall{key: "b", val: "2"}) {
		t.Fatalf("SETs = %+v", f.sets)
	}
}

func TestSetErrors(t *testing.T) {
	newSink := func(cfg string) *setSink {
		s := &setSink{open: inject(newFake())}
		if err := s.Open(context.Background(), baseConfig(cfg)); err != nil {
			t.Fatalf("open: %v", err)
		}
		return s
	}

	// missing value field
	err := writeBatch(t, newSink(""), func(bld *record.Builder, b *record.Batch) {
		bld.BeginMap()
		bld.KeyLiteral("key")
		bld.StringLiteral("k")
		bld.EndMap()
		b.Append(bld.Finish())
	})
	if err == nil || !strings.Contains(err.Error(), "value") {
		t.Fatalf("missing value: got %v", err)
	}

	// no static key and no key field
	err = writeBatch(t, newSink(""), func(bld *record.Builder, b *record.Batch) {
		bld.BeginMap()
		bld.KeyLiteral("value")
		bld.StringLiteral("v")
		bld.EndMap()
		b.Append(bld.Finish())
	})
	if err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("missing key: got %v", err)
	}

	// composite value rejected (string values first-class)
	err = writeBatch(t, newSink(`,"key":"k"`), func(bld *record.Builder, b *record.Batch) {
		bld.BeginMap()
		bld.KeyLiteral("value")
		bld.BeginList()
		bld.Int(1)
		bld.EndList()
		bld.EndMap()
		b.Append(bld.Finish())
	})
	if err == nil || !strings.Contains(err.Error(), "scalar") {
		t.Fatalf("composite value: got %v", err)
	}

	// empty key field
	err = writeBatch(t, newSink(""), func(bld *record.Builder, b *record.Batch) {
		bld.BeginMap()
		bld.KeyLiteral("key")
		bld.StringLiteral("")
		bld.KeyLiteral("value")
		bld.StringLiteral("v")
		bld.EndMap()
		b.Append(bld.Finish())
	})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty key: got %v", err)
	}
}

// --- delete ---------------------------------------------------------------

func runDelete(t *testing.T, f *fakeRedis, key string) record.Value {
	t.Helper()
	s := &deleteSource{open: inject(f)}
	if err := s.Open(context.Background(), baseConfig(`,"key":"`+key+`"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	b, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	recs := b.Records()
	if len(recs) != 1 {
		t.Fatalf("delete emitted %d records, want 1", len(recs))
	}
	rec := record.CopyValue(record.NewBatch(), recs[0])
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return rec
}

func TestDeleteStatusAndIdempotency(t *testing.T) {
	f := newFake()
	f.putString("victim", "x")

	rec := runDelete(t, f, "victim")
	if op, _ := rec.Field("op"); op.String() != "delete" {
		t.Errorf("op = %q", op.String())
	}
	if d, _ := rec.Field("deleted"); d.Int() != 1 {
		t.Errorf("deleted = %d, want 1", d.Int())
	}
	if ok, _ := rec.Field("ok"); !ok.Bool() {
		t.Error("ok not true")
	}

	// Idempotent replay: key already gone → deleted 0, still ok.
	rec2 := runDelete(t, f, "victim")
	if d, _ := rec2.Field("deleted"); d.Int() != 0 {
		t.Errorf("replay deleted = %d, want 0", d.Int())
	}
	if ok, _ := rec2.Field("ok"); !ok.Bool() {
		t.Error("replay ok not true")
	}
}

func TestDeleteError(t *testing.T) {
	f := newFake()
	f.delErr = errors.New("down")
	s := &deleteSource{open: inject(f)}
	_ = s.Open(context.Background(), baseConfig(`,"key":"k"`))
	if _, err := s.Next(context.Background()); err == nil {
		t.Fatal("expected DEL error to propagate")
	}
}

// --- config validation ----------------------------------------------------

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"missing addr": `{"allow_local":true}`,
		"bad addr":     `{"addr":"no-port","allow_local":true}`,
		"negative db":  `{"addr":"h:6379","db":-1,"allow_local":true}`,
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(cfg), &c); err == nil {
				t.Fatalf("%s: expected error", name)
			}
		})
	}

	t.Run("bad json", func(t *testing.T) {
		var c config
		if err := parseConfig([]byte("{"), &c); err == nil {
			t.Fatal("expected json error")
		}
	})

	t.Run("delete requires key", func(t *testing.T) {
		s := &deleteSource{open: inject(newFake())}
		if err := s.Open(context.Background(), baseConfig("")); err == nil {
			t.Fatal("delete without key: expected error")
		}
	})
}

// --- guard ----------------------------------------------------------------

func TestGuard(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		allow   bool
		wantErr bool
	}{
		{"loopback denied", "127.0.0.1:6379", false, true},
		{"loopback allowed", "127.0.0.1:6379", true, false},
		{"private denied", "10.1.2.3:6379", false, true},
		{"private allowed", "10.1.2.3:6379", true, false},
		{"cgnat denied", "100.100.100.200:6379", false, true},
		{"link-local denied", "169.254.169.254:6379", false, true},
		{"public allowed", "8.8.8.8:6379", false, false},
		{"unspecified denied", "0.0.0.0:6379", false, true},
		{"unresolvable", "not-an-ip:6379", false, true},
		{"bad address", "no-colon", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := guard(tc.allow)("tcp", tc.addr, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("guard(%q, allow=%v) err = %v, wantErr=%v", tc.addr, tc.allow, err, tc.wantErr)
			}
		})
	}
}

// --- valueToString --------------------------------------------------------

func TestValueToString(t *testing.T) {
	b := record.NewBatch()
	bld := b.Builder()
	mk := func(build func()) record.Value {
		build()
		return bld.Finish()
	}
	if v, _ := valueToString(mk(func() { bld.StringLiteral("s") })); v != "s" {
		t.Errorf("string = %q", v)
	}
	if v, _ := valueToString(mk(func() { bld.Int(42) })); v != "42" {
		t.Errorf("int = %q", v)
	}
	if v, _ := valueToString(mk(func() { bld.Float(1.5) })); v != "1.5" {
		t.Errorf("float = %q", v)
	}
	if v, _ := valueToString(mk(func() { bld.Bool(true) })); v != "true" {
		t.Errorf("bool = %q", v)
	}
	if v, _ := valueToString(mk(func() { bld.Null() })); v != "" {
		t.Errorf("null = %q", v)
	}
	if _, err := valueToString(mk(func() { bld.BeginMap(); bld.EndMap() })); err == nil {
		t.Error("map: expected scalar error")
	}
}

// --- go-redis adapter (offline: the network guard refuses loopback before any
// packet is sent, so every adapter method exercises its call + error path with
// no real Redis server) ----------------------------------------------------

func TestClientAdapterOffline(t *testing.T) {
	c, err := openClient(&config{Addr: "127.0.0.1:6379", AllowLocal: false})
	if err != nil {
		t.Fatalf("openClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if _, _, err := c.Scan(ctx, 0, "*", 10); err == nil {
		t.Error("Scan: expected guard error")
	}
	if _, err := c.Type(ctx, "k"); err == nil {
		t.Error("Type: expected guard error")
	}
	if _, err := c.Get(ctx, "k"); err == nil {
		t.Error("Get: expected guard error")
	}
	if _, err := c.HGetAll(ctx, "k"); err == nil {
		t.Error("HGetAll: expected guard error")
	}
	if _, err := c.LRange(ctx, "k", 0, -1); err == nil {
		t.Error("LRange: expected guard error")
	}
	if err := c.Set(ctx, "k", "v", 0); err == nil {
		t.Error("Set: expected guard error")
	}
	if _, err := c.Del(ctx, "k"); err == nil {
		t.Error("Del: expected guard error")
	}
}

func TestOpenClientTLS(t *testing.T) {
	c, err := openClient(&config{Addr: "redis.example.com:6379", TLS: true, Username: "u", Password: "p", DB: 3})
	if err != nil {
		t.Fatalf("openClient tls: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
