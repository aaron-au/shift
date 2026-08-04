package fsconn

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/sdk"
)

// writeLines writes n NDJSON records {"i":0..n-1} and returns the file path.
func writeLines(t *testing.T, root string, n int) string {
	t.Helper()
	var sb strings.Builder
	for i := range n {
		sb.WriteString(`{"i":`)
		sb.WriteString(itoa(i))
		sb.WriteString("}\n")
	}
	p := filepath.Join(root, "in.ndjson")
	if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// drain reads the source to EOF, returning every record's $.i value in order.
func drain(t *testing.T, s *getSource) []int64 {
	t.Helper()
	var got []int64
	for {
		b, err := s.Next(t.Context())
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, r := range b.Records() {
			v, _ := r.Field("i")
			got = append(got, v.Int())
		}
	}
}

func openGet(t *testing.T, root, path string) *getSource {
	t.Helper()
	s := &getSource{}
	if err := s.Open(t.Context(), cfgJSON(t, map[string]any{"root": root, "path": path, "format": "ndjson"})); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// The capability is opt-in and the interface is what the SDK server dispatches
// on, so a compile-time assertion is the assertion that matters.
func TestGetSourceIsResumable(t *testing.T) {
	var _ sdk.ResumableSource = (*getSource)(nil)
}

// The core property: a run interrupted after k records, resumed from the
// cursor taken at that point, delivers exactly the remainder — no gap, no
// duplicate.
func TestResumeDeliversExactlyTheRemainder(t *testing.T) {
	root := testRoot(t)
	// More than one default batch (1024 records), so the interruption lands
	// at a real batch boundary rather than at EOF.
	const total = 2600
	writeLines(t, root, total)

	// First attempt: read two batches, then "die", keeping the cursor.
	first := openGet(t, root, "in.ndjson")
	var delivered int64
	for range 2 {
		b, err := first.Next(t.Context())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		delivered += int64(b.Len())
	}
	cur := first.Checkpoint()
	if len(cur) == 0 {
		t.Fatal("no checkpoint after two batches")
	}

	// Second attempt: resume.
	second := openGet(t, root, "in.ndjson")
	if err := second.Resume(t.Context(), cur); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got := drain(t, second)

	if int64(len(got))+delivered != total {
		t.Fatalf("delivered %d then %d = %d, want %d records total (a gap or a duplicate)",
			delivered, len(got), int64(len(got))+delivered, total)
	}
	if got[0] != delivered {
		t.Fatalf("resumed at record %d, want %d — the first record after the cursor", got[0], delivered)
	}
}

// A skip must be able to land INSIDE a batch: batch boundaries are not stable
// across attempts, so a cursor that only worked on a boundary would be a
// latent off-by-a-batch bug.
func TestResumeSkipsIntoTheMiddleOfABatch(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 20)

	s := openGet(t, root, "in.ndjson")
	cur, err := json.Marshal(fsCursor{
		V: fsCursorVersion, Path: "in.ndjson", N: 7,
		Size:  fileSize(t, filepath.Join(root, "in.ndjson")),
		MTime: fileMTime(t, filepath.Join(root, "in.ndjson")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(t.Context(), cur); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got := drain(t, s)
	if len(got) != 13 || got[0] != 7 {
		t.Fatalf("got %d records starting at %d, want 13 starting at 7", len(got), got[0])
	}
}

// An empty cursor must behave exactly like not resuming — the runner passes
// through whatever the hub returned without testing it first.
func TestEmptyCursorReadsFromTheStart(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 5)

	s := openGet(t, root, "in.ndjson")
	if err := s.Resume(t.Context(), nil); err != nil {
		t.Fatalf("resume(nil): %v", err)
	}
	if got := drain(t, s); len(got) != 5 || got[0] != 0 {
		t.Fatalf("got %d records starting at %v, want all 5 from the start", len(got), got)
	}
}

// A record ordinal against a file that changed resumes at the WRONG place, and
// nothing downstream could detect it. Refusing is the only safe answer; the
// runner then falls back to a full replay.
func TestResumeRefusesWhenTheFileChanged(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 10)
	p := filepath.Join(root, "in.ndjson")

	s := openGet(t, root, "in.ndjson")
	if _, err := s.Next(t.Context()); err != nil {
		t.Fatal(err)
	}
	cur := s.Checkpoint()

	// Rewrite with different content (and a different size).
	if err := os.WriteFile(p, []byte(`{"i":99}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Some filesystems have coarse mtime; the size change alone must be enough.
	time.Sleep(10 * time.Millisecond)

	again := openGet(t, root, "in.ndjson")
	err := again.Resume(t.Context(), cur)
	if err == nil {
		t.Fatal("resumed against a changed file — this silently skips records")
	}
	if !strings.Contains(err.Error(), "changed") {
		t.Fatalf("err = %v, want it to name the change", err)
	}
}

// The node's config can be edited between attempts. A count taken against one
// file must not be applied to another.
func TestResumeRefusesACursorForAnotherFile(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 5)
	if err := os.WriteFile(filepath.Join(root, "other.ndjson"), []byte(`{"i":0}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := openGet(t, root, "in.ndjson")
	if _, err := s.Next(t.Context()); err != nil {
		t.Fatal(err)
	}
	cur := s.Checkpoint()

	other := openGet(t, root, "other.ndjson")
	if err := other.Resume(t.Context(), cur); err == nil {
		t.Fatal("a cursor for in.ndjson was accepted for other.ndjson")
	}
}

func TestResumeRejectsMalformedAndUnknownVersionCursors(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 3)

	for name, cur := range map[string][]byte{
		"malformed":       []byte(`{not json`),
		"unknown version": []byte(`{"v":99,"path":"in.ndjson","n":1}`),
		"negative count":  []byte(`{"v":1,"path":"in.ndjson","n":-1}`),
	} {
		s := openGet(t, root, "in.ndjson")
		if err := s.Resume(t.Context(), cur); err == nil {
			t.Errorf("%s cursor accepted, want rejection", name)
		}
	}
}

// Checkpoint must not go backwards or skip: it counts records handed
// downstream, which is what the runner pairs with its sink-confirmed count.
func TestCheckpointTracksEmittedRecords(t *testing.T) {
	root := testRoot(t)
	writeLines(t, root, 12)

	s := openGet(t, root, "in.ndjson")
	var emitted int64
	for {
		b, err := s.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		emitted += int64(b.Len())

		var c fsCursor
		if err := json.Unmarshal(s.Checkpoint(), &c); err != nil {
			t.Fatalf("checkpoint is not a valid cursor: %v", err)
		}
		if c.N != emitted {
			t.Fatalf("checkpoint N = %d, want %d (records emitted so far)", c.N, emitted)
		}
	}
	if emitted != 12 {
		t.Fatalf("emitted %d, want 12", emitted)
	}
}

func fileSize(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func fileMTime(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.ModTime().UnixNano()
}
