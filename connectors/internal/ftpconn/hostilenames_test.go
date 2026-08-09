package ftpconn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jlaffaye/ftp"
)

// legitimateNames are names a real remote system genuinely produces. Every
// hardening check below must let all of them through: rejecting these would
// break ordinary customers, which is a worse outcome than the attack being
// defended against.
var legitimateNames = []string{
	"invoices 2026-08.csv",             // spaces
	"rapport-financier-ao\u00fbt.csv",  // non-ASCII, NFC (precomposed û)
	"rapport-financier-aou\u0302t.csv", // the same name in NFD (u + combining circumflex)
	"发票.ndjson",                        // non-Latin script
	"файл.csv",                         // Cyrillic homoglyphs of Latin letters
	"invoice\u202Efdp.csv",             // right-to-left override: displays reversed
	"-rf",                              // looks like a flag, is a filename
	".hidden",                          // leading dot
	"archive.tar.gz.",                  // trailing dot
	"trailing space ",                  // trailing space
	"...",                              // three dots is a legal, ordinary name
	"..hidden",                         // starts with two dots but is not ".."
	"a..b",                             // dots in the middle
	strings.Repeat("x", 255),           // the longest name most filesystems allow
}

// hostileNames are names that must be refused: each one either escapes the
// listed directory once path.Join cleans it, or cannot be represented in a log
// or fed back into a line-oriented protocol.
var hostileNames = []string{
	"..",                                     // exactly the parent
	".",                                      // exactly the directory itself
	"",                                       // no name at all
	"../../etc/passwd",                       // traversal at the start
	"data/../../etc/passwd",                  // traversal in the middle
	"data/..",                                // traversal at the end
	"/etc/passwd",                            // absolute
	"sub/dir/file.csv",                       // a separator at all, even without traversal
	"file\x00.csv",                           // NUL truncates the name in a C server
	"file\n.csv",                             // newline: an FTP command injection when fed back
	"file\r.csv",                             // carriage return, likewise
	"file\x1b[2Klog.csv",                     // ANSI escape: rewrites the operator's terminal
	"\x7f",                                   // DEL
	strings.Repeat("../", 80) + "etc/passwd", // long traversal
}

// recordingFTPServer speaks just enough of RFC 959 for the client to log in and
// issue commands, recording every raw line it receives. Recording the WIRE is
// the point: FTP command injection is invisible at the Go API, where the whole
// hostile path is a single string argument, and only shows up as an extra line
// on the control channel.
func recordingFTPServer(t *testing.T) (host string, port int, lines func() []string) {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var mu sync.Mutex
	var got []string
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go serveRecording(conn, &mu, &got)
		}
	}()
	a := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", a.Port, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

func serveRecording(conn net.Conn, mu *sync.Mutex, got *[]string) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprint(conn, "220 ready\r\n")
	br := bufio.NewReader(conn)
	for {
		line, rerr := br.ReadString('\n')
		if rerr != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		mu.Lock()
		*got = append(*got, line)
		mu.Unlock()
		switch {
		case strings.HasPrefix(line, "USER"):
			_, _ = fmt.Fprint(conn, "331 need pass\r\n")
		case strings.HasPrefix(line, "PASS"):
			_, _ = fmt.Fprint(conn, "230 logged in\r\n")
		case strings.HasPrefix(line, "FEAT"):
			_, _ = fmt.Fprint(conn, "211-Features:\r\n211 End\r\n")
		case strings.HasPrefix(line, "QUIT"):
			_, _ = fmt.Fprint(conn, "221 bye\r\n")
			return
		case strings.HasPrefix(line, "DELE"), strings.HasPrefix(line, "RMD"), strings.HasPrefix(line, "RNTO"):
			_, _ = fmt.Fprint(conn, "250 ok\r\n")
		case strings.HasPrefix(line, "RNFR"):
			_, _ = fmt.Fprint(conn, "350 pending\r\n")
		default:
			_, _ = fmt.Fprint(conn, "200 ok\r\n")
		}
	}
}

// TestACarriageReturnInAPathCannotInjectASecondFTPCommand is the regression
// test for a genuine command injection (CWE-93).
//
// jlaffaye/ftp builds "DELE %s" and net/textproto terminates it with CRLF;
// neither validates the argument. A configured path of
// "a.txt\r\nDELE /etc/passwd" therefore put TWO DELE commands on the control
// channel — the second running with the connection's credentials, outside the
// single verb and single path the flow author was given. The same hole would
// let an injected PORT/EPRT make the SERVER dial an address of the attacker's
// choosing, which the connector's egress guard cannot see.
//
// The assertion is made on the wire, not on the returned error, because the
// wire is where the extra command exists.
func TestACarriageReturnInAPathCannotInjectASecondFTPCommand(t *testing.T) {
	host, port, lines := recordingFTPServer(t)
	cfg := fmt.Appendf(nil,
		`{"host":%q,"port":%d,"user":"u","explicit_tls":false,"allow_local":true,"path":%q}`,
		host, port, "a.txt\r\nDELE /etc/passwd")

	src := &opSource{op: opDelete}
	err := src.Open(context.Background(), cfg)
	if err == nil {
		_, err = src.Next(context.Background())
	}
	// The wire is checked first and unconditionally: it is where the damage is.
	for _, l := range lines() {
		if strings.Contains(l, "/etc/passwd") {
			t.Errorf("injected command reached the server: %q (full transcript: %q)", l, lines())
		}
	}
	if err == nil {
		t.Fatal("a path containing CRLF was accepted; it names no file, it appends a command")
	}
	if !strings.Contains(err.Error(), "control character") {
		t.Errorf("error should tell the operator what is wrong with the path, got: %v", err)
	}
}

// TestEveryVerbRefusesAControlCharacterInItsConfiguredPath: the guard has to sit
// on every path that reaches the wire, not just the one that was reported.
// rename is the easy one to miss — it has two.
func TestEveryVerbRefusesAControlCharacterInItsConfiguredPath(t *testing.T) {
	const evil = "ok.ndjson\r\nDELE /etc/passwd"
	base := `"host":"h","user":"u","explicit_tls":false,"allow_local":true`

	cases := []struct {
		verb string
		cfg  string
		open func(cfg []byte) error
	}{
		{"get", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&getSource{dial: refuseDial}).Open(context.Background(), c)
		}},
		{"put", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&putSink{dial: refuseDial}).Open(context.Background(), c)
		}},
		{"list", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&listSource{dial: refuseDial}).Open(context.Background(), c)
		}},
		{"delete", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&opSource{op: opDelete, dial: refuseDial}).Open(context.Background(), c)
		}},
		{"mkdir", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&opSource{op: opMkdir, dial: refuseDial}).Open(context.Background(), c)
		}},
		{"rmdir", fmt.Sprintf(`{%s,"path":%q}`, base, evil), func(c []byte) error {
			return (&opSource{op: opRmdir, dial: refuseDial}).Open(context.Background(), c)
		}},
		{"rename/from", fmt.Sprintf(`{%s,"from":%q,"to":"b.ndjson"}`, base, evil), func(c []byte) error {
			return (&opSource{op: opRename, dial: refuseDial}).Open(context.Background(), c)
		}},
		{"rename/to", fmt.Sprintf(`{%s,"from":"a.ndjson","to":%q}`, base, evil), func(c []byte) error {
			return (&opSource{op: opRename, dial: refuseDial}).Open(context.Background(), c)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.verb, func(t *testing.T) {
			err := tc.open([]byte(tc.cfg))
			if err == nil {
				t.Fatalf("%s accepted a path containing CRLF", tc.verb)
			}
			if !strings.Contains(err.Error(), "control character") {
				t.Errorf("%s failed for the wrong reason: %v", tc.verb, err)
			}
		})
	}
}

// refuseDial fails any attempt to connect, so a test that expects validation to
// happen BEFORE the network proves exactly that: if the guard were missing, the
// test would see a dial error instead of a validation error.
func refuseDial(context.Context, *config) (ftpConn, func() error, error) {
	return nil, nil, errors.New("ftp: test dial refused — validation should have rejected the config first")
}

// TestOrdinaryRemotePathsAreStillAccepted: the CRLF guard must not become a
// general-purpose filename policy. These are paths customers really configure.
func TestOrdinaryRemotePathsAreStillAccepted(t *testing.T) {
	for _, p := range []string{
		"/incoming/invoices 2026-08.csv",
		"/données/rapport-août.ndjson",
		"relative/path/file.csv",
		"/very/" + strings.Repeat("deep/", 40) + "file.csv",
		"/trailing/dots...",
		"/x/发票.ndjson",
		"C:\\windows\\style.csv", // a legal name on a remote Windows FTP server
	} {
		if err := validateRemotePath("path", p); err != nil {
			t.Errorf("legitimate path %q rejected: %v", p, err)
		}
	}
}

// TestAListingEntryThatEscapesTheListedDirectoryIsRefused is the regression test
// for the zip-slip shape.
//
// FTP entry names are parsed out of free-form LIST output, so a partner who can
// choose a filename — or a hostile server that simply says so — can put "/" and
// ".." into one. The emitted `path` field exists to be fed into a following
// get/delete node, so path.Join silently cleaning that name means the next node
// operates outside the directory the author listed, with this connector's
// credentials.
func TestAListingEntryThatEscapesTheListedDirectoryIsRefused(t *testing.T) {
	for _, name := range hostileNames {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			fs := newFakeFS()
			fs.entries = []*ftp.Entry{
				{Name: "harmless.csv", Type: ftp.EntryTypeFile},
				{Name: name, Type: ftp.EntryTypeFile},
			}
			src := &listSource{dial: fs.dialer()}
			cfg := []byte(`{"host":"h","user":"u","explicit_tls":false,"allow_local":true,"path":"/incoming"}`)
			err := src.Open(context.Background(), cfg)
			if err == nil {
				// Show what would have been emitted — that is the damage.
				t.Fatalf("listing accepted entry %q; emitted path would be %q", name, joined("/incoming", name))
			}
			if !strings.Contains(err.Error(), "refusing directory entry") &&
				!strings.Contains(err.Error(), "empty name") {
				t.Errorf("listing failed for the wrong reason: %v", err)
			}
		})
	}
}

// joined mirrors what list.go would emit, for the failure message only.
func joined(dir, name string) string {
	if name == "" {
		return dir
	}
	return dir + "/" + name
}

// TestAListingOfOrdinaryNamesStillSucceeds: the entry-name guard is a narrow
// containment check, not a filename policy. Spaces, every unicode form, RTL
// overrides, homoglyphs, leading/trailing dots and 255-byte names are all
// legitimate on a real server and must survive — including the ones that
// display misleadingly, which are an audit-trail hazard rather than a traversal.
func TestAListingOfOrdinaryNamesStillSucceeds(t *testing.T) {
	fs := newFakeFS()
	for _, n := range legitimateNames {
		fs.entries = append(fs.entries, &ftp.Entry{Name: n, Type: ftp.EntryTypeFile})
	}
	src := &listSource{dial: fs.dialer()}
	cfg := []byte(`{"host":"h","user":"u","explicit_tls":false,"allow_local":true,"path":"/incoming"}`)
	if err := src.Open(context.Background(), cfg); err != nil {
		t.Fatalf("a listing of ordinary names was refused: %v", err)
	}
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != len(legitimateNames) {
		t.Fatalf("emitted %d records for %d legitimate names", b.Len(), len(legitimateNames))
	}
	// The name must reach the record byte-for-byte: normalising it here would
	// make the emitted path fail to match the file on the server.
	for i, want := range legitimateNames {
		got, _ := b.Records()[i].Field("name")
		if got.String() != want {
			t.Errorf("name %d = %q, want %q — the emitted name must be byte-identical to the server's", i, got.String(), want)
		}
	}
}
