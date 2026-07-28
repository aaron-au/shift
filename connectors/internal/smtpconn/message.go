package smtpconn

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"mime"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// placeholderRe matches a $identifier template placeholder. A bare "$" not
// followed by an identifier is left untouched.
var placeholderRe = regexp.MustCompile(`\$([A-Za-z_][A-Za-z0-9_]*)`)

// substitute replaces every $field placeholder in tmpl with the record's
// corresponding field value. Unknown placeholders are left verbatim.
func substitute(tmpl string, rec record.Value) string {
	if !strings.ContainsRune(tmpl, '$') {
		return tmpl
	}
	return placeholderRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		if v, ok := rec.Field(m[1:]); ok {
			return valueString(v)
		}
		return m
	})
}

// valueString renders a scalar record Value as text for substitution/body use.
// Containers are not inlined (their kind name is returned) — a body template
// should reference scalar fields.
func valueString(v record.Value) string {
	switch v.Kind() {
	case record.KindString, record.KindBytes:
		return v.String()
	case record.KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case record.KindFloat:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case record.KindBool:
		return strconv.FormatBool(v.Bool())
	case record.KindNull:
		return ""
	default:
		return v.Kind().String()
	}
}

// renderBody renders a whole record as a plain-text body: one "key: value" line
// per top-level field. A non-map record renders as its scalar value.
func renderBody(rec record.Value) string {
	if rec.Kind() != record.KindMap {
		return valueString(rec)
	}
	var b strings.Builder
	for i := 0; i < rec.Len(); i++ {
		b.WriteString(string(rec.KeyAt(i)))
		b.WriteString(": ")
		b.WriteString(valueString(rec.Index(i)))
		b.WriteByte('\n')
	}
	return b.String()
}

// sanitizeHeader strips CR and LF from a header value so a record-supplied
// field can never inject extra headers (SMTP header-injection defense).
func sanitizeHeader(s string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(s)
}

// bareAddr extracts the addr-spec (local@domain) from an address that may carry
// a display name; on parse failure the sanitized input is returned so the
// envelope still gets something.
func bareAddr(s string) string {
	if a, err := mail.ParseAddress(s); err == nil {
		return a.Address
	}
	return sanitizeHeader(strings.TrimSpace(s))
}

// toCRLF normalizes body line endings to CRLF as required on the SMTP wire
// (net/smtp's DATA writer handles dot-stuffing but not LF→CRLF conversion).
func toCRLF(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\n", "\r\n")
}

// messageID builds a random RFC 5322 Message-ID using the sender's domain.
func messageID(from string) string {
	var buf [16]byte
	// crypto/rand.Read never returns an error on the platforms we target; a
	// short read would still yield a usable (if less random) id.
	_, _ = rand.Read(buf[:])
	domain := "localhost"
	if at := strings.LastIndexByte(bareAddr(from), '@'); at >= 0 {
		domain = bareAddr(from)[at+1:]
	}
	return "<" + hex.EncodeToString(buf[:]) + "@" + domain + ">"
}

// buildMessage assembles an RFC 5322 message: sanitized address/subject
// headers, a generated Date and Message-ID, and a UTF-8 text/plain body with
// CRLF line endings. Subject is RFC 2047 encoded when it contains non-ASCII.
func buildMessage(from string, to, cc []string, subject, body string, now time.Time) []byte {
	var b bytes.Buffer
	writeHeader(&b, "From", sanitizeHeader(from))
	writeHeader(&b, "To", sanitizeHeader(strings.Join(to, ", ")))
	if len(cc) > 0 {
		writeHeader(&b, "Cc", sanitizeHeader(strings.Join(cc, ", ")))
	}
	writeHeader(&b, "Subject", mime.QEncoding.Encode("utf-8", sanitizeHeader(subject)))
	writeHeader(&b, "Date", now.Format(time.RFC1123Z))
	writeHeader(&b, "Message-ID", messageID(from))
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("\r\n")
	b.WriteString(toCRLF(body))
	return b.Bytes()
}

func writeHeader(b *bytes.Buffer, name, value string) {
	b.WriteString(name)
	b.WriteString(": ")
	b.WriteString(value)
	b.WriteString("\r\n")
}
