package schema

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// formatKind is one supported `format` value.
//
// JSON Schema makes `format` an ANNOTATION by default: a conformant validator
// may accept "not-a-date" for {"format": "date"}. That default is useless for
// input verification — the whole reason an author writes it is to reject bad
// input — so here it ASSERTS. The set is closed and small for the same reason
// the keyword set is: a format that is accepted but not checked is a promise
// that silently is not kept.
type formatKind uint8

const (
	formatNone formatKind = iota
	formatDateTime
	formatDate
	formatTime
	formatEmail
	formatUUID
)

func (f formatKind) String() string {
	switch f {
	case formatDateTime:
		return "RFC 3339 date-time"
	case formatDate:
		return "date (YYYY-MM-DD)"
	case formatTime:
		return "time (HH:MM:SS)"
	case formatEmail:
		return "email address"
	case formatUUID:
		return "UUID"
	default:
		return "value"
	}
}

func compileFormat(v any, ptr string) (formatKind, error) {
	s, ok := v.(string)
	if !ok {
		return formatNone, fmt.Errorf("%w: %s: format must be a string", errCompile, at(ptr))
	}
	switch s {
	case "date-time":
		return formatDateTime, nil
	case "date":
		return formatDate, nil
	case "time":
		return formatTime, nil
	case "email":
		return formatEmail, nil
	case "uuid":
		return formatUUID, nil
	default:
		return formatNone, fmt.Errorf("%w: %s: unsupported format %q — this subset ASSERTS formats rather than "+
			"annotating them, so it only accepts the ones it can check", errCompile, at(ptr), s)
	}
}

func (f formatKind) valid(b []byte) bool {
	switch f {
	case formatDateTime:
		return validDateTime(b)
	case formatDate:
		return validDate(b)
	case formatTime:
		return validTime(b)
	case formatEmail:
		return validEmail(b)
	case formatUUID:
		return validUUID(b)
	default:
		return true
	}
}

// validDate checks a full-date: YYYY-MM-DD, with real month and day ranges.
// Hand-written rather than time.Parse because time.Parse accepts things the
// grammar does not (and allocates).
func validDate(b []byte) bool {
	if len(b) != 10 || b[4] != '-' || b[7] != '-' {
		return false
	}
	if !allDigits(b[0:4]) || !allDigits(b[5:7]) || !allDigits(b[8:10]) {
		return false
	}
	year := atoi(b[0:4])
	month := atoi(b[5:7])
	day := atoi(b[8:10])
	if month < 1 || month > 12 || day < 1 {
		return false
	}
	return day <= daysIn(year, month)
}

// validTime checks a full-time: HH:MM:SS, optional fractional seconds, and an
// offset (Z or ±HH:MM) which RFC 3339 requires.
func validTime(b []byte) bool {
	if len(b) < 9 || b[2] != ':' || b[5] != ':' {
		return false
	}
	if !allDigits(b[0:2]) || !allDigits(b[3:5]) || !allDigits(b[6:8]) {
		return false
	}
	h, m, s := atoi(b[0:2]), atoi(b[3:5]), atoi(b[6:8])
	if h > 23 || m > 59 || s > 60 {
		return false
	}
	rest := b[8:]
	if len(rest) > 0 && rest[0] == '.' {
		i := 1
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 1 {
			return false // a dot with no digits
		}
		rest = rest[i:]
	}
	offset, ok := parseOffset(rest)
	if !ok {
		return false
	}
	if s == 60 {
		// A leap second is inserted at 23:59:60 UTC and nowhere else, so
		// 22:59:60Z is not a time that has ever existed. Accepting second 60 at
		// any hour — which is the obvious implementation, and was this one —
		// lets a whole class of malformed timestamps through.
		utc := (h*60 + m) - offset
		utc = ((utc % 1440) + 1440) % 1440
		if utc != 23*60+59 {
			return false
		}
	}
	return true
}

// parseOffset returns the UTC offset in minutes for a trailing "Z" or "±HH:MM".
func parseOffset(rest []byte) (int, bool) {
	switch {
	case len(rest) == 1 && (rest[0] == 'Z' || rest[0] == 'z'):
		return 0, true
	case len(rest) == 6 && (rest[0] == '+' || rest[0] == '-'):
		if rest[3] != ':' || !allDigits(rest[1:3]) || !allDigits(rest[4:6]) {
			return 0, false
		}
		hh, mm := atoi(rest[1:3]), atoi(rest[4:6])
		if hh > 23 || mm > 59 {
			return 0, false
		}
		off := hh*60 + mm
		if rest[0] == '-' {
			off = -off
		}
		return off, true
	default:
		return 0, false
	}
}

func validDateTime(b []byte) bool {
	i := strings.IndexAny(string(b), "Tt ")
	if i != 10 {
		return false
	}
	return validDate(b[:10]) && validTime(b[11:])
}

// validEmail follows RFC 5321's mailbox grammar, which is the one the
// conformance suite tests and the one mail systems implement.
//
// The first version of this was "structural": one @, no spaces, a dot in the
// domain. It disagreed with the specification in BOTH directions — rejecting
// quoted local parts and address literals that are legal, while accepting
// leading, trailing and doubled dots that are not. Either direction is a bug,
// and the second kind is the worse one: a validator that passes bad input is
// the failure this whole package exists to avoid.
func validEmail(b []byte) bool {
	s := string(b)
	local, domain, ok := splitMailbox(s)
	if !ok || local == "" || domain == "" || len(local) > 64 || len(domain) > 255 {
		return false
	}
	if !validLocalPart(local) {
		return false
	}
	return validDomainPart(domain)
}

// splitMailbox splits at the LAST @ that is not inside a quoted local part,
// because a quoted local part may legally contain one ("joe@bloggs"@example.com).
func splitMailbox(s string) (local, domain string, ok bool) {
	if strings.HasPrefix(s, `"`) {
		// Scan the quoted string, honouring backslash escapes, then require an
		// @ immediately after the closing quote.
		for i := 1; i < len(s); i++ {
			switch s[i] {
			case '\\':
				i++ // skip the escaped character
			case '"':
				if i+1 >= len(s) || s[i+1] != '@' {
					return "", "", false
				}
				return s[:i+1], s[i+2:], true
			}
		}
		return "", "", false
	}
	at := strings.IndexByte(s, '@')
	if at < 0 || at != strings.LastIndexByte(s, '@') {
		return "", "", false
	}
	return s[:at], s[at+1:], true
}

func validLocalPart(local string) bool {
	if strings.HasPrefix(local, `"`) {
		if len(local) < 2 || !strings.HasSuffix(local, `"`) {
			return false
		}
		inner := local[1 : len(local)-1]
		for i := 0; i < len(inner); i++ {
			c := inner[i]
			switch {
			case c == '\\':
				i++
				if i >= len(inner) {
					return false
				}
			case c == '"':
				return false // an unescaped quote closes the string early
			case c < 0x20 || c == 0x7F:
				return false
			}
		}
		return true
	}
	// dot-string: atext atoms separated by SINGLE dots, with no dot at either
	// end. ".test@x.com", "test.@x.com" and "te..st@x.com" are all invalid.
	if strings.HasPrefix(local, ".") || strings.HasSuffix(local, ".") || strings.Contains(local, "..") {
		return false
	}
	for i := range len(local) {
		if local[i] != '.' && !isAtext(local[i]) {
			return false
		}
	}
	return true
}

func validDomainPart(domain string) bool {
	// An address literal: [192.0.2.1] or [IPv6:::1]. The address inside is
	// PARSED, not merely bracketed — "[127.0.0.300]" looks like an address and
	// is not one, and accepting it is the kind of near-miss that makes a
	// validator worse than useless.
	if strings.HasPrefix(domain, "[") {
		if !strings.HasSuffix(domain, "]") || len(domain) <= 2 {
			return false
		}
		inner := domain[1 : len(domain)-1]
		if rest, ok := strings.CutPrefix(inner, "IPv6:"); ok {
			addr, err := netip.ParseAddr(rest)
			return err == nil && addr.Is6()
		}
		addr, err := netip.ParseAddr(inner)
		return err == nil && addr.Is4()
	}
	if strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") || strings.Contains(domain, "..") {
		return false
	}
	if !strings.Contains(domain, ".") {
		return false
	}
	for label := range strings.SplitSeq(domain, ".") {
		if label == "" || len(label) > 63 ||
			strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
		for i := range len(label) {
			c := label[i]
			if !isAlnum(c) && c != '-' {
				return false
			}
		}
	}
	return true
}

// isAtext reports whether c is RFC 5322 atext: the characters allowed in an
// unquoted local part.
func isAtext(c byte) bool {
	if isAlnum(c) {
		return true
	}
	return strings.IndexByte("!#$%&'*+-/=?^_`{|}~", c) >= 0
}

func isAlnum(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// validUUID checks the 8-4-4-4-12 hex form. Version and variant bits are NOT
// checked: callers legitimately send nil UUIDs and non-standard versions, and
// rejecting those would be stricter than anyone means by "uuid".
func validUUID(b []byte) bool {
	if len(b) != 36 {
		return false
	}
	for i, c := range b {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHex(c) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func allDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(b) > 0
}

func atoi(b []byte) int {
	n, _ := strconv.Atoi(string(b))
	return n
}

func daysIn(year, month int) int {
	switch month {
	case 4, 6, 9, 11:
		return 30
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	default:
		return 31
	}
}
