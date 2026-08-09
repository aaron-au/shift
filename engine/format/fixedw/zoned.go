package fixedw

import (
	"errors"
	"fmt"
)

// Zoned (overpunch) decimals: a signed number whose LAST byte encodes both the
// final digit and the sign, so the field costs no extra column for the sign.
//
// The encoding is inherited from punched cards via EBCDIC, and survives in
// COBOL signed DISPLAY fields, which is why a modern ASCII extract from a
// mainframe still contains "0001010{". Nothing about the file says a column is
// zoned, so the layout must declare it — a zoned field read as plain digits
// gives the right magnitude with a corrupted last digit and no sign, which is
// the worst possible failure: plausible, and wrong.
//
//	positive: { A B C D E F G H I   →  0 1 2 3 4 5 6 7 8 9
//	negative: } J K L M N O P Q R   →  0 1 2 3 4 5 6 7 8 9
//
// A plain digit in the final position is also accepted on read and taken as
// positive: some systems write unsigned values into a signed field.
var (
	overpunchPositive = [10]byte{'{', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'}
	overpunchNegative = [10]byte{'}', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'}
)

// decodeOverpunch maps a final byte to its digit and sign.
func decodeOverpunch(b byte) (digit byte, negative bool, ok bool) {
	switch {
	case b >= '0' && b <= '9':
		return b, false, true
	case b == '{':
		return '0', false, true
	case b == '}':
		return '0', true, true
	case b >= 'A' && b <= 'I':
		return '0' + (b - 'A' + 1), false, true
	case b >= 'J' && b <= 'R':
		return '0' + (b - 'J' + 1), true, true
	// Lower case appears in files written by systems that down-cased on
	// export; the mapping is unambiguous, so accept it rather than fail.
	case b >= 'a' && b <= 'i':
		return '0' + (b - 'a' + 1), false, true
	case b >= 'j' && b <= 'r':
		return '0' + (b - 'j' + 1), true, true
	default:
		return 0, false, false
	}
}

// encodeOverpunch maps a digit and sign to the final byte.
func encodeOverpunch(digit byte, negative bool) (byte, error) {
	if digit < '0' || digit > '9' {
		return 0, fmt.Errorf("fixedw: %q is not a digit", digit)
	}
	if negative {
		return overpunchNegative[digit-'0'], nil
	}
	return overpunchPositive[digit-'0'], nil
}

// zonedDigits rewrites a zoned cell into plain digits plus a sign, into dst.
// The returned slice is dst's backing array, so callers reuse one buffer.
func zonedDigits(dst, cell []byte) (digits []byte, negative bool, err error) {
	if len(cell) == 0 {
		return nil, false, errors.New("fixedw: empty zoned value")
	}
	digit, neg, ok := decodeOverpunch(cell[len(cell)-1])
	if !ok {
		return nil, false, fmt.Errorf("fixedw: %q is not a valid zoned sign character", cell[len(cell)-1])
	}
	dst = dst[:0]
	for _, b := range cell[:len(cell)-1] {
		if b < '0' || b > '9' {
			return nil, false, fmt.Errorf("fixedw: %q is not a digit in a zoned value", b)
		}
		dst = append(dst, b)
	}
	return append(dst, digit), neg, nil
}
