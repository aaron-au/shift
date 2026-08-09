package schema

import (
	"fmt"
	"math"
	"strconv"

	"github.com/aaron-au/shift/engine/record"
)

// constVal is one compiled `const` or `enum` member.
//
// Only scalars and the boolean-false schema are representable. Object and
// array members are rejected at compile time rather than compared structurally:
// equality for them is defined recursively over unordered members, which is a
// meaningful amount of hot-path work for a keyword that is, in practice, always
// used on scalars. Rejecting is honest; comparing wrongly is not.
type constVal struct {
	kind constKind
	b    bool
	f    float64
	s    string
}

type constKind uint8

const (
	constNull constKind = iota
	constBool
	constNumber
	constString
	// constNever matches nothing. It is how the `false` schema is compiled.
	constNever
)

func toConst(v any, ptr string) (constVal, error) {
	switch t := v.(type) {
	case nil:
		return constVal{kind: constNull}, nil
	case bool:
		return constVal{kind: constBool, b: t}, nil
	case float64:
		return constVal{kind: constNumber, f: t}, nil
	case string:
		return constVal{kind: constString, s: t}, nil
	default:
		return constVal{}, fmt.Errorf("%w: %s: const/enum members must be scalars in this subset "+
			"(object and array members are rejected rather than compared approximately)", errCompile, at(ptr))
	}
}

func (c *constVal) equal(v record.Value) bool {
	switch c.kind {
	case constNever:
		return false
	case constNull:
		return v.Kind() == record.KindNull
	case constBool:
		return v.Kind() == record.KindBool && v.Bool() == c.b
	case constNumber:
		// 1 and 1.0 are the same number to JSON Schema, so compare as numbers
		// rather than by kind.
		switch v.Kind() {
		case record.KindInt:
			return float64(v.Int()) == c.f
		case record.KindFloat, record.KindDecimal:
			// The schema's own constant came from JSON and is a float64, so
			// this comparison is as exact as that float is — widening the
			// decimal is not what loses precision here, the schema literal is.
			return v.Float() == c.f
		default:
			return false
		}
	case constString:
		return v.Kind() == record.KindString && string(v.Bytes()) == c.s
	default:
		return false
	}
}

func (c *constVal) String() string {
	switch c.kind {
	case constNull:
		return "null"
	case constBool:
		return strconv.FormatBool(c.b)
	case constNumber:
		if c.f == math.Trunc(c.f) && math.Abs(c.f) < 1e15 {
			return strconv.FormatInt(int64(c.f), 10)
		}
		return strconv.FormatFloat(c.f, 'g', -1, 64)
	case constString:
		return strconv.Quote(c.s)
	case constNever:
		return "nothing"
	default:
		return "?"
	}
}
