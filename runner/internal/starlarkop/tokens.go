package starlarkop

import (
	"fmt"

	"go.starlark.net/syntax"
)

// syntaxToken aliases the Starlark token type so the arithmetic and comparison
// code reads without a second package qualifier on every line.
type syntaxToken = syntax.Token

const (
	tokenPlus       = syntax.PLUS
	tokenMinus      = syntax.MINUS
	tokenStar       = syntax.STAR
	tokenSlash      = syntax.SLASH
	tokenSlashSlash = syntax.SLASHSLASH
	tokenPercent    = syntax.PERCENT
)

// compareResult turns a -1/0/1 ordering into the answer for one comparison
// operator.
func compareResult(op syntaxToken, c int) (bool, error) {
	switch op {
	case syntax.EQL:
		return c == 0, nil
	case syntax.NEQ:
		return c != 0, nil
	case syntax.LT:
		return c < 0, nil
	case syntax.LE:
		return c <= 0, nil
	case syntax.GT:
		return c > 0, nil
	case syntax.GE:
		return c >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison %s", op)
	}
}
