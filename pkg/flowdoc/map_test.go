package flowdoc

import (
	"strings"
	"testing"
)

func mapDoc(opJSON string) string {
	return `{"name":"m","source":{"connector":"http","action":"get"},"ops":[` + opJSON + `],"sink":{"connector":"http","action":"post"}}`
}

func TestMapValidation(t *testing.T) {
	ok := `{"type":"map","maps":[
      {"out":"id","from":"$.orderId"},
      {"out":"customer.name","from":"$.name"},
      {"out":"customer.tier","const":"gold"},
      {"out":"total","from":"$.amount","to":"float"},
      {"out":"label","concat":["order-","$.orderId"]},
      {"out":"region","from":"$.region","default":"unknown"}]}`
	if _, err := Parse([]byte(mapDoc(ok))); err != nil {
		t.Fatalf("valid map rejected: %v", err)
	}

	cases := []struct{ name, op, want string }{
		{"no fields", `{"type":"map","maps":[]}`, "map needs fields"},
		{"no out", `{"type":"map","maps":[{"from":"$.a"}]}`, "needs an out path"},
		{"two sources", `{"type":"map","maps":[{"out":"a","from":"$.a","const":"x"}]}`, "exactly one of from/const/concat"},
		{"no source", `{"type":"map","maps":[{"out":"a"}]}`, "exactly one of from/const/concat"},
		{"bad coerce", `{"type":"map","maps":[{"out":"a","from":"$.a","to":"date"}]}`, "unknown coerce kind"},
		{"collision", `{"type":"map","maps":[{"out":"a","const":"x"},{"out":"a.b","const":"y"}]}`, "collides"},
		{"bad path", `{"type":"map","maps":[{"out":"a","from":"$.["}]}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(mapDoc(tc.op)))
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestMapOutSegments(t *testing.T) {
	for in, want := range map[string]string{
		"a":         "a",
		"a.b.c":     "a|b|c",
		"$.a.b":     "a|b",
		"$customer": "customer",
	} {
		if got := strings.Join(MapOutSegments(in), "|"); got != want {
			t.Errorf("MapOutSegments(%q) = %q, want %q", in, got, want)
		}
	}
}
