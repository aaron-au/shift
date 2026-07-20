package connpolicy

import "testing"

func TestPolicy(t *testing.T) {
	cases := []struct {
		name        string
		allow, deny string
		checks      map[string]bool // connector name → expected Allowed
		restricted  bool
	}{
		{
			name:       "unrestricted default",
			checks:     map[string]bool{"http": true, "fs": true, "anything": true},
			restricted: false,
		},
		{
			name:       "denylist hides dangerous",
			deny:       "fs,disk,exec",
			checks:     map[string]bool{"http": true, "gen": true, "fs": false, "disk": false, "exec": false},
			restricted: true,
		},
		{
			name:       "allowlist permits only members",
			allow:      "http,gen",
			checks:     map[string]bool{"http": true, "gen": true, "fs": false, "anything": false},
			restricted: true,
		},
		{
			name:       "deny wins over allow",
			allow:      "http,fs",
			deny:       "fs",
			checks:     map[string]bool{"http": true, "fs": false},
			restricted: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Parse(c.allow, c.deny)
			if p.Restricted() != c.restricted {
				t.Errorf("Restricted() = %v, want %v", p.Restricted(), c.restricted)
			}
			for name, want := range c.checks {
				if got := p.Allowed(name); got != want {
					t.Errorf("Allowed(%q) = %v, want %v", name, got, want)
				}
			}
		})
	}
}

func TestNilPolicyAllowsAll(t *testing.T) {
	var p *Policy
	if !p.Allowed("anything") || p.Restricted() {
		t.Fatal("nil policy must allow everything and be unrestricted")
	}
}
