package config

import (
	"strings"
	"testing"
)

// Both computers derive the same key from the same password, and different
// passwords never collide.
func TestDeriveTokenIsStableAndPasswordSpecific(t *testing.T) {
	a := DeriveToken("shared-secret-123")
	b := DeriveToken("shared-secret-123")
	if a != b {
		t.Fatalf("same password produced %q and %q", a, b)
	}
	if !IsDerivedToken(a) {
		t.Fatalf("derived token %q is not marked", a)
	}
	if strings.Contains(a, "shared-secret-123") {
		t.Fatal("derived token leaks the password")
	}
	if DeriveToken("shared-secret-124") == a {
		t.Fatal("different passwords produced the same key")
	}
	if DeriveToken("") != "" {
		t.Fatal("an empty password should derive nothing")
	}
	if len(a) < len(DerivedTokenPrefix)+20 {
		t.Fatalf("derived token is too short: %q", a)
	}
}

func TestDeriveTokenFromInputKeepsRawTokens(t *testing.T) {
	raw := "st_abc123"
	if got := DeriveTokenFromInput(raw); got != raw {
		t.Fatalf("raw token changed to %q", got)
	}
	derived := DeriveToken("pw")
	if got := DeriveTokenFromInput(derived); got != derived {
		t.Fatalf("derived token re-derived to %q", got)
	}
	if got := DeriveTokenFromInput("my password"); !IsDerivedToken(got) {
		t.Fatalf("password input produced %q", got)
	}
}
