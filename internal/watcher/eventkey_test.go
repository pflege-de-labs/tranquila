package watcher

import "testing"

func TestDecodeEventKey(t *testing.T) {
	tests := []struct {
		name, raw, want string
	}{
		{"plain key unchanged", "dir/file.txt", "dir/file.txt"},
		{"plus is a space", "a+b.txt", "a b.txt"},
		{"escaped plus is a plus", "c%2Bd.txt", "c+d.txt"},
		{"slash may arrive escaped", "dir%2Ffile.txt", "dir/file.txt"},
		{"multibyte utf-8", "%E2%82%AC.txt", "€.txt"},
		{"malformed escape passes through raw", "bad%zz.txt", "bad%zz.txt"},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeEventKey("b", tc.raw); got != tc.want {
				t.Errorf("decodeEventKey(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
