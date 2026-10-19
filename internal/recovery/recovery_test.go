package recovery

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestHandle(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf).With().Str("bucket", "b1").Logger()

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = Handle("transfer", r, logger)
			}
		}()
		panic("boom")
	}()

	if err == nil || err.Error() != "panic in transfer: boom" {
		t.Fatalf("err = %v, want %q", err, "panic in transfer: boom")
	}
	out := buf.String()
	for _, want := range []string{`"site":"transfer"`, `"panic":"boom"`, `"bucket":"b1"`, `"stack":`, "TestHandle"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %s: %s", want, out)
		}
	}
}
