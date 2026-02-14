package profile

import (
	"bytes"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func TestProfileCommand_DoesNotExposeInitSubcommand(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewProfileCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"init"})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected unknown command error for profile init")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected error: %v", err)
	}
}
