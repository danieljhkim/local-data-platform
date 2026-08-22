package util

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

var (
	isTerminalFn      = term.IsTerminal
	readTTYPasswordFn = term.ReadPassword
)

// IsSecretInputTerminal reports whether in is a terminal that can disable echo.
func IsSecretInputTerminal(in io.Reader) bool {
	f, ok := in.(*os.File)
	if !ok {
		return false
	}
	return isTerminalFn(int(f.Fd()))
}

// ReadSecretFile reads a password from a file, trimming a single trailing newline.
func ReadSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read password file %s: %w", path, err)
	}
	return strings.TrimRight(string(data), "\r\n"), nil
}

// ReadSecret reads a secret from in. When in is a terminal, echo is disabled.
func ReadSecret(in io.Reader, promptOut io.Writer, prompt string) (string, error) {
	if IsSecretInputTerminal(in) {
		if prompt != "" && promptOut != nil {
			if _, err := fmt.Fprint(promptOut, prompt); err != nil {
				return "", err
			}
		}
		return readTTYSecret(in.(*os.File), promptOut)
	}
	if prompt != "" && promptOut != nil && in == nil {
		if _, err := fmt.Fprint(promptOut, prompt); err != nil {
			return "", err
		}
	}
	return readBufferedSecretLine(bufio.NewReader(in))
}

// ReadSecretLine reads a secret, using a shared buffered reader for non-TTY input
// so callers that already consumed prior lines keep a consistent stream.
func ReadSecretLine(reader *bufio.Reader, in io.Reader, newlineOut io.Writer) (string, error) {
	if reader != nil && reader.Buffered() > 0 {
		return readBufferedSecretLine(reader)
	}
	if IsSecretInputTerminal(in) {
		f, ok := in.(*os.File)
		if !ok {
			return "", fmt.Errorf("failed to read password: stdin is not a file")
		}
		return readTTYSecret(f, newlineOut)
	}
	if reader == nil {
		reader = bufio.NewReader(in)
	}
	return readBufferedSecretLine(reader)
}

func readTTYSecret(f *os.File, newlineOut io.Writer) (string, error) {
	b, err := readTTYPasswordFn(int(f.Fd()))
	if newlineOut != nil {
		_, _ = fmt.Fprintln(newlineOut)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("failed to read password: %w", err)
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

func readBufferedSecretLine(reader *bufio.Reader) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("failed to read password: no input")
	}
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if line == "" && errors.Is(err, io.EOF) {
		return "", nil
	}
	return strings.TrimRight(line, "\r\n"), nil
}
