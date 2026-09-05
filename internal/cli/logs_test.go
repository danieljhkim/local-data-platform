package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func writeLogFixture(t *testing.T, paths *config.Paths, service, name, content string) string {
	t.Helper()
	dir := paths.ServiceStateDir(service).LogsDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestCollectLogs_RejectsUnknownServiceBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	if _, err := collectLogs(paths, "hdfs", "bogus", defaultLogLines); err == nil {
		t.Fatal("expected error for unknown service")
	}
}

func TestCollectLogs_RejectsInvalidLinesBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	// Write a real file so a failure to validate-before-read would be visible
	// via unexpected file collection.
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\n")

	if _, err := collectLogs(paths, "local", "hive", -1); err == nil {
		t.Fatal("expected error for negative --lines")
	}
	if _, err := collectLogs(paths, "local", "hive", maxLogLines+1); err == nil {
		t.Fatal("expected error for --lines exceeding max")
	}
}

func TestCollectLogs_LocalProfileDefaultsToHiveOnly(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "local", "", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 1 || report.Services[0].Name != "hive" {
		t.Fatalf("report.Services = %#v, want only hive", report.Services)
	}
}

func TestCollectLogs_HdfsProfileDefaultsToAllServices(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "hdfs", "", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 3 {
		t.Fatalf("report.Services = %#v, want hdfs+yarn+hive", report.Services)
	}
}

func TestCollectLogs_ExplicitSelectionIgnoresActiveProfile(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "local", "hdfs", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 1 || report.Services[0].Name != "hdfs" {
		t.Fatalf("report.Services = %#v, want only hdfs despite local profile", report.Services)
	}
}

func TestCollectLogs_UnknownExplicitServiceIsRejected(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	if _, err := collectLogs(paths, "hdfs", "spark", defaultLogLines); err == nil {
		t.Fatal("expected error for unsupported explicit service")
	}
}

func TestCollectLogs_TailsMultilineFileToLastNLines(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "l1\nl2\nl3\nl4\nl5\n")

	report, err := collectLogs(paths, "local", "hive", 2)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "l4,l5" {
		t.Fatalf("lines = %#v, want [l4 l5]", got)
	}
}

func TestCollectLogs_NoFinalNewlinePreservesLastLine(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc")

	report, err := collectLogs(paths, "local", "hive", 10)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("lines = %#v, want [a b c]", got)
	}
}

func TestCollectLogs_EmptyFileProducesNoLinesAndNoError(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "")

	report, err := collectLogs(paths, "local", "hive", 10)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	f := report.Services[0].Files[0]
	if f.Missing || f.Error != "" || len(f.Lines) != 0 {
		t.Fatalf("file = %#v, want present/no-error/no-lines", f)
	}
}

func TestCollectLogs_ZeroLinesSkipsReadingContent(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc\n")

	report, err := collectLogs(paths, "local", "hive", 0)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	f := report.Services[0].Files[0]
	if f.Missing || f.Error != "" || len(f.Lines) != 0 {
		t.Fatalf("file = %#v, want present/no-error/no-lines for --lines 0", f)
	}
}

func TestCollectLogs_LargeCountReturnsWholeFile(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc\n")

	report, err := collectLogs(paths, "local", "hive", maxLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("lines = %#v, want [a b c]", got)
	}
}

func TestCollectLogs_MissingFileIsNotAnError(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	// Neither metastore.log nor hiveserver2.log exists.
	report, err := collectLogs(paths, "local", "hive", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("errors = %#v, want none for missing files", report.Errors)
	}
	for _, f := range report.Services[0].Files {
		if !f.Missing {
			t.Fatalf("file = %#v, want missing=true", f)
		}
	}
}

func TestCollectLogs_ReadErrorDoesNotSuppressOtherFiles(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	dir := paths.ServiceStateDir("hive").LogsDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// metastore.log is a directory, not a readable log file.
	if err := os.MkdirAll(filepath.Join(dir, "metastore.log"), 0755); err != nil {
		t.Fatalf("mkdir metastore.log: %v", err)
	}
	writeLogFixture(t, paths, "hive", "hiveserver2.log", "ready\n")

	report, err := collectLogs(paths, "local", "hive", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs returned unexpected top-level error: %v", err)
	}
	if len(report.Errors) != 1 {
		t.Fatalf("errors = %#v, want exactly one", report.Errors)
	}

	var hs2 *logsFile
	for i := range report.Services[0].Files {
		if strings.HasSuffix(report.Services[0].Files[i].Path, "hiveserver2.log") {
			hs2 = &report.Services[0].Files[i]
		}
	}
	if hs2 == nil {
		t.Fatal("hiveserver2.log entry not found in report")
	}
	if hs2.Error != "" || len(hs2.Lines) != 1 || hs2.Lines[0] != "ready" {
		t.Fatalf("hiveserver2.log entry = %#v, want unaffected by metastore.log error", hs2)
	}
}

func TestLogsCmd_RejectsInvalidLinesFlagBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	cmd := NewLogsCmd(func() *config.Paths { return paths })
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive", "--lines", "-5"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for negative --lines")
	}
}

func TestLogsCmd_RejectsUnknownService(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	cmd := NewLogsCmd(func() *config.Paths { return paths })
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"bogus"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for unknown service")
	}
}

func TestLogsCmd_PrintsDeterministicSourceLabelsAndTail(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "one\ntwo\nthree\n")

	cmd := NewLogsCmd(func() *config.Paths { return paths })
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive", "--lines", "2"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("logs hive: %v", err)
	}

	metastorePath := filepath.Join(paths.ServiceStateDir("hive").LogsDir, "metastore.log")
	if !strings.Contains(out.String(), "==> "+metastorePath+" <==") {
		t.Fatalf("missing deterministic source label:\n%s", out.String())
	}
	if strings.Contains(out.String(), "one") {
		t.Fatalf("expected tail to exclude line beyond --lines 2:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "two") || !strings.Contains(out.String(), "three") {
		t.Fatalf("expected last 2 lines present:\n%s", out.String())
	}
}

func TestLogsCmd_ReadFailureReturnsNonzeroExitButKeepsOtherOutput(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	dir := paths.ServiceStateDir("hive").LogsDir
	if err := os.MkdirAll(filepath.Join(dir, "metastore.log"), 0755); err != nil {
		t.Fatalf("mkdir metastore.log: %v", err)
	}
	writeLogFixture(t, paths, "hive", "hiveserver2.log", "ready\n")

	cmd := NewLogsCmd(func() *config.Paths { return paths })
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected nonzero exit for read failure")
	}
	if !strings.Contains(out.String(), "ready") {
		t.Fatalf("expected hiveserver2.log output despite metastore.log read error:\n%s", out.String())
	}
}

type countingFile struct {
	*os.File
	reads     int
	seeks     int
	bytesRead int64
}

func (c *countingFile) Read(p []byte) (int, error) {
	n, err := c.File.Read(p)
	c.reads++
	c.bytesRead += int64(n)
	return n, err
}

func (c *countingFile) Seek(offset int64, whence int) (int64, error) {
	c.seeks++
	return c.File.Seek(offset, whence)
}

type fakeFileInfo struct {
	size int64
	mode os.FileMode
}

func (f fakeFileInfo) Name() string       { return "log" }
func (f fakeFileInfo) Size() int64        { return f.size }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeFileInfo) Sys() any           { return nil }

type memFile struct {
	data      []byte
	pos       int64
	reads     int
	seeks     int
	bytesRead int64
	onStat    func(*memFile)
	onSeek    func(*memFile)
}

func (m *memFile) Stat() (os.FileInfo, error) {
	if m.onStat != nil {
		m.onStat(m)
	}
	mode := os.FileMode(0644)
	return fakeFileInfo{size: int64(len(m.data)), mode: mode}, nil
}

func (m *memFile) Seek(offset int64, whence int) (int64, error) {
	m.seeks++
	if m.onSeek != nil {
		m.onSeek(m)
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = m.pos + offset
	case io.SeekEnd:
		abs = int64(len(m.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative seek")
	}
	m.pos = abs
	return abs, nil
}

func (m *memFile) Read(p []byte) (int, error) {
	m.reads++
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	m.bytesRead += int64(n)
	return n, nil
}

func TestTailRegularFile_CountingSeamReadsSuffixNotWholeFile(t *testing.T) {
	const fileSize = 4 << 20 // 4MiB regular file
	const wantLines = 5
	path := filepath.Join(t.TempDir(), "large.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	line := []byte("0123456789abcd\n") // 15 bytes
	buf := bytes.Repeat(line, 4096)
	written := 0
	for written < fileSize {
		n, werr := f.Write(buf)
		if werr != nil {
			t.Fatalf("write: %v", werr)
		}
		written += n
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	opened, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer opened.Close()
	cf := &countingFile{File: opened}

	got, err := tailRegularFile(cf, wantLines)
	if err != nil {
		t.Fatalf("tailRegularFile: %v", err)
	}
	if len(got) != wantLines {
		t.Fatalf("len(lines)=%d, want %d (%v)", len(got), wantLines, got)
	}
	for _, l := range got {
		if l != "0123456789abcd" {
			t.Fatalf("unexpected line %q", l)
		}
	}
	if cf.bytesRead >= fileSize {
		t.Fatalf("bytesRead=%d, scanned the whole %d-byte file", cf.bytesRead, fileSize)
	}
	// Suffix of 5 short lines fits in one 32KiB block; allow a second block of slack.
	maxWanted := int64(2 * tailBlockSize)
	if cf.bytesRead > maxWanted {
		t.Fatalf("bytesRead=%d, want <= %d (suffix-sized blocks, not the %d-byte file); seeks=%d reads=%d",
			cf.bytesRead, maxWanted, fileSize, cf.seeks, cf.reads)
	}
	if cf.seeks == 0 {
		t.Fatal("expected Seek to be used for end-based reads")
	}
}

func TestTailRegularFile_EmptyInput(t *testing.T) {
	got, err := tailRegularFile(&memFile{}, 10)
	if err != nil {
		t.Fatalf("tailRegularFile: %v", err)
	}
	if got != nil && len(got) != 0 {
		t.Fatalf("lines = %#v, want empty", got)
	}
}

func TestTailRegularFile_FinalNewlinePresenceAndAbsence(t *testing.T) {
	withNL, err := tailRegularFile(&memFile{data: []byte("a\nb\nc\n")}, 10)
	if err != nil {
		t.Fatalf("with newline: %v", err)
	}
	withoutNL, err := tailRegularFile(&memFile{data: []byte("a\nb\nc")}, 10)
	if err != nil {
		t.Fatalf("without newline: %v", err)
	}
	if strings.Join(withNL, ",") != "a,b,c" {
		t.Fatalf("with final newline: %#v", withNL)
	}
	if strings.Join(withoutNL, ",") != "a,b,c" {
		t.Fatalf("without final newline: %#v", withoutNL)
	}
}

func TestTailRegularFile_BlockBoundaryLineBreak(t *testing.T) {
	suffix := make([]byte, tailBlockSize)
	suffix[0] = '\n'
	end := []byte("keep-a\nkeep-b\n")
	fillEnd := len(suffix) - len(end)
	for i := 1; i < fillEnd-1; i++ {
		suffix[i] = 'y'
	}
	if fillEnd > 0 {
		suffix[fillEnd-1] = '\n'
	}
	copy(suffix[fillEnd:], end)
	data := append([]byte("discarded-history\n"), suffix...)

	got, err := tailRegularFile(&memFile{data: data}, 2)
	if err != nil {
		t.Fatalf("tailRegularFile: %v", err)
	}
	if strings.Join(got, ",") != "keep-a,keep-b" {
		t.Fatalf("lines = %#v, want [keep-a keep-b] (newline at 32KiB boundary)", got)
	}
}

func TestTailRegularFile_FewerThanNLines(t *testing.T) {
	got, err := tailRegularFile(&memFile{data: []byte("only\ntwo\n")}, 10)
	if err != nil {
		t.Fatalf("tailRegularFile: %v", err)
	}
	if strings.Join(got, ",") != "only,two" {
		t.Fatalf("lines = %#v, want [only two]", got)
	}
}

func TestTailRegularFile_OversizedDiscardedHistoricalLine(t *testing.T) {
	hist := bytes.Repeat([]byte{'X'}, maxLogLineBytes+128)
	data := append(hist, []byte("\nrecent-a\nrecent-b\n")...)
	mf := &memFile{data: data}

	got, err := tailRegularFile(mf, 2)
	if err != nil {
		t.Fatalf("historical oversized line should not fail suffix tail: %v", err)
	}
	if strings.Join(got, ",") != "recent-a,recent-b" {
		t.Fatalf("lines = %#v, want [recent-a recent-b]", got)
	}
	if mf.bytesRead >= int64(len(hist)) {
		t.Fatalf("bytesRead=%d scanned historical oversized line of %d bytes", mf.bytesRead, len(hist))
	}
}

func TestTailRegularFile_OversizedSuffixLineIsError(t *testing.T) {
	data := bytes.Repeat([]byte{'Y'}, maxLogLineBytes+1)
	_, err := tailRegularFile(&memFile{data: data}, 1)
	if err == nil {
		t.Fatal("expected error for oversized suffix line")
	}
	if !strings.Contains(err.Error(), "log line exceeds") {
		t.Fatalf("error = %v, want suffix line limit", err)
	}
}

func TestTailRegularFile_ConcurrentAppendIgnoresGrowthAfterSnapshot(t *testing.T) {
	mf := &memFile{data: []byte("a\nb\nc\n")}
	mf.onStat = func(m *memFile) {
		if m.reads > 0 && !bytes.Contains(m.data, []byte("GROW")) {
			m.data = append(m.data, []byte("GROW\n")...)
		}
	}

	got, err := tailRegularFile(mf, 2)
	if err != nil {
		t.Fatalf("tailRegularFile: %v", err)
	}
	joined := strings.Join(got, ",")
	if strings.Contains(joined, "GROW") {
		t.Fatalf("lines = %#v, concurrent append after snapshot must not be included", got)
	}
	if joined != "b,c" {
		t.Fatalf("lines = %#v, want snapshot suffix [b c]", got)
	}
}

func TestTailRegularFile_ConcurrentTruncateRetriesThenTailsNewSize(t *testing.T) {
	full := []byte("l0\nl1\nl2\nl3\nl4\nl5\n")
	shrunk := []byte("l0\nl1\nl2\n")
	mf := &memFile{data: append([]byte{}, full...)}
	mf.onSeek = func(m *memFile) {
		if bytes.Equal(m.data, full) {
			m.data = append([]byte{}, shrunk...)
		}
	}

	got, err := tailRegularFile(mf, 2)
	if err != nil {
		t.Fatalf("tailRegularFile after truncate retry: %v", err)
	}
	if strings.Join(got, ",") != "l1,l2" {
		t.Fatalf("lines = %#v, want last 2 of truncated file [l1 l2]", got)
	}
}

func TestTailRegularFile_PersistentTruncateIsBoundedError(t *testing.T) {
	mf := &memFile{data: bytes.Repeat([]byte("line\n"), 200)}
	mf.onStat = func(m *memFile) {
		if len(m.data) > 0 {
			m.data = m.data[:len(m.data)-1]
		}
	}

	_, err := tailRegularFile(mf, 3)
	if !errors.Is(err, errTailTruncated) {
		t.Fatalf("error = %v, want errTailTruncated after bounded retries", err)
	}
	if mf.seeks > (maxTailTruncateRetries+1)*16 {
		t.Fatalf("seeks=%d looks unbounded for %d truncate retries", mf.seeks, maxTailTruncateRetries)
	}
}

func TestCollectLogs_OversizedHistoricalLineDoesNotFailFile(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	hist := bytes.Repeat([]byte{'X'}, maxLogLineBytes+64)
	content := append(hist, []byte("\nkeep\n")...)
	writeLogFixture(t, paths, "hive", "metastore.log", string(content))

	report, err := collectLogs(paths, "local", "hive", 1)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("errors = %#v, historical oversized line must not fail the file", report.Errors)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "keep" {
		t.Fatalf("lines = %#v, want [keep]", got)
	}
}

func BenchmarkTailFile_32MiBFile_Last10Lines(b *testing.B) {
	const fileSize = 32 << 20 // 32MiB
	const wantLines = 10
	path := filepath.Join(b.TempDir(), "bench.log")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	buf := bytes.Repeat([]byte("abcdefghij\n"), 4096)
	written := 0
	for written < fileSize {
		n, werr := f.Write(buf)
		if werr != nil {
			b.Fatalf("write: %v", werr)
		}
		written += n
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lines, missing, err := tailFile(path, wantLines)
		if err != nil {
			b.Fatalf("tailFile: %v", err)
		}
		if missing || len(lines) != wantLines {
			b.Fatalf("missing=%v len=%d, want %d lines from 32MiB file", missing, len(lines), wantLines)
		}
	}
}
