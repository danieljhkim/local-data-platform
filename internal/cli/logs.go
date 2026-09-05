package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

const (
	// defaultLogLines matches the historical `tail -n120` behavior.
	defaultLogLines = 120
	// maxLogLines bounds --lines to keep output and memory use predictable.
	maxLogLines = 100000
	// tailBlockSize is the backward-read chunk for regular files.
	tailBlockSize = 32 * 1024
	// maxLogLineBytes is the per-line cap for a line inside the requested
	// suffix (same 1MiB token limit as the previous bufio.Scanner path).
	maxLogLineBytes = 1024 * 1024
	// maxTailTruncateRetries bounds restarts when a concurrent truncate
	// shrinks the file mid-read.
	maxTailTruncateRetries = 3
)

// errTailTruncated is returned after bounded retries if the file keeps
// shrinking during a tail read.
var errTailTruncated = errors.New("file truncated during tail read")

// seekStatReader is the regular-file tail seam: counted Seek/Read
// implementations can wrap *os.File or a fake without scanning from byte 0.
type seekStatReader interface {
	io.Reader
	io.Seeker
	Stat() (os.FileInfo, error)
}

// logFilesByService lists the on-disk log file names for each service, in
// display order. These are fixed paths under $BASE_DIR/state/<service>/logs
// and are resolved without touching binary/environment discovery, so logs
// from a stopped or uninstalled service remain inspectable.
var logFilesByService = map[string][]string{
	"hdfs": {"namenode.log", "datanode.log"},
	"yarn": {"resourcemanager.log", "nodemanager.log"},
	"hive": {"metastore.log", "hiveserver2.log"},
}

// logServiceOrder is the display order used when no service is selected.
var logServiceOrder = []string{"hdfs", "yarn", "hive"}

// logsFile is one log file's collected observation.
type logsFile struct {
	Path    string
	Missing bool
	Lines   []string
	Error   string
}

// logsService groups the log files collected for a single service.
type logsService struct {
	Name  string
	Files []logsFile
}

// logsReport is the structured result of a logs collection pass.
type logsReport struct {
	Profile  string
	Lines    int
	Services []logsService
	Errors   []string
}

// NewLogsCmd creates the logs command
func NewLogsCmd(pathsGetter func() *config.Paths) *cobra.Command {
	var lines int
	cmd := &cobra.Command{
		Use:          "logs [hdfs|yarn|hive]",
		Short:        "Show recent log entries from one or all services",
		SilenceUsage: true,
		Long: fmt.Sprintf(`Display the most recent log entries from HDFS, YARN, and Hive service log files.

With no service argument, selection follows the active profile (like "status"):
  - hdfs profile: shows logs for HDFS, YARN, and Hive
  - local profile: shows only Hive logs

With a service name, shows logs for only that service, regardless of the
active profile.

Log files are located by their fixed on-disk paths under
$BASE_DIR/state/<service>/logs, independent of whether the underlying
Hadoop/Hive/Spark executables are installed or the service is running, so
logs from a stopped service remain inspectable.

--lines controls how many trailing lines are printed per log file (default
%d). 0 lists the selected log files without printing content. The maximum
accepted value is %d. A missing log file is reported as such and is not an
error; a file that exists but cannot be read is reported as a read error
without preventing other files from being shown.

Examples:
  local-data logs                  # Logs for the active profile's services
  local-data logs hdfs             # HDFS logs only
  local-data logs hive --lines 50  # Last 50 lines of each Hive log file
  local-data logs hive --lines 0   # List Hive log files without content`, defaultLogLines, maxLogLines),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			target := ""
			if len(args) > 0 {
				target = args[0]
			}
			profile, _ := paths.ActiveProfile()

			report, err := collectLogs(paths, profile, target, lines)
			if err != nil {
				return err
			}

			renderLogsReport(cmd.OutOrStdout(), report)

			if len(report.Errors) > 0 {
				return fmt.Errorf("failed to read %d log file(s): %s", len(report.Errors), strings.Join(report.Errors, "; "))
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&lines, "lines", defaultLogLines,
		fmt.Sprintf("trailing lines to show per log file (0-%d; 0 lists files without content)", maxLogLines))

	return cmd
}

// collectLogs validates the service selection and --lines value, then reads
// the selected log files. Validation happens before any file is opened, and
// file paths are computed purely from paths (no binary/environment
// discovery), so this works even when Hadoop/Hive/Spark are not installed.
func collectLogs(paths *config.Paths, profile, target string, lines int) (logsReport, error) {
	report := logsReport{Profile: profile, Lines: lines}

	if lines < 0 {
		return report, fmt.Errorf("invalid --lines value %d: must be >= 0", lines)
	}
	if lines > maxLogLines {
		return report, fmt.Errorf("invalid --lines value %d: must be <= %d", lines, maxLogLines)
	}

	var names []string
	switch target {
	case "":
		if profile == "local" {
			names = []string{"hive"}
		} else {
			names = logServiceOrder
		}
	case "hdfs", "yarn", "hive":
		names = []string{target}
	default:
		return report, fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
	}

	for _, name := range names {
		logDir := paths.ServiceStateDir(name).LogsDir
		svc := logsService{Name: name}
		for _, fileName := range logFilesByService[name] {
			path := filepath.Join(logDir, fileName)
			content, missing, readErr := tailFile(path, lines)
			lf := logsFile{Path: path, Missing: missing, Lines: content}
			if readErr != nil {
				lf.Error = readErr.Error()
				report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", path, readErr))
			}
			svc.Files = append(svc.Files, lf)
		}
		report.Services = append(report.Services, svc)
	}

	return report, nil
}

// renderLogsReport prints a collected report with deterministic per-file
// source labels, mirroring the multi-file "==> path <==" convention of
// coreutils tail.
func renderLogsReport(w io.Writer, report logsReport) {
	first := true
	for _, svc := range report.Services {
		for _, f := range svc.Files {
			if !first {
				fmt.Fprintln(w)
			}
			first = false

			fmt.Fprintf(w, "==> %s <==\n", f.Path)
			switch {
			case f.Error != "":
				fmt.Fprintf(w, "(error reading log file: %s)\n", f.Error)
			case f.Missing:
				fmt.Fprintln(w, "(missing)")
			case report.Lines == 0:
				fmt.Fprintln(w, "(0 lines requested; file exists)")
			case len(f.Lines) == 0:
				fmt.Fprintln(w, "(empty)")
			default:
				for _, line := range f.Lines {
					fmt.Fprintln(w, line)
				}
			}
		}
	}
}

// tailFile returns up to n trailing lines of path. A missing file is
// reported via the missing return value rather than an error. n == 0 skips
// reading file content entirely.
func tailFile(path string, n int) (lines []string, missing bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	defer f.Close()

	info, statErr := f.Stat()
	if statErr == nil && info.IsDir() {
		return nil, false, fmt.Errorf("%s is a directory, not a log file", path)
	}

	if n == 0 {
		return nil, false, nil
	}

	if statErr == nil && info.Mode().IsRegular() {
		lines, err = tailRegularFile(f, n)
	} else {
		lines, err = tailLines(f, n)
	}
	if err != nil {
		return nil, false, err
	}
	return lines, false, nil
}

// tailRegularFile returns the last n lines of a regular file by reading
// fixed-size blocks from the end until the suffix is identified.
//
// Concurrent result semantics:
//   - Append: a size snapshot is taken at the start of each attempt. Bytes
//     written after that snapshot are not read, so a growing file cannot
//     cause unbounded I/O or an infinite loop.
//   - Truncate: if the file shrinks below the snapshot while reading, the
//     attempt is abandoned and retried with a fresh snapshot, up to
//     maxTailTruncateRetries extra times. Persistent shrinking is reported
//     as errTailTruncated (a per-file read error).
//
// Oversized-line policy: a line inside the requested suffix may be at most
// maxLogLineBytes. Longer suffix lines fail this file. An oversized line
// older than the suffix is never scanned once N line starts are found.
func tailRegularFile(f seekStatReader, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	for attempt := 0; attempt <= maxTailTruncateRetries; attempt++ {
		lines, retry, err := tailRegularFileOnce(f, n)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		return lines, nil
	}
	return nil, errTailTruncated
}

func tailRegularFileOnce(f seekStatReader, n int) (lines []string, retry bool, err error) {
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	size := info.Size()
	if size == 0 {
		return nil, false, nil
	}

	var pieces [][]byte
	pos := size
	remaining := n
	skipTrailingNL := true
	bytesSinceNL := 0

	for pos > 0 && remaining > 0 {
		chunk := int64(tailBlockSize)
		if chunk > pos {
			chunk = pos
		}
		readAt := pos - chunk
		if _, err := f.Seek(readAt, io.SeekStart); err != nil {
			return nil, false, err
		}
		buf := make([]byte, chunk)
		got, readErr := io.ReadFull(f, buf)
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF || int64(got) != chunk {
			return nil, true, nil
		}
		if readErr != nil {
			return nil, false, readErr
		}
		if cur, statErr := f.Stat(); statErr == nil && cur.Size() < size {
			return nil, true, nil
		}

		scanFrom := len(buf) - 1
		if skipTrailingNL {
			skipTrailingNL = false
			if scanFrom >= 0 && buf[scanFrom] == '\n' {
				scanFrom--
			}
		}

		cut := 0
		found := false
		for i := scanFrom; i >= 0; i-- {
			if buf[i] == '\n' {
				bytesSinceNL = 0
				remaining--
				if remaining == 0 {
					cut = i + 1
					found = true
					break
				}
				continue
			}
			bytesSinceNL++
			if bytesSinceNL > maxLogLineBytes {
				return nil, false, fmt.Errorf("log line exceeds %d-byte limit", maxLogLineBytes)
			}
		}
		if found {
			pieces = append(pieces, buf[cut:])
			break
		}
		pieces = append(pieces, buf)
		pos = readAt
	}

	total := 0
	for _, p := range pieces {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for i := len(pieces) - 1; i >= 0; i-- {
		out = append(out, pieces[i]...)
	}
	return splitTailLines(out), false, nil
}

func splitTailLines(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	if b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return []string{""}
	}
	return strings.Split(string(b), "\n")
}

// tailLines reads r line by line and returns at most the last n lines, in
// original order, using a fixed-size ring buffer so memory use is bounded by
// n rather than the file size. Used for non-regular files that cannot seek.
// Handles empty input and a final line with no trailing newline.
func tailLines(r io.Reader, n int) ([]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxLogLineBytes)

	ring := make([]string, n)
	count := 0
	for scanner.Scan() {
		ring[count%n] = scanner.Text()
		count++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	size := n
	if count < n {
		size = count
	}
	result := make([]string, size)
	start := count - size
	for i := 0; i < size; i++ {
		result[i] = ring[(start+i)%n]
	}
	return result, nil
}
