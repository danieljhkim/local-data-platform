package env

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// HomebrewDetector handles detection of Homebrew-installed packages
type HomebrewDetector struct{}

// NewHomebrewDetector creates a new Homebrew detector
func NewHomebrewDetector() *HomebrewDetector {
	return &HomebrewDetector{}
}

// Prefix returns the installation prefix for a Homebrew formula
// Mirrors: brew --prefix <formula>
// Returns empty string (not an error) if formula not found
func (h *HomebrewDetector) Prefix(formula string) string {
	cmd := exec.Command("brew", "--prefix", formula)
	output, err := cmd.Output()
	if err != nil {
		// Not found or brew not installed - not an error, just return empty
		return ""
	}

	return strings.TrimSpace(string(output))
}

// IsInstalled checks if brew command is available
func (h *HomebrewDetector) IsInstalled() bool {
	_, err := exec.LookPath("brew")
	return err == nil
}

// JavaDetector handles Java installation detection
type JavaDetector struct{}

// NewJavaDetector creates a new Java detector
func NewJavaDetector() *JavaDetector {
	return &JavaDetector{}
}

// FindJavaHome finds the Java installation home directory
// Prefers Java 17 if available, otherwise returns any available Java
// Mirrors: /usr/libexec/java_home -v 17 || /usr/libexec/java_home
func (j *JavaDetector) FindJavaHome() string {
	// First check JAVA_HOME environment variable
	if javaHome := os.Getenv("JAVA_HOME"); javaHome != "" {
		return javaHome
	}

	// On macOS, use /usr/libexec/java_home
	javaHomeBin := "/usr/libexec/java_home"
	if _, err := os.Stat(javaHomeBin); err == nil {
		// Try Java 17 first (preferred for Hadoop/Hive)
		cmd := exec.Command(javaHomeBin, "-v", "17")
		if output, err := cmd.Output(); err == nil {
			return strings.TrimSpace(string(output))
		}

		// Fallback to any available Java version
		cmd = exec.Command(javaHomeBin)
		if output, err := cmd.Output(); err == nil {
			return strings.TrimSpace(string(output))
		}
	}

	return ""
}

// MajorVersion returns the major version of the installed Java
// Parses output from: java -version
// Returns 0 if Java is not found or version cannot be parsed
func (j *JavaDetector) MajorVersion() int {
	cmd := exec.Command("java", "-version")
	// java -version outputs to stderr, not stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return 0
	}

	output := stderr.String()

	// Parse version from output like:
	// openjdk version "17.0.9" 2023-10-17
	// java version "1.8.0_392"
	// Regex to match version string
	re := regexp.MustCompile(`version "([^"]+)"`)
	matches := re.FindStringSubmatch(output)
	if len(matches) < 2 {
		return 0
	}

	versionStr := matches[1]

	// Handle both old format (1.8.x) and new format (17.x)
	parts := strings.Split(versionStr, ".")
	if len(parts) == 0 {
		return 0
	}

	// If starts with "1.", use second part (e.g., "1.8" -> 8)
	if parts[0] == "1" && len(parts) > 1 {
		major, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0
		}
		return major
	}

	// Otherwise, use first part (e.g., "17.0.9" -> 17)
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0
	}

	return major
}

// IsInstalled checks if java command is available
func (j *JavaDetector) IsInstalled() bool {
	_, err := exec.LookPath("java")
	return err == nil
}

// ToolDetector provides generic command detection
type ToolDetector struct{}

// NewToolDetector creates a new tool detector
func NewToolDetector() *ToolDetector {
	return &ToolDetector{}
}

// IsInstalled checks if a command is available in PATH
func (t *ToolDetector) IsInstalled(command string) bool {
	_, err := exec.LookPath(command)
	return err == nil
}

// DetectAll detects all necessary tools and returns their installation status
func (t *ToolDetector) DetectAll(tools []string) map[string]bool {
	results := make(map[string]bool)
	for _, tool := range tools {
		results[tool] = t.IsInstalled(tool)
	}
	return results
}

// FindSparkHome finds Spark installation home
// For Homebrew installs, adds /libexec suffix
func FindSparkHome() string {
	// Check environment variable first
	if sparkHome := os.Getenv("SPARK_HOME"); sparkHome != "" {
		return sparkHome
	}

	// Try Homebrew
	hb := NewHomebrewDetector()

	// Try apache-spark first
	prefix := hb.Prefix("apache-spark")
	if prefix == "" {
		// Fallback to spark
		prefix = hb.Prefix("spark")
	}

	if prefix != "" {
		// Homebrew Spark needs /libexec suffix
		return prefix + "/libexec"
	}

	return ""
}

// HadoopInstall contains Hadoop installation paths
type HadoopInstall struct {
	Prefix string // Brew prefix (for bin/sbin in PATH)
	Home   string // HADOOP_HOME (libexec for Homebrew)
}

// FindHadoopInstall finds Hadoop installation paths
func FindHadoopInstall() *HadoopInstall {
	// Check environment variable first
	if hadoopHome := os.Getenv("HADOOP_HOME"); hadoopHome != "" {
		return &HadoopInstall{
			Prefix: hadoopHome,
			Home:   hadoopHome,
		}
	}

	// Try Homebrew
	hb := NewHomebrewDetector()
	prefix := hb.Prefix("hadoop")
	if prefix != "" {
		return &HadoopInstall{
			Prefix: prefix,
			// Homebrew Hadoop needs /libexec suffix for proper library resolution
			Home: prefix + "/libexec",
		}
	}

	return nil
}

// FindHadoopHome finds Hadoop installation home (legacy, returns Home)
func FindHadoopHome() string {
	install := FindHadoopInstall()
	if install != nil {
		return install.Home
	}
	return ""
}

// FindHiveHome finds Hive installation home
func FindHiveHome() string {
	// Check environment variable first
	if hiveHome := os.Getenv("HIVE_HOME"); hiveHome != "" {
		return hiveHome
	}

	// Try Homebrew
	hb := NewHomebrewDetector()

	// Try apache-hive first
	prefix := hb.Prefix("apache-hive")
	if prefix == "" {
		// Fallback to hive
		prefix = hb.Prefix("hive")
	}

	if prefix != "" {
		// Homebrew Hive needs /libexec suffix for proper library resolution
		return prefix + "/libexec"
	}

	return ""
}

// DetectionResult holds the result of environment detection
type DetectionResult struct {
	JavaHome     string
	JavaMajor    int
	HadoopHome   string
	HadoopPrefix string // Brew prefix for PATH (may differ from Home)
	HiveHome     string
	SparkHome    string
}

// DetectEnvironment performs comprehensive environment detection
func DetectEnvironment() (*DetectionResult, error) {
	javaDetector := NewJavaDetector()

	result := &DetectionResult{
		JavaHome:  javaDetector.FindJavaHome(),
		JavaMajor: javaDetector.MajorVersion(),
		HiveHome:  FindHiveHome(),
		SparkHome: FindSparkHome(),
	}

	// Set Hadoop paths
	hadoopInstall := FindHadoopInstall()
	if hadoopInstall != nil {
		result.HadoopHome = hadoopInstall.Home
		result.HadoopPrefix = hadoopInstall.Prefix
	}

	// Hive is required
	if result.HiveHome == "" {
		return nil, fmt.Errorf("could not determine HIVE_HOME (install Homebrew Hive or set HIVE_HOME)")
	}

	return result, nil
}
