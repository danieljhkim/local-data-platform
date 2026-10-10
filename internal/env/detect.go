package env

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

// homebrewPrefixes are the ARM and Intel Homebrew prefixes searched for JDKs.
var homebrewPrefixes = []string{"/opt/homebrew", "/usr/local"}

// FindJavaHome honors an explicit JAVA_HOME. Otherwise it keeps the Java 17
// default used by the Hadoop stack unless the installed Hive needs a newer
// Java, in which case it selects a compatible installed JDK.
func (j *JavaDetector) FindJavaHome() string {
	if home := os.Getenv("JAVA_HOME"); home != "" {
		return home
	}
	return j.FindJavaHomeForHive(FindHiveHome())
}

// FindJavaHomeForHive is FindJavaHome for a caller that already knows HIVE_HOME.
// An explicit JAVA_HOME is never replaced. With JAVA_HOME unset, the Java 17
// default is kept when Hive accepts it (or its requirement is unknown);
// otherwise the lowest installed Homebrew openjdk that satisfies Hive wins,
// then the java on PATH. When nothing satisfies Hive, the Java 17 default is
// returned so the Hive Java check reports the mismatch.
func (j *JavaDetector) FindJavaHomeForHive(hiveHome string) string {
	if home := os.Getenv("JAVA_HOME"); home != "" {
		return home
	}
	var fallback string
	for _, prefix := range homebrewPrefixes {
		path := filepath.Join(prefix, "opt", "openjdk@17")
		if _, err := os.Stat(path); err == nil {
			fallback = path
			break
		}
	}
	required, err := HiveRequiredJavaMajor(hiveHome)
	if err != nil || required == 0 {
		return fallback
	}
	if fallback != "" && javaHomeMajor(fallback) >= required {
		return fallback
	}
	for _, candidate := range installedJavaHomes() {
		if javaHomeMajor(candidate) >= required {
			return candidate
		}
	}
	return fallback
}

// installedJavaHomes lists Homebrew openjdk@N installations in ascending
// version order, then the unversioned openjdk, then the java found on PATH.
func installedJavaHomes() []string {
	var homes []string
	for _, prefix := range homebrewPrefixes {
		matches, _ := filepath.Glob(filepath.Join(prefix, "opt", "openjdk@*"))
		sort.SliceStable(matches, func(a, b int) bool {
			return openjdkSuffix(matches[a]) < openjdkSuffix(matches[b])
		})
		homes = append(homes, matches...)
	}
	for _, prefix := range homebrewPrefixes {
		path := filepath.Join(prefix, "opt", "openjdk")
		if _, err := os.Stat(path); err == nil {
			homes = append(homes, path)
		}
	}
	if home := pathJavaHome(); home != "" {
		homes = append(homes, home)
	}
	return homes
}

// openjdkSuffix extracts N from .../openjdk@N so versions sort numerically.
func openjdkSuffix(path string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "openjdk@"))
	if err != nil {
		return 0
	}
	return n
}

// pathJavaHome derives a JAVA_HOME from the java found on PATH, or "".
func pathJavaHome() string {
	java, err := ResolveExecutable("java", []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		return ""
	}
	resolved, err := filepath.EvalSymlinks(java)
	if err != nil || filepath.Base(resolved) != "java" {
		return ""
	}
	bin := filepath.Dir(resolved)
	if filepath.Base(bin) != "bin" {
		return ""
	}
	return filepath.Dir(bin)
}

func javaHomeMajor(home string) int {
	major, _ := selectedJavaMajor(&Environment{JavaHome: home, Path: os.Getenv("PATH")})
	return major
}

// MajorVersion returns the major version of the installed Java
// Returns 0 if the selected runtime cannot be measured.
func (j *JavaDetector) MajorVersion() int {
	return javaHomeMajor(j.FindJavaHome())
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
	hiveHome := FindHiveHome()
	javaHome := NewJavaDetector().FindJavaHomeForHive(hiveHome)

	result := &DetectionResult{
		JavaHome:  javaHome,
		JavaMajor: javaHomeMajor(javaHome),
		HiveHome:  hiveHome,
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
