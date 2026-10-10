package env

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var javaVersionPattern = regexp.MustCompile(`(?:java|openjdk) version "(?:1\.)?([0-9]+)`)

// selectedJavaMajor measures the executable Hive will use, rather than inferring
// a version from the installation directory's name. An explicit JAVA_HOME must
// not silently fall back to a different java on PATH.
func selectedJavaMajor(environment *Environment) (int, error) {
	name := "java"
	if environment.JavaHome != "" {
		name = filepath.Join(environment.JavaHome, "bin", "java")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd, err := CommandContext(ctx, name, []string{"-version"}, environment.Export())
	if err != nil {
		return 0, fmt.Errorf("cannot determine selected Java version: %w", err)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("cannot determine selected Java version: %w", err)
	}
	match := javaVersionPattern.FindSubmatch(output)
	if len(match) != 2 {
		return 0, fmt.Errorf("cannot determine selected Java version from java -version")
	}
	major, err := strconv.Atoi(string(match[1]))
	if err != nil || major == 0 {
		return 0, fmt.Errorf("invalid selected Java version")
	}
	return major, nil
}

// HiveRequiredJavaMajor reads the installed schema tool's bytecode. A missing
// class returns zero (unknown), retaining support for legacy/custom Hive layouts.
// Both the current beeline and older standalone metastore class names are used.
func HiveRequiredJavaMajor(hiveHome string) (int, error) {
	if hiveHome == "" {
		return 0, nil
	}
	jars, err := filepath.Glob(filepath.Join(hiveHome, "lib", "hive-*.jar"))
	if err != nil {
		return 0, fmt.Errorf("cannot inspect Hive Java requirement: %w", err)
	}
	required := 0
	for _, jar := range jars {
		major, err := schemaToolJavaMajor(jar)
		if err != nil {
			return 0, fmt.Errorf("cannot inspect Hive Java requirement in %s: %w", jar, err)
		}
		if major > required {
			required = major
		}
	}
	return required, nil
}

func schemaToolJavaMajor(jar string) (int, error) {
	reader, err := zip.OpenReader(jar)
	if err != nil {
		return 0, err
	}
	defer func() { _ = reader.Close() }()
	required := 0
	for _, file := range reader.File {
		if file.Name != "org/apache/hive/beeline/schematool/HiveSchemaTool.class" &&
			file.Name != "org/apache/hadoop/hive/metastore/tools/schematool/HiveSchemaTool.class" {
			continue
		}
		class, err := file.Open()
		if err != nil {
			return 0, err
		}
		var header [8]byte
		_, err = io.ReadFull(class, header[:])
		_ = class.Close()
		if err != nil {
			return 0, err
		}
		classMajor := int(binary.BigEndian.Uint16(header[6:8]))
		if binary.BigEndian.Uint32(header[:4]) != 0xcafebabe || classMajor < 45 {
			return 0, fmt.Errorf("invalid HiveSchemaTool class header")
		}
		// Class-file major 45 corresponds to Java 1; major 61 to Java 17.
		if major := classMajor - 44; major > required {
			required = major
		}
	}
	return required, nil
}

func hiveJavaMismatch(major, required int) error {
	if major < required {
		return fmt.Errorf("hive Java mismatch: selected Java %d cannot load HiveSchemaTool (class-file major %d requires Java %d); install a compatible JDK and set JAVA_HOME, or use Hive built for Java %d", major, required+44, required, major)
	}
	return nil
}

// CheckHiveJava fails before launching schematool or Hive daemons when the
// selected JVM cannot load the installed schema tool. This dependency error is
// fatal even where database/schema failures normally degrade to warnings.
func CheckHiveJava(environment *Environment) error {
	required, err := HiveRequiredJavaMajor(environment.HiveHome)
	if err != nil || required == 0 {
		return err
	}
	major, err := selectedJavaMajor(environment)
	if err != nil {
		return fmt.Errorf("hive requires Java %d: %w", required, err)
	}
	return hiveJavaMismatch(major, required)
}
