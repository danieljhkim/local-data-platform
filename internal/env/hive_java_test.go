package env

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeHiveClassFixture(t *testing.T, home, jarName, className string, header []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "lib"), 0755); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	writer := zip.NewWriter(&data)
	class, err := writer.Create(className)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := class.Write(header); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "lib", jarName), data.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

func hiveClassHeader(major uint16) []byte {
	header := []byte{0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(header[6:], major)
	return header
}

func javaHomeFixture(t *testing.T, version string) string {
	t.Helper()
	home := t.TempDir()
	bin := filepath.Join(home, "bin")
	if err := os.MkdirAll(bin, 0755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, bin, "java", fmt.Sprintf("#!/bin/sh\necho 'openjdk version \"%s\"' >&2\n", version))
	return home
}

func TestHiveRequiredJavaMajor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		header  []byte
		want    int
		wantErr bool
	}{
		{"Java17", hiveClassHeader(61), 17, false},
		{"Java21", hiveClassHeader(65), 21, false},
		{"truncated", []byte{0xca, 0xfe}, 0, true},
		{"invalid magic", make([]byte, 8), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			writeHiveClassFixture(t, home, "hive-beeline-test.jar", "org/apache/hive/beeline/schematool/HiveSchemaTool.class", tc.header)
			got, err := HiveRequiredJavaMajor(home)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("HiveRequiredJavaMajor() = %d, %v; want %d, error=%t", got, err, tc.want, tc.wantErr)
			}
		})
	}

	t.Run("older package and multiple jars", func(t *testing.T) {
		home := t.TempDir()
		writeHiveClassFixture(t, home, "hive-beeline-old.jar", "org/apache/hive/beeline/schematool/HiveSchemaTool.class", hiveClassHeader(61))
		writeHiveClassFixture(t, home, "hive-standalone-metastore-test.jar", "org/apache/hadoop/hive/metastore/tools/schematool/HiveSchemaTool.class", hiveClassHeader(65))
		if got, err := HiveRequiredJavaMajor(home); err != nil || got != 21 {
			t.Fatalf("requirement = %d, %v; want 21", got, err)
		}
	})
	t.Run("missing class remains unknown", func(t *testing.T) {
		home := t.TempDir()
		writeHiveClassFixture(t, home, "hive-beeline-test.jar", "unrelated.class", hiveClassHeader(65))
		if got, err := HiveRequiredJavaMajor(home); err != nil || got != 0 {
			t.Fatalf("requirement = %d, %v; want unknown", got, err)
		}
	})
}

func TestJavaDetectorMeasuresSelectedRuntime(t *testing.T) {
	for _, version := range []string{"17.0.1", "21.0.2", "1.8.0_402", "21-ea"} {
		t.Run(version, func(t *testing.T) {
			home := javaHomeFixture(t, version)
			t.Setenv("JAVA_HOME", home)
			// A conflicting Java on the parent PATH must not affect selection.
			t.Setenv("PATH", filepath.Join(javaHomeFixture(t, "11.0.1"), "bin"))
			major, err := selectedJavaMajor(&Environment{JavaHome: home})
			if err != nil || major == 0 {
				t.Fatalf("selected runtime = %d, %v", major, err)
			}
			want := 17
			if strings.HasPrefix(version, "21") {
				want = 21
			}
			if strings.HasPrefix(version, "1.8") {
				want = 8
			}
			if got := NewJavaDetector().MajorVersion(); got != want || NewJavaDetector().FindJavaHome() != home {
				t.Fatalf("detector = %d; want %d and explicit home", got, want)
			}
		})
	}
	t.Run("invalid explicit home does not fall back", func(t *testing.T) {
		t.Setenv("JAVA_HOME", filepath.Join(t.TempDir(), "missing"))
		t.Setenv("PATH", filepath.Join(javaHomeFixture(t, "21.0.2"), "bin"))
		if got := NewJavaDetector().MajorVersion(); got != 0 {
			t.Fatalf("invalid selected home reported Java %d", got)
		}
	})
}

func TestFindJavaHomeAutoSelectsHiveCompatibleJDK(t *testing.T) {
	setup := func(t *testing.T, hiveMajor uint16, jdks map[string]string) (prefix, hiveHome string) {
		t.Helper()
		prefix = t.TempDir()
		for name, version := range jdks {
			home := javaHomeFixture(t, version)
			if err := os.MkdirAll(filepath.Join(prefix, "opt"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(home, filepath.Join(prefix, "opt", name)); err != nil {
				t.Fatal(err)
			}
		}
		hiveHome = t.TempDir()
		writeHiveClassFixture(t, hiveHome, "hive-beeline-test.jar", "org/apache/hive/beeline/schematool/HiveSchemaTool.class", hiveClassHeader(hiveMajor))
		old := homebrewPrefixes
		homebrewPrefixes = []string{prefix}
		t.Cleanup(func() { homebrewPrefixes = old })
		t.Setenv("JAVA_HOME", "")
		t.Setenv("PATH", t.TempDir())
		return prefix, hiveHome
	}

	t.Run("Hive needing Java 21 skips the Java 17 default", func(t *testing.T) {
		prefix, hive := setup(t, 65, map[string]string{"openjdk@17": "17.0.1", "openjdk@21": "21.0.2"})
		got := NewJavaDetector().FindJavaHomeForHive(hive)
		if want := filepath.Join(prefix, "opt", "openjdk@21"); got != want {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, want)
		}
	})
	t.Run("lowest compatible version wins", func(t *testing.T) {
		prefix, hive := setup(t, 65, map[string]string{"openjdk@25": "25.0.1", "openjdk@21": "21.0.2", "openjdk@17": "17.0.1"})
		got := NewJavaDetector().FindJavaHomeForHive(hive)
		if want := filepath.Join(prefix, "opt", "openjdk@21"); got != want {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, want)
		}
	})
	t.Run("Java 17 default kept when Hive accepts it", func(t *testing.T) {
		prefix, hive := setup(t, 61, map[string]string{"openjdk@17": "17.0.1", "openjdk@21": "21.0.2"})
		got := NewJavaDetector().FindJavaHomeForHive(hive)
		if want := filepath.Join(prefix, "opt", "openjdk@17"); got != want {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, want)
		}
	})
	t.Run("unknown requirement keeps Java 17 default", func(t *testing.T) {
		prefix, _ := setup(t, 65, map[string]string{"openjdk@17": "17.0.1", "openjdk@21": "21.0.2"})
		got := NewJavaDetector().FindJavaHomeForHive("")
		if want := filepath.Join(prefix, "opt", "openjdk@17"); got != want {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, want)
		}
	})
	t.Run("only Java 17 installed fails closed on the default", func(t *testing.T) {
		prefix, hive := setup(t, 65, map[string]string{"openjdk@17": "17.0.1"})
		got := NewJavaDetector().FindJavaHomeForHive(hive)
		if want := filepath.Join(prefix, "opt", "openjdk@17"); got != want {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, want)
		}
		if err := CheckHiveJava(&Environment{JavaHome: got, HiveHome: hive, Path: os.Getenv("PATH")}); err == nil {
			t.Fatal("expected Java mismatch error")
		}
	})
	t.Run("java on PATH is used when no Homebrew JDK fits", func(t *testing.T) {
		_, hive := setup(t, 65, map[string]string{"openjdk@17": "17.0.1"})
		pathHome := javaHomeFixture(t, "21.0.2")
		t.Setenv("PATH", filepath.Join(pathHome, "bin"))
		resolved, err := filepath.EvalSymlinks(pathHome)
		if err != nil {
			t.Fatal(err)
		}
		if got := NewJavaDetector().FindJavaHomeForHive(hive); got != resolved {
			t.Fatalf("FindJavaHomeForHive() = %q; want %q", got, resolved)
		}
	})
	t.Run("explicit JAVA_HOME is never replaced", func(t *testing.T) {
		_, hive := setup(t, 65, map[string]string{"openjdk@21": "21.0.2"})
		explicit := javaHomeFixture(t, "17.0.1")
		t.Setenv("JAVA_HOME", explicit)
		if got := NewJavaDetector().FindJavaHomeForHive(hive); got != explicit {
			t.Fatalf("FindJavaHomeForHive() = %q; want explicit %q", got, explicit)
		}
	})
}
