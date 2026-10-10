package hive

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

func TestHiveJavaPreflightBeforeBootstrapAndStart(t *testing.T) {
	for _, tc := range []struct {
		classMajor uint16
		javaMajor  int
	}{
		{61, 17}, {65, 17}, {65, 21},
	} {
		for _, operation := range []string{"bootstrap", "start", "schema non-strict"} {
			t.Run(fmt.Sprintf("class%d/java%d/%s", tc.classMajor, tc.javaMajor, operation), func(t *testing.T) {
				root := t.TempDir()
				hiveHome := filepath.Join(root, "hive")
				javaHome := filepath.Join(root, "jdk")
				for _, dir := range []string{filepath.Join(hiveHome, "lib"), filepath.Join(hiveHome, "bin"), filepath.Join(javaHome, "bin")} {
					if err := os.MkdirAll(dir, 0755); err != nil {
						t.Fatal(err)
					}
				}
				var jar bytes.Buffer
				writer := zip.NewWriter(&jar)
				class, err := writer.Create("org/apache/hive/beeline/schematool/HiveSchemaTool.class")
				if err != nil {
					t.Fatal(err)
				}
				header := []byte{0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 0}
				binary.BigEndian.PutUint16(header[6:], tc.classMajor)
				if _, err := class.Write(header); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(hiveHome, "lib", "hive-beeline-test.jar"), jar.Bytes(), 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(javaHome, "bin", "java"), []byte(fmt.Sprintf("#!/bin/sh\necho 'openjdk version \"%d.0.1\"' >&2\n", tc.javaMajor)), 0755); err != nil {
					t.Fatal(err)
				}
				marker := filepath.Join(root, "schematool-launched")
				script := "#!/bin/sh\nprintf '%s' \"$JAVA_HOME\" > '" + marker + "'\necho 'Metastore schema version: test'\n"
				if err := os.WriteFile(filepath.Join(hiveHome, "bin", "schematool"), []byte(script), 0755); err != nil {
					t.Fatal(err)
				}
				paths := config.NewPaths(root, filepath.Join(root, "base"))
				if err := writeTestHiveSite(paths.CurrentHiveConf(), util.HadoopProperty{Name: "javax.jdo.option.ConnectionURL", Value: "jdbc:derby:;databaseName=" + filepath.Join(root, "db") + ";create=true"}); err != nil {
					t.Fatal(err)
				}
				svc, err := newHiveServiceWithEnv(paths, &env.Environment{JavaHome: javaHome, HiveHome: hiveHome, HiveConfDir: paths.CurrentHiveConf(), Path: filepath.Join(hiveHome, "bin")})
				if err != nil {
					t.Fatal(err)
				}
				launched := false
				svc.startMetastoreHook = func(context.Context) (bool, error) { launched = true; return true, nil }
				svc.startHiveServer2Hook = func(context.Context) (bool, error) { return true, nil }
				svc.waitMetastoreHook = func(context.Context) error { return nil }
				svc.waitHiveServer2Hook = func(context.Context) error { return nil }
				switch operation {
				case "bootstrap":
					err = svc.BootstrapMetastore(strings.NewReader(""), io.Discard, io.Discard)
				case "start":
					_, err = svc.StartContext(context.Background())
				case "schema non-strict":
					err = svc.ensureMetastoreSchemaForType(context.Background(), metastore.Derby, false)
				}
				if tc.javaMajor < int(tc.classMajor)-44 {
					if err == nil || !strings.Contains(err.Error(), "selected Java 17") || !strings.Contains(err.Error(), "requires Java 21") {
						t.Fatalf("expected mismatch, got %v", err)
					}
					if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
						t.Fatalf("schematool launched: %v", statErr)
					}
					if launched {
						t.Fatal("daemon launched despite incompatible Java")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					got, err := os.ReadFile(marker)
					if err != nil || string(got) != javaHome {
						t.Fatalf("schematool did not use selected JAVA_HOME: %q, %v", got, err)
					}
					if operation == "start" && !launched {
						t.Fatal("compatible startup did not launch")
					}
				}
			})
		}
	}
}
