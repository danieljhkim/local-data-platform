package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	sentinelCurrent   = "sentinel-secret-CURRENT"
	sentinelCandidate = "sentinel-secret-CANDIDATE"
)

func overlayTree(t *testing.T, root string) map[string]string {
	t.Helper()
	files, err := listOverlayFiles(root)
	if err != nil {
		t.Fatalf("list overlay %s: %v", root, err)
	}
	out := make(map[string]string, len(files))
	for _, rel := range files {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		out[rel] = string(data)
	}
	return out
}

func TestProfileDiffUnchangedProfile(t *testing.T) {
	_, pm := initializedConfig(t, "local")

	diff, err := pm.Diff("local")
	if err != nil {
		t.Fatalf("Diff(local): %v", err)
	}
	if !diff.empty() {
		t.Fatalf("expected no differences for the active profile:\n%s", diff.Format())
	}
	if got := diff.Format(); !strings.Contains(got, `No configuration differences for profile "local"`) {
		t.Fatalf("unexpected empty diff output:\n%s", got)
	}
}

func TestProfileDiffIgnoresPropertyReorder(t *testing.T) {
	paths, pm := initializedConfig(t, "local")

	hivePath := filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")
	cfg, err := util.ParseHadoopXML(hivePath)
	if err != nil {
		t.Fatalf("parse hive-site: %v", err)
	}
	if len(cfg.Properties) < 2 {
		t.Fatalf("need at least 2 hive properties to reorder, got %d", len(cfg.Properties))
	}
	reversed := make([]util.HadoopProperty, len(cfg.Properties))
	for i, prop := range cfg.Properties {
		reversed[len(cfg.Properties)-1-i] = prop
	}
	cfg.Properties = reversed
	if err := cfg.WriteXML(hivePath); err != nil {
		t.Fatalf("rewrite hive-site: %v", err)
	}

	sparkPath := filepath.Join(paths.CurrentSparkConf(), "spark-defaults.conf")
	data, err := os.ReadFile(sparkPath)
	if err != nil {
		t.Fatalf("read spark-defaults: %v", err)
	}
	var comments, props []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			comments = append(comments, line)
			continue
		}
		props = append(props, line)
	}
	if len(props) < 2 {
		t.Fatalf("need at least 2 spark properties to reorder, got %d", len(props))
	}
	for i, j := 0, len(props)-1; i < j; i, j = i+1, j-1 {
		props[i], props[j] = props[j], props[i]
	}
	rewritten := strings.Join(append(comments, props...), "\n")
	if !strings.HasSuffix(rewritten, "\n") {
		rewritten += "\n"
	}
	if err := os.WriteFile(sparkPath, []byte(rewritten), 0644); err != nil {
		t.Fatalf("rewrite spark-defaults: %v", err)
	}

	diff, err := pm.Diff("local")
	if err != nil {
		t.Fatalf("Diff(local): %v", err)
	}
	if !diff.empty() {
		t.Fatalf("property reorder should not count as a change:\n%s", diff.Format())
	}
}

func TestProfileDiffReportsAddedAndRemovedFiles(t *testing.T) {
	_, pm := initializedConfig(t, "local")

	diff, err := pm.Diff("hdfs")
	if err != nil {
		t.Fatalf("Diff(hdfs): %v", err)
	}
	if diff.empty() {
		t.Fatal("expected local vs hdfs differences")
	}

	added := strings.Join(diff.FilesAdded, "\n")
	for _, want := range []string{
		"hadoop/core-site.xml",
		"hadoop/hdfs-site.xml",
		"hadoop/mapred-site.xml",
		"hadoop/yarn-site.xml",
	} {
		if !strings.Contains(added, want) {
			t.Fatalf("FilesAdded missing %s:\n%s", want, diff.Format())
		}
	}

	foundWarehouse := false
	for _, props := range diff.Properties {
		if props.Path != "hive/hive-site.xml" {
			continue
		}
		for _, change := range props.Changed {
			if change.Name == "hive.metastore.warehouse.dir" {
				foundWarehouse = true
			}
		}
	}
	if !foundWarehouse {
		t.Fatalf("expected hive warehouse property change:\n%s", diff.Format())
	}

	first := diff.Format()
	second, err := pm.Diff("hdfs")
	if err != nil {
		t.Fatalf("second Diff(hdfs): %v", err)
	}
	if first != second.Format() {
		t.Fatalf("diff output was not deterministic\nfirst:\n%s\nsecond:\n%s", first, second.Format())
	}
}

func TestProfileDiffCandidateMatchesSet(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)

	opts := &generator.InitOptions{
		User:       "daniel",
		DBType:     "postgres",
		DBUrl:      "jdbc:postgresql://alice:preview-pass@localhost:5432/metastore",
		DBPassword: "init-secret",
	}
	if err := pm.Init(false, opts); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	dest := t.TempDir()
	src := filepath.Join(paths.UserProfilesDir(), "hdfs")
	if err := pm.materializeOverlay(src, dest, "hdfs"); err != nil {
		t.Fatalf("materialize candidate: %v", err)
	}

	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("set hdfs: %v", err)
	}

	want := overlayTree(t, dest)
	got := overlayTree(t, paths.CurrentConfDir())
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("candidate overlay does not match profile set\nwant keys %d got keys %d", len(want), len(got))
	}
}

func TestProfileDiffDoesNotMutatePublishedState(t *testing.T) {
	paths, pm := initializedConfig(t, "local")
	settingsPath := NewSettingsManager(paths).Path()
	watch := []string{
		paths.ActiveProfileFile(),
		filepath.Join(paths.CurrentConfDir(), ".profile"),
		filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
		settingsPath,
	}
	beforeFiles := snapshotFiles(t, watch)
	beforeTree := overlayTree(t, paths.CurrentConfDir())
	stateExisted := util.DirExists(paths.StateDir())

	diff, err := pm.Diff("hdfs")
	if err != nil {
		t.Fatalf("Diff(hdfs): %v", err)
	}
	if diff.empty() {
		t.Fatal("expected differences between local and hdfs")
	}

	afterFiles := snapshotFiles(t, watch)
	if !reflect.DeepEqual(beforeFiles, afterFiles) {
		t.Fatalf("preview mutated published files")
	}
	afterTree := overlayTree(t, paths.CurrentConfDir())
	if !reflect.DeepEqual(beforeTree, afterTree) {
		t.Fatalf("preview mutated the runtime overlay")
	}
	assertNoTransactionDebris(t, paths)
	if !stateExisted && util.DirExists(paths.StateDir()) {
		t.Fatal("preview created service state")
	}
}

func TestProfileDiffFailureCleansTempAndLeavesOverlay(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("TMPDIR", tmpHome)

	paths, pm := initializedConfig(t, "local")
	before := snapshotFiles(t, []string{
		paths.ActiveProfileFile(),
		filepath.Join(paths.CurrentConfDir(), ".profile"),
		filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
		NewSettingsManager(paths).Path(),
	})
	beforeTree := overlayTree(t, paths.CurrentConfDir())

	paths.testHook = func(got string) error {
		if got == "overlay.copy" {
			return errInjected
		}
		return nil
	}

	_, err := pm.Diff("hdfs")
	if err == nil || !strings.Contains(err.Error(), "overlay.copy") {
		t.Fatalf("Diff() error = %v, want injected overlay.copy failure", err)
	}

	matches, globErr := filepath.Glob(filepath.Join(tmpHome, "ldp-profile-diff-*"))
	if globErr != nil {
		t.Fatalf("glob temp preview dirs: %v", globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("preview temp directories were not cleaned: %v", matches)
	}
	assertNoTransactionDebris(t, paths)

	after := snapshotFiles(t, []string{
		paths.ActiveProfileFile(),
		filepath.Join(paths.CurrentConfDir(), ".profile"),
		filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
		NewSettingsManager(paths).Path(),
	})
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed preview mutated published configuration")
	}
	if !reflect.DeepEqual(beforeTree, overlayTree(t, paths.CurrentConfDir())) {
		t.Fatal("failed preview mutated the runtime overlay")
	}
}

func TestProfileDiffRedactsSecrets(t *testing.T) {
	paths, pm := initializedConfig(t, "local")

	currentHive := filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")
	if err := rewriteSecrets(currentHive, sentinelCurrent); err != nil {
		t.Fatalf("rewrite current hive-site: %v", err)
	}
	sparkHive := filepath.Join(paths.CurrentSparkConf(), "hive-site.xml")
	if err := rewriteSecrets(sparkHive, sentinelCurrent); err != nil {
		t.Fatalf("rewrite current spark hive-site: %v", err)
	}

	candidateHive := filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml")
	if err := rewriteSecrets(candidateHive, sentinelCandidate); err != nil {
		t.Fatalf("rewrite candidate hive-site: %v", err)
	}

	diff, err := pm.Diff("local")
	if err != nil {
		t.Fatalf("Diff(local): %v", err)
	}
	out := diff.Format()
	assertNoSentinel(t, out)
	if !strings.Contains(out, util.RedactedValue) {
		t.Fatalf("expected redacted values in output:\n%s", out)
	}

	foundPassword := false
	foundURL := false
	for _, props := range diff.Properties {
		for _, change := range props.Changed {
			if change.Name == util.HiveConnectionPasswordProperty {
				foundPassword = true
				if change.Current != util.RedactedValue || change.Candidate != util.RedactedValue {
					t.Fatalf("password sides were not redacted: %+v", change)
				}
			}
			if change.Name == "javax.jdo.option.ConnectionURL" {
				foundURL = true
				if strings.Contains(change.Current, sentinelCurrent) || strings.Contains(change.Candidate, sentinelCandidate) {
					t.Fatalf("URL sides leaked sentinel: %+v", change)
				}
				if !strings.Contains(change.Current, util.RedactedValue) || !strings.Contains(change.Candidate, util.RedactedValue) {
					t.Fatalf("URL sides were not redacted: %+v", change)
				}
			}
		}
	}
	if !foundPassword || !foundURL {
		t.Fatalf("expected password and URL changes, got:\n%s", out)
	}
}

func TestProfileDiffUnknownProfile(t *testing.T) {
	_, pm := initializedConfig(t, "local")
	_, err := pm.Diff("does-not-exist")
	if err == nil {
		t.Fatal("expected unknown profile error")
	}
	if !strings.Contains(err.Error(), "unknown profile") {
		t.Fatalf("error = %v", err)
	}
}

func TestProfileDiffMissingOverlay(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}

	_, err := pm.Diff("local")
	if err == nil {
		t.Fatal("expected missing overlay error")
	}
	if !strings.Contains(err.Error(), "runtime conf overlay not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestProfileDiffMalformedManagedConfig(t *testing.T) {
	paths, pm := initializedConfig(t, "local")
	hivePath := filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")
	if err := os.WriteFile(hivePath, []byte("not-xml"), 0644); err != nil {
		t.Fatalf("write malformed hive-site: %v", err)
	}

	_, err := pm.Diff("local")
	if err == nil {
		t.Fatal("expected malformed configuration error")
	}
	if !strings.Contains(err.Error(), hivePath) {
		t.Fatalf("error should name the malformed file: %v", err)
	}
	if !strings.Contains(err.Error(), "malformed Hadoop/Hive configuration") {
		t.Fatalf("error = %v", err)
	}
}

func rewriteSecrets(path, secret string) error {
	cfg, err := util.ParseHadoopXML(path)
	if err != nil {
		return err
	}
	cfg.SetProperty(util.HiveConnectionPasswordProperty, secret)
	cfg.SetProperty("javax.jdo.option.ConnectionURL", "jdbc:postgresql://alice:"+secret+"@localhost:5432/metastore")
	return cfg.WriteXML(path)
}

func assertNoSentinel(t *testing.T, text string) {
	t.Helper()
	for _, secret := range []string{sentinelCurrent, sentinelCandidate} {
		if strings.Contains(text, secret) {
			t.Fatalf("output leaked %s:\n%s", secret, text)
		}
	}
}
