package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

var errInjected = errors.New("test publish failure")

func initializedConfig(t *testing.T, profile string) (*Paths, *ProfileManager) {
	t.Helper()
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)
	if err := pm.Init(false, &generator.InitOptions{
		User:       "old-user",
		DBType:     "postgres",
		DBUrl:      "jdbc:postgresql://old-host:5432/old-db",
		DBPassword: "old-secret",
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set(profile); err != nil {
		t.Fatalf("set %s: %v", profile, err)
	}
	return paths, pm
}

func snapshotFiles(t *testing.T, paths []string) map[string]string {
	t.Helper()
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		result[path] = string(data)
	}
	return result
}

func assertNoTransactionDebris(t *testing.T, paths *Paths) {
	t.Helper()
	err := filepath.WalkDir(paths.BaseDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if strings.Contains(name, stageMarker) || strings.Contains(name, backupMarker) || strings.HasPrefix(name, transactionPrefix) {
			t.Errorf("transaction debris remains: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk transaction debris: %v", err)
	}
}

func TestProfileSetFailurePreservesPublishedProfile(t *testing.T) {
	points := []string{
		"overlay.copy",
		"overlay.spark-copy",
		"overlay.marker-write",
		"active-profile.write",
		"publish.current-overlay",
		"publish.active-profile",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			paths, pm := initializedConfig(t, "local")
			before := snapshotFiles(t, []string{
				paths.ActiveProfileFile(),
				filepath.Join(paths.CurrentConfDir(), ".profile"),
				filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
			})
			paths.testHook = func(got string) error {
				if got == point {
					return errInjected
				}
				return nil
			}

			if err := pm.Set("hdfs"); err == nil || !strings.Contains(err.Error(), point) {
				t.Fatalf("Set() error = %v, want injected %s failure", err, point)
			}
			paths.testHook = nil
			after := snapshotFiles(t, []string{
				paths.ActiveProfileFile(),
				filepath.Join(paths.CurrentConfDir(), ".profile"),
				filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
			})
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("published profile changed after %s failure", point)
			}
			if util.DirExists(paths.CurrentHadoopConf()) {
				t.Fatalf("partial hdfs overlay visible after %s failure", point)
			}
			assertNoTransactionDebris(t, paths)
		})
	}
}

func TestInitFailurePreservesProfilesSettingsAndOverlay(t *testing.T) {
	points := []string{
		"profiles.generate",
		"settings.write",
		"publish.profiles",
		"publish.current-overlay",
		"publish.settings",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			paths, pm := initializedConfig(t, "hdfs")
			files := []string{
				NewSettingsManager(paths).Path(),
				filepath.Join(paths.UserProfilesDir(), "hdfs", "hive", "hive-site.xml"),
				filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml"),
				filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"),
				filepath.Join(paths.CurrentSparkConf(), "hive-site.xml"),
			}
			before := snapshotFiles(t, files)
			paths.testHook = func(got string) error {
				if got == point {
					return errInjected
				}
				return nil
			}

			err := pm.Init(true, &generator.InitOptions{
				User:       "new-user",
				DBType:     "postgres",
				DBUrl:      "jdbc:postgresql://new-host:5432/new-db",
				DBPassword: "new-secret",
			})
			if err == nil || !strings.Contains(err.Error(), point) {
				t.Fatalf("Init() error = %v, want injected %s failure", err, point)
			}
			paths.testHook = nil
			if after := snapshotFiles(t, files); !reflect.DeepEqual(after, before) {
				t.Fatalf("configuration changed after %s failure", point)
			}
			assertNoTransactionDebris(t, paths)
		})
	}
}

func TestSettingFailureRollsBackSettingsAndEveryHiveXML(t *testing.T) {
	relativeTargets := []string{
		"conf/current/hive/hive-site.xml",
		"conf/current/spark/hive-site.xml",
		"conf/profiles/hdfs/hive/hive-site.xml",
		"conf/profiles/local/hive/hive-site.xml",
	}
	points := []string{"settings.write", "publish.settings"}
	for _, target := range relativeTargets {
		points = append(points, "xml.write:"+target, "publish.hive-xml:"+target)
	}

	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			paths, _ := initializedConfig(t, "hdfs")
			sm := NewSettingsManager(paths)
			files := []string{sm.Path()}
			for _, target := range relativeTargets {
				files = append(files, filepath.Join(paths.BaseDir, filepath.FromSlash(target)))
			}
			before := snapshotFiles(t, files)
			paths.testHook = func(got string) error {
				if got == point {
					return errInjected
				}
				return nil
			}

			err := sm.UpdateAndApply(func(settings *Settings) error {
				settings.User = "new-user"
				settings.DBURL = "jdbc:postgresql://new-host:5432/new-db"
				settings.DBPassword = "new-secret"
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), point) {
				t.Fatalf("UpdateAndApply() error = %v, want injected %s failure", err, point)
			}
			paths.testHook = nil
			if after := snapshotFiles(t, files); !reflect.DeepEqual(after, before) {
				t.Fatalf("settings transaction changed files after %s failure", point)
			}
			for _, path := range files {
				assertNotGroupOrWorldReadable(t, path)
			}
			assertNoTransactionDebris(t, paths)
		})
	}
}

func TestApplyActiveWaitsForProfileSetCommit(t *testing.T) {
	paths, pm := initializedConfig(t, "local")
	entered := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	paths.testHook = func(point string) error {
		if point == "publish.active-profile" && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}

	setDone := make(chan error, 1)
	go func() { setDone <- pm.Set("hdfs") }()
	<-entered

	type applyResult struct {
		profile string
		err     error
	}
	applyDone := make(chan applyResult, 1)
	go func() {
		profile, err := pm.ApplyActive()
		applyDone <- applyResult{profile: profile, err: err}
	}()
	select {
	case result := <-applyDone:
		t.Fatalf("reader escaped transaction lock with profile %q and error %v", result.profile, result.err)
	case <-time.After(75 * time.Millisecond):
	}
	close(release)
	if err := <-setDone; err != nil {
		t.Fatalf("Set(): %v", err)
	}
	result := <-applyDone
	if result.err != nil || result.profile != "hdfs" {
		t.Fatalf("ApplyActive() = %q, %v; want hdfs", result.profile, result.err)
	}
}

func TestSameBaseConfigurationContentionIsBounded(t *testing.T) {
	paths, pm := initializedConfig(t, "local")
	paths.lockTimeout = 50 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	paths.testHook = func(point string) error {
		if point == "lock.acquired" && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- pm.Apply("local") }()
	<-entered
	started := time.Now()
	err := pm.Set("hdfs")
	if err == nil || !strings.Contains(err.Error(), "configuration lock contention") {
		t.Fatalf("Set() error = %v, want bounded contention", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("contention error was not bounded: %s", elapsed)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Apply(): %v", err)
	}
}

func TestDifferentBaseDirectoriesRemainConcurrent(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		paths := NewPaths("", filepath.Join(t.TempDir(), "base"))
		paths.testHook = func(point string) error {
			if point == "lock.acquired" {
				entered <- struct{}{}
				<-release
			}
			return nil
		}
		wg.Add(1)
		go func(paths *Paths) {
			defer wg.Done()
			errs <- NewSettingsManager(paths).Save(defaultSettings(paths.BaseDir))
		}(paths)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("different base directories did not acquire locks concurrently")
		}
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Save(): %v", err)
		}
	}
}

func TestAbandonedTransactionRollsBackAndCleansStaging(t *testing.T) {
	paths := NewPaths("", filepath.Join(t.TempDir(), "base"))
	if err := os.MkdirAll(paths.SettingsDir(), 0755); err != nil {
		t.Fatal(err)
	}
	target := paths.SettingsFile()
	if err := util.WriteFile(target, []byte("old"), util.PrivateFileMode); err != nil {
		t.Fatal(err)
	}
	transactionID := newTransactionID()
	entry := newStagedReplacement(target, "settings", transactionID)
	entry.HadOriginal = true
	if err := util.WriteFile(entry.Stage, []byte("new"), util.PrivateFileMode); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(paths.BaseDir, transactionPrefix+transactionID+".json")
	if err := writeManifest(manifestPath, transactionManifest{Entries: []stagedReplacement{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(target, entry.Backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(entry.Stage, target); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(paths.SettingsDir(), ".orphan"+stageMarker+"test")
	if err := os.WriteFile(orphan, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := withConfigLock(paths, func() error { return nil }); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old" {
		t.Fatalf("recovered settings = %q, want old", data)
	}
	assertNoTransactionDebris(t, paths)
}
