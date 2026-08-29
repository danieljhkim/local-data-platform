package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	defaultConfigLockTimeout = 5 * time.Second
	configLockRetryInterval  = 10 * time.Millisecond
	transactionPrefix        = ".ldp-config-transaction-"
	stageMarker              = ".ldp-stage-"
	backupMarker             = ".ldp-backup-"
)

var transactionSequence atomic.Uint64

type stagedReplacement struct {
	Target      string `json:"target"`
	Stage       string `json:"stage"`
	Backup      string `json:"backup"`
	Point       string `json:"-"`
	HadOriginal bool   `json:"had_original"`
}

type transactionManifest struct {
	Entries []stagedReplacement `json:"entries"`
}

func (p *Paths) runConfigHook(point string) error {
	if p.testHook == nil {
		return nil
	}
	if err := p.testHook(point); err != nil {
		return fmt.Errorf("injected failure at %s: %w", point, err)
	}
	return nil
}

func withConfigLock(paths *Paths, fn func() error) error {
	if err := os.MkdirAll(paths.BaseDir, 0700); err != nil {
		return fmt.Errorf("failed to create configuration base directory: %w", err)
	}

	lockPath := filepath.Join(paths.BaseDir, ".ldp-config.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, util.PrivateFileMode)
	if err != nil {
		return fmt.Errorf("failed to open configuration lock: %w", err)
	}
	defer lockFile.Close()
	if err := os.Chmod(lockPath, util.PrivateFileMode); err != nil {
		return fmt.Errorf("failed to secure configuration lock: %w", err)
	}

	timeout := paths.lockTimeout
	if timeout <= 0 {
		timeout = defaultConfigLockTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("failed to acquire configuration lock for %s: %w", paths.BaseDir, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("configuration lock contention for %s exceeded %s", paths.BaseDir, timeout)
		}
		time.Sleep(configLockRetryInterval)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN) //nolint:errcheck

	if err := recoverConfigTransactions(paths); err != nil {
		return err
	}
	if err := paths.runConfigHook("lock.acquired"); err != nil {
		return err
	}
	return fn()
}

func newTransactionID() string {
	return fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), transactionSequence.Add(1))
}

func newStagedReplacement(target, point, transactionID string) stagedReplacement {
	base := filepath.Base(target)
	parent := filepath.Dir(target)
	return stagedReplacement{
		Target: target,
		Stage:  filepath.Join(parent, "."+base+stageMarker+transactionID),
		Backup: filepath.Join(parent, "."+base+backupMarker+transactionID),
		Point:  point,
	}
}

func publishStaged(paths *Paths, transactionID string, entries []stagedReplacement) error {
	if len(entries) == 0 {
		return nil
	}
	cleanupStages := func() {
		for _, entry := range entries {
			_ = os.RemoveAll(entry.Stage)
		}
	}
	for i := range entries {
		if _, err := os.Lstat(entries[i].Stage); err != nil {
			cleanupStages()
			return fmt.Errorf("missing staged replacement for %s: %w", entries[i].Target, err)
		}
		_, err := os.Lstat(entries[i].Target)
		entries[i].HadOriginal = err == nil
		if err != nil && !os.IsNotExist(err) {
			cleanupStages()
			return fmt.Errorf("failed to inspect replacement target %s: %w", entries[i].Target, err)
		}
	}

	manifestPath := filepath.Join(paths.BaseDir, transactionPrefix+transactionID+".json")
	if err := writeManifest(manifestPath, transactionManifest{Entries: entries}); err != nil {
		cleanupStages()
		return err
	}

	rollback := func(cause error) error {
		if rollbackErr := rollbackTransaction(manifestPath, entries); rollbackErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", cause, rollbackErr)
		}
		return cause
	}

	for _, entry := range entries {
		if err := paths.runConfigHook("publish." + entry.Point); err != nil {
			return rollback(err)
		}
		if entry.HadOriginal {
			if err := os.Rename(entry.Target, entry.Backup); err != nil {
				return rollback(fmt.Errorf("failed to preserve %s: %w", entry.Target, err))
			}
		}
		if err := os.Rename(entry.Stage, entry.Target); err != nil {
			return rollback(fmt.Errorf("failed to publish %s: %w", entry.Target, err))
		}
		if err := syncDir(filepath.Dir(entry.Target)); err != nil {
			return rollback(fmt.Errorf("failed to sync published configuration: %w", err))
		}
	}

	// Removing the manifest is the commit point. Backups deliberately remain
	// until after it so a crash before this point is recoverable.
	if err := os.Remove(manifestPath); err != nil {
		return rollback(fmt.Errorf("failed to commit configuration transaction: %w", err))
	}
	// The state is committed once the manifest is removed. A directory-sync
	// failure cannot be reported as a rolled-back mutation at this point.
	_ = syncDir(paths.BaseDir)
	for _, entry := range entries {
		_ = os.RemoveAll(entry.Backup)
	}
	return nil
}

func writeManifest(path string, manifest transactionManifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal configuration transaction: %w", err)
	}
	tmp := path + ".tmp"
	if err := util.WriteFile(tmp, append(data, '\n'), util.PrivateFileMode); err != nil {
		return fmt.Errorf("failed to write configuration transaction: %w", err)
	}
	if err := syncFile(tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to publish configuration transaction: %w", err)
	}
	return syncDir(filepath.Dir(path))
}

func rollbackTransaction(manifestPath string, entries []stagedReplacement) error {
	var rollbackErrors []error
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		backupExists := pathExists(entry.Backup)
		stageExists := pathExists(entry.Stage)

		switch {
		case backupExists:
			if err := os.RemoveAll(entry.Target); err != nil {
				rollbackErrors = append(rollbackErrors, err)
				continue
			}
			if err := os.Rename(entry.Backup, entry.Target); err != nil {
				rollbackErrors = append(rollbackErrors, err)
			}
		case !entry.HadOriginal && !stageExists:
			if err := os.RemoveAll(entry.Target); err != nil {
				rollbackErrors = append(rollbackErrors, err)
			}
		}
		if err := os.RemoveAll(entry.Stage); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	if len(rollbackErrors) == 0 {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	return errors.Join(rollbackErrors...)
}

func recoverConfigTransactions(paths *Paths) error {
	manifests, err := filepath.Glob(filepath.Join(paths.BaseDir, transactionPrefix+"*.json"))
	if err != nil {
		return fmt.Errorf("failed to scan configuration transactions: %w", err)
	}
	for _, manifestPath := range manifests {
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return fmt.Errorf("failed to read abandoned configuration transaction: %w", err)
		}
		var manifest transactionManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return fmt.Errorf("failed to parse abandoned configuration transaction %s: %w", manifestPath, err)
		}
		if err := rollbackTransaction(manifestPath, manifest.Entries); err != nil {
			return fmt.Errorf("failed to recover abandoned configuration transaction %s: %w", manifestPath, err)
		}
	}

	for _, root := range []string{paths.ConfRootDir(), paths.SettingsDir()} {
		if err := removeOrphanedTransactionPaths(root); err != nil {
			return err
		}
	}
	matches, _ := filepath.Glob(filepath.Join(paths.BaseDir, transactionPrefix+"*.tmp"))
	for _, match := range matches {
		_ = os.Remove(match)
	}
	return nil
}

func removeOrphanedTransactionPaths(root string) error {
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if path == root {
			return nil
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") && (strings.Contains(name, stageMarker) || strings.Contains(name, backupMarker)) {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			if entry.IsDir() {
				return filepath.SkipDir
			}
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clean abandoned configuration staging under %s: %w", root, err)
	}
	return nil
}

func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}
