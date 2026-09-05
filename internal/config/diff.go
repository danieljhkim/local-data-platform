package config

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// OverlayDiff is the effective difference between the published overlay and the
// overlay profile set would activate for a named profile.
type OverlayDiff struct {
	Profile       string
	ActiveProfile string
	FilesAdded    []string
	FilesRemoved  []string
	FilesChanged  []string
	Properties    []FilePropertyDiff
}

// FilePropertyDiff is the property-level delta for one managed config file.
type FilePropertyDiff struct {
	Path    string
	Added   []PropertyDelta
	Removed []PropertyDelta
	Changed []PropertyDelta
}

// PropertyDelta is one added, removed, or changed configuration property.
// Values are already redacted.
type PropertyDelta struct {
	Name      string
	Current   string
	Candidate string
}

// Diff compares the published runtime overlay with the overlay `Set` would
// activate for profile. It materializes the candidate in an isolated temporary
// directory and never publishes the overlay, active-profile marker, or settings.
func (pm *ProfileManager) Diff(profile string) (*OverlayDiff, error) {
	var result *OverlayDiff
	err := withConfigLock(pm.paths, func() error {
		diff, err := pm.diffUnlocked(profile)
		result = diff
		return err
	})
	return result, err
}

func (pm *ProfileManager) diffUnlocked(profile string) (*OverlayDiff, error) {
	if profile == "" {
		return nil, fmt.Errorf("profile name required")
	}

	srcRoot := filepath.Join(pm.paths.UserProfilesDir(), profile)
	if !util.DirExists(srcRoot) {
		return nil, fmt.Errorf("unknown profile '%s' (expected: %s)", profile, srcRoot)
	}

	current := pm.paths.CurrentConfDir()
	if !util.DirExists(current) {
		return nil, fmt.Errorf("runtime conf overlay not found. Run: local-data profile set <name>")
	}

	dest, err := os.MkdirTemp("", "ldp-profile-diff-")
	if err != nil {
		return nil, fmt.Errorf("failed to create isolated overlay preview directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dest) }()

	if err := pm.materializeOverlay(srcRoot, dest, profile); err != nil {
		return nil, err
	}

	diff, err := compareOverlays(current, dest)
	if err != nil {
		return nil, err
	}
	diff.Profile = profile
	if active, activeErr := pm.paths.activeProfileUnlocked(); activeErr == nil {
		diff.ActiveProfile = active
	}
	return diff, nil
}

func compareOverlays(currentRoot, candidateRoot string) (*OverlayDiff, error) {
	currentFiles, err := listOverlayFiles(currentRoot)
	if err != nil {
		return nil, err
	}
	candidateFiles, err := listOverlayFiles(candidateRoot)
	if err != nil {
		return nil, err
	}

	currentSet := make(map[string]struct{}, len(currentFiles))
	for _, rel := range currentFiles {
		currentSet[rel] = struct{}{}
	}
	candidateSet := make(map[string]struct{}, len(candidateFiles))
	for _, rel := range candidateFiles {
		candidateSet[rel] = struct{}{}
	}

	all := make([]string, 0, len(currentSet)+len(candidateSet))
	seen := make(map[string]struct{}, len(currentSet)+len(candidateSet))
	for _, rel := range append(append([]string{}, currentFiles...), candidateFiles...) {
		if _, ok := seen[rel]; ok {
			continue
		}
		seen[rel] = struct{}{}
		all = append(all, rel)
	}
	sort.Strings(all)

	diff := &OverlayDiff{}
	for _, rel := range all {
		_, inCurrent := currentSet[rel]
		_, inCandidate := candidateSet[rel]
		currentPath := filepath.Join(currentRoot, filepath.FromSlash(rel))
		candidatePath := filepath.Join(candidateRoot, filepath.FromSlash(rel))

		switch {
		case inCandidate && !inCurrent:
			diff.FilesAdded = append(diff.FilesAdded, rel)
		case inCurrent && !inCandidate:
			diff.FilesRemoved = append(diff.FilesRemoved, rel)
		default:
			if isManagedConfig(rel) {
				props, err := diffManagedFile(rel, currentPath, candidatePath)
				if err != nil {
					return nil, err
				}
				if props != nil && !props.empty() {
					diff.Properties = append(diff.Properties, *props)
				}
				continue
			}
			equal, err := filesEqual(currentPath, candidatePath)
			if err != nil {
				return nil, err
			}
			if !equal {
				diff.FilesChanged = append(diff.FilesChanged, rel)
			}
		}
	}
	return diff, nil
}

func (d *OverlayDiff) empty() bool {
	return d == nil || (len(d.FilesAdded) == 0 && len(d.FilesRemoved) == 0 && len(d.FilesChanged) == 0 && len(d.Properties) == 0)
}

func (p *FilePropertyDiff) empty() bool {
	return p == nil || (len(p.Added) == 0 && len(p.Removed) == 0 && len(p.Changed) == 0)
}

// Format renders a deterministic, redacted preview.
func (d *OverlayDiff) Format() string {
	if d == nil || d.empty() {
		profile := "the requested profile"
		if d != nil && d.Profile != "" {
			profile = d.Profile
		}
		return fmt.Sprintf("No configuration differences for profile %q.\n", profile)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Preview of profile %q", d.Profile)
	if d.ActiveProfile != "" {
		fmt.Fprintf(&b, " (active overlay: %s)", d.ActiveProfile)
	}
	b.WriteString("\n")

	writePathList(&b, "Files added", d.FilesAdded)
	writePathList(&b, "Files removed", d.FilesRemoved)
	writePathList(&b, "Files changed", d.FilesChanged)

	for _, props := range d.Properties {
		fmt.Fprintf(&b, "\n%s\n", props.Path)
		for _, delta := range props.Added {
			fmt.Fprintf(&b, "  + %s = %s\n", delta.Name, delta.Candidate)
		}
		for _, delta := range props.Removed {
			fmt.Fprintf(&b, "  - %s = %s\n", delta.Name, delta.Current)
		}
		for _, delta := range props.Changed {
			fmt.Fprintf(&b, "  ~ %s\n", delta.Name)
			fmt.Fprintf(&b, "      current:   %s\n", delta.Current)
			fmt.Fprintf(&b, "      candidate: %s\n", delta.Candidate)
		}
	}
	return b.String()
}

func writePathList(b *strings.Builder, title string, paths []string) {
	if len(paths) == 0 {
		return
	}
	fmt.Fprintf(b, "\n%s:\n", title)
	for _, path := range paths {
		fmt.Fprintf(b, "  %s\n", path)
	}
}

func listOverlayFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list overlay files under %s: %w", root, err)
	}
	sort.Strings(files)
	return files, nil
}

func isManagedConfig(rel string) bool {
	base := filepath.Base(rel)
	if base == "spark-defaults.conf" {
		return true
	}
	return strings.HasSuffix(rel, ".xml")
}

func diffManagedFile(rel, currentPath, candidatePath string) (*FilePropertyDiff, error) {
	currentProps, err := loadManagedProperties(currentPath)
	if err != nil {
		return nil, err
	}
	candidateProps, err := loadManagedProperties(candidatePath)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(currentProps)+len(candidateProps))
	seen := make(map[string]struct{}, len(currentProps)+len(candidateProps))
	for name := range currentProps {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for name := range candidateProps {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)

	out := &FilePropertyDiff{Path: rel}
	for _, name := range names {
		currentVal, inCurrent := currentProps[name]
		candidateVal, inCandidate := candidateProps[name]
		switch {
		case inCandidate && !inCurrent:
			out.Added = append(out.Added, PropertyDelta{
				Name:      name,
				Candidate: redactPropertyValue(name, candidateVal),
			})
		case inCurrent && !inCandidate:
			out.Removed = append(out.Removed, PropertyDelta{
				Name:    name,
				Current: redactPropertyValue(name, currentVal),
			})
		case currentVal != candidateVal:
			out.Changed = append(out.Changed, PropertyDelta{
				Name:      name,
				Current:   redactPropertyValue(name, currentVal),
				Candidate: redactPropertyValue(name, candidateVal),
			})
		}
	}
	return out, nil
}

func loadManagedProperties(path string) (map[string]string, error) {
	if strings.HasSuffix(path, ".xml") || strings.HasSuffix(filepath.ToSlash(path), ".xml") {
		return loadHadoopXMLProperties(path)
	}
	if filepath.Base(path) == "spark-defaults.conf" {
		return loadSparkProperties(path)
	}
	return nil, fmt.Errorf("unsupported managed configuration: %s", path)
}

func loadHadoopXMLProperties(path string) (map[string]string, error) {
	cfg, err := util.ParseHadoopXML(path)
	if err != nil {
		return nil, fmt.Errorf("malformed Hadoop/Hive configuration %s: %w", path, err)
	}
	props := make(map[string]string, len(cfg.Properties))
	for _, prop := range cfg.Properties {
		name := strings.TrimSpace(prop.Name)
		if name == "" {
			continue
		}
		props[name] = prop.Value
	}
	return props, nil
}

func loadSparkProperties(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read Spark configuration %s: %w", path, err)
	}

	props := make(map[string]string)
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, " ")
		if !ok {
			key, value, ok = strings.Cut(line, "=")
			if !ok {
				return nil, fmt.Errorf("malformed Spark configuration %s:%d: %q", path, i+1, raw)
			}
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			return nil, fmt.Errorf("malformed Spark configuration %s:%d: %q", path, i+1, raw)
		}
		props[key] = value
	}
	return props, nil
}

func filesEqual(a, b string) (bool, error) {
	left, err := os.ReadFile(a)
	if err != nil {
		return false, fmt.Errorf("failed to read %s: %w", a, err)
	}
	right, err := os.ReadFile(b)
	if err != nil {
		return false, fmt.Errorf("failed to read %s: %w", b, err)
	}
	return bytes.Equal(left, right), nil
}

func redactPropertyValue(name, value string) string {
	if util.IsSensitivePropertyName(name) {
		return util.RedactedValue
	}
	return util.RedactJDBCURL(value)
}
