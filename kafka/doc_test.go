package kafka

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
)

// headingIsFor reports whether a CHANGELOG section heading is the one for
// version: the hand-written form "## v1.2.0 (date)" or the generated form
// "## [1.2.0](compare link) (date)". A bare substring match would not do: the
// generated heading of the next release names this version in its compare
// link.
func headingIsFor(line, version string) bool {
	return strings.Contains(line, "["+version+"]") || strings.Contains(line, "v"+version+" ")
}

// The version lives in three places that a release updates together. A release
// PR whose files disagree fails this when CI is run on its branch, before the
// merge. After the merge it is too late to rely on it: the next green run on
// main tags the merged release commit whether its own run passed or not.
func TestVersion_MatchesManifestAndChangelog(t *testing.T) {
	for line, want := range map[string]bool{
		"## v1.2.0 (2026-11-13)": true,
		"## [1.2.0](https://example.com/compare/v1.1.0...v1.2.0) (2026-12-01)": true,
		"## [1.3.0](https://example.com/compare/v1.2.0...v1.3.0) (2026-12-01)": false,
	} {
		if got := headingIsFor(line, "1.2.0"); got != want {
			t.Errorf("headingIsFor(%q, 1.2.0) = %v, want %v", line, got, want)
		}
	}

	raw, err := os.ReadFile("../.release-please-manifest.json")
	if errors.Is(err, fs.ErrNotExist) {
		t.Skip("no release manifest next to the package: this is not a checkout of the repository")
	}
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest map[string]string
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if manifest["."] != Version {
		t.Errorf("manifest version = %q, Version = %q", manifest["."], Version)
	}

	changelog, err := os.ReadFile("../CHANGELOG.md")
	if err != nil {
		t.Fatalf("read changelog: %v", err)
	}
	for line := range strings.Lines(string(changelog)) {
		if strings.HasPrefix(line, "## ") {
			if !headingIsFor(line, Version) {
				t.Errorf("first CHANGELOG section is %q, want the one for %s", strings.TrimSpace(line), Version)
			}
			return
		}
	}
	t.Error("CHANGELOG.md has no version section")
}
