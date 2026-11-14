package kafka

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The version lives in three places that a release updates together. A release
// PR whose files disagree fails this when CI is run on its branch, before the
// merge. After the merge it is too late to rely on it: the next green run on
// main tags the merged release commit whether its own run passed or not.
func TestVersion_MatchesManifestAndChangelog(t *testing.T) {
	raw, err := os.ReadFile("../.release-please-manifest.json")
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
			if !strings.Contains(line, Version) {
				t.Errorf("first CHANGELOG section is %q, want the one for %s", strings.TrimSpace(line), Version)
			}
			return
		}
	}
	t.Error("CHANGELOG.md has no version section")
}
