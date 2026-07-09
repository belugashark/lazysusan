package install

import (
	"strings"
	"testing"
)

func TestRenderPinnedVersion(t *testing.T) {
	s := Render(Config{Repo: "owner/repo", Version: "v1.2.3"})
	if !strings.Contains(s, `version="${SUSAN_VERSION:-v1.2.3}"`) {
		t.Errorf("pinned render missing version default:\n%s", s)
	}
	if !strings.Contains(s, `base="https://github.com/${repo}/releases/download/${version}"`) {
		t.Errorf("pinned render missing versioned download base:\n%s", s)
	}
}

func TestRenderLatest(t *testing.T) {
	s := Render(Config{Repo: "owner/repo", Version: "latest"})
	if !strings.Contains(s, `base="https://github.com/${repo}/releases/latest/download"`) {
		t.Errorf("latest render missing releases/latest base:\n%s", s)
	}
}
