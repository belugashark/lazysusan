// Package install renders the /get install script. The shell script template
// is parameterized by repo and version; the script verifies the downloaded
// binary against the release's checksums.txt at install time (same pattern
// goreleaser's own installer uses).
package install

import (
	"fmt"
	"strings"
)

// Config is the data the install script template is rendered with.
type Config struct {
	Repo    string // e.g. "owner/repo"
	Version string // release tag, e.g. "v0.1.0"; "latest" installs the newest release
}

// Render returns the shell script body. The script downloads the per-(os,arch)
// binary and the release's checksums.txt, then verifies sha256 before
// installing. macOS's `shasum -a 256` is used when `sha256sum` is unavailable.
func Render(cfg Config) string {
	var sb strings.Builder
	sb.WriteString("#!/bin/sh\nset -e\n")
	sb.WriteString("os=$(uname -s | tr A-Z a-z)\n")
	sb.WriteString("arch=$(uname -m)\n")
	sb.WriteString("case \"$arch\" in\n")
	sb.WriteString("  x86_64) arch=amd64 ;;\n")
	sb.WriteString("  aarch64|arm64) arch=arm64 ;;\n")
	sb.WriteString("  *) echo \"unsupported arch: $arch\" >&2; exit 1 ;;\n")
	sb.WriteString("esac\n")
	sb.WriteString("case \"$os\" in\n")
	sb.WriteString("  darwin|linux) ;;\n")
	sb.WriteString("  *) echo \"unsupported os: $os\" >&2; exit 1 ;;\n")
	sb.WriteString("esac\n\n")
	fmt.Fprintf(&sb, "version=\"${SUSAN_VERSION:-%s}\"\n", cfg.Version)
	fmt.Fprintf(&sb, "repo=\"%s\"\n", cfg.Repo)
	sb.WriteString("asset=\"susan_${os}_${arch}\"\n")
	sb.WriteString("if [ \"$version\" = latest ]; then\n")
	sb.WriteString("  base=\"https://github.com/${repo}/releases/latest/download\"\n")
	sb.WriteString("else\n")
	sb.WriteString("  base=\"https://github.com/${repo}/releases/download/${version}\"\n")
	sb.WriteString("fi\n\n")
	sb.WriteString("tmpdir=$(mktemp -d)\n")
	sb.WriteString("trap 'rm -rf \"$tmpdir\"' EXIT\n")
	sb.WriteString("cd \"$tmpdir\"\n\n")
	sb.WriteString("curl -fsSL \"${base}/${asset}\" -o \"$asset\"\n")
	sb.WriteString("curl -fsSL \"${base}/checksums.txt\" -o checksums.txt\n\n")
	sb.WriteString("expected=$(awk -v a=\"$asset\" '$2==a {print $1}' checksums.txt)\n")
	sb.WriteString("[ -n \"$expected\" ] || { echo \"no checksum for $asset in checksums.txt\" >&2; exit 1; }\n")
	sb.WriteString("if command -v sha256sum >/dev/null 2>&1; then\n")
	sb.WriteString("  actual=$(sha256sum \"$asset\" | cut -d' ' -f1)\n")
	sb.WriteString("else\n")
	sb.WriteString("  actual=$(shasum -a 256 \"$asset\" | cut -d' ' -f1)\n")
	sb.WriteString("fi\n")
	sb.WriteString("[ \"$actual\" = \"$expected\" ] || { echo \"checksum mismatch — refusing to install\" >&2; exit 1; }\n\n")
	sb.WriteString("mkdir -p \"$HOME/.local/bin\"\n")
	sb.WriteString("mv \"$asset\" \"$HOME/.local/bin/susan\"\n")
	sb.WriteString("chmod +x \"$HOME/.local/bin/susan\"\n")
	sb.WriteString("echo \"installed susan ${version} to ~/.local/bin/susan\"\n")
	sb.WriteString("case \":$PATH:\" in *\":$HOME/.local/bin:\"*) ;;\n")
	sb.WriteString("  *) echo \"add ~/.local/bin to your PATH\" ;;\n")
	sb.WriteString("esac\n")
	return sb.String()
}
