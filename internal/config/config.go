package config

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Dedicated self-use endpoints for SyncT.
const (
	DefaultPublicDomain   = "synctreei.suosmanthus.cyou"
	DefaultPublicURL      = "https://synctreei.suosmanthus.cyou"
	DefaultLocalPort      = 18920
	DefaultLocalListenURL = "https://localhost:18920"
)

// Role defines whether SyncT acts as the tunnel host (this machine) or connects as a client (work PC).
type Role string

const (
	RoleHost   Role = "host"   //の本机服务端: 监听 https://localhost:18920 并通过 Token Tunnel 发布至 synctreei.suosmanthus.cyou
	RoleClient Role = "client" // 工作电脑客户端 (User受限权限): 零监听端口、零子进程，仅通过 HTTPS 443 连接 synctreei.suosmanthus.cyou
)

// TunnelMode defines how cloudflared is used in Host mode.
type TunnelMode string

const (
	TunnelSystem TunnelMode = "system" // Reuse the system's already-running cloudflared service/process (synctreei.suosmanthus.cyou -> https://localhost:18920)
	TunnelToken  TunnelMode = "token"  // Spawn a managed cloudflared process with an explicit Tunnel Token
	TunnelQuick  TunnelMode = "quick"  // TryCloudflare temporary tunnel
	TunnelDirect TunnelMode = "direct" // Local/LAN server only, without cloudflared
)

// SyncDirection controls how files flow between local and remote folders.
type SyncDirection string

const (
	SyncBidirectional SyncDirection = "bidirectional" // Two-way real-time sync
	SyncPushOnly      SyncDirection = "push_only"     // Local -> Remote only
	SyncPullOnly      SyncDirection = "pull_only"     // Remote -> Local only
)

// ConflictPolicy controls what happens when both sides modify the same file concurrently.
type ConflictPolicy string

const (
	ConflictKeepBoth  ConflictPolicy = "keep_both"  // Save conflicting version as .conflict-<timestamp>
	ConflictNewerWins ConflictPolicy = "newer_wins" // Overwrite with newer modification time
)

// DefaultFolderID is the canonical ID for the primary sync folder.
const DefaultFolderID = "default"

// FolderMapping defines a synchronized folder pair between this machine and the remote peer.
// It supports syncing multiple folders as well as mapping a local folder on this PC
// to a different folder path on the other PC (本机文件夹 <-> 另一台电脑目标文件夹).
type FolderMapping struct {
	ID         string `json:"id"`                    // Mapping ID/alias shared between both PCs (e.g. "default", "work-docs")
	Label      string `json:"label,omitempty"`       // Display name (e.g. "主同步目录", "工作文档")
	LocalPath  string `json:"local_path"`            // Local directory path on this computer
	RemotePath string `json:"remote_path,omitempty"` // Target directory path on the other computer (optional cross-PC path mapping)
	Enabled    bool   `json:"enabled"`               // Whether this folder mapping is active
}

// NormalizeFolderID sanitizes a folder mapping ID so it is safe for URLs and virtual paths.
func NormalizeFolderID(id string) string {
	id = strings.TrimSpace(id)
	id = strings.TrimPrefix(id, "@")
	if id == "" {
		return DefaultFolderID
	}
	var sb strings.Builder
	for _, r := range id {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r > 127:
			sb.WriteRune(r)
		default:
			sb.WriteByte('-')
		}
	}
	res := strings.Trim(sb.String(), "-.")
	if res == "" {
		return DefaultFolderID
	}
	return res
}

// Config holds all persistent portable settings for SyncT.
type Config struct {
	SyncDir          string          `json:"sync_dir"`
	SyncFolders      []FolderMapping `json:"sync_folders,omitempty"`
	Role             Role            `json:"role"`
	TunnelMode       TunnelMode      `json:"tunnel_mode"`
	TunnelToken      string          `json:"tunnel_token"`
	CustomPublicURL  string          `json:"custom_public_url"`
	LocalPort        int             `json:"local_port"`
	LocalTLS         bool            `json:"local_tls"`
	LowPrivilegeMode bool            `json:"low_privilege_mode"`
	RemoteURL        string          `json:"remote_url"`
	AuthToken        string          `json:"auth_token"`
	SyncDirection    SyncDirection   `json:"sync_direction"`
	ConflictPolicy   ConflictPolicy  `json:"conflict_policy"`
	SyncDeletions    bool            `json:"sync_deletions"`
	AutoStartSync    bool            `json:"auto_start_sync"`
	KeepAwake        bool            `json:"keep_awake"`
	MinimizeToTray   bool            `json:"minimize_to_tray"`
	CloudflaredPath  string          `json:"cloudflared_path"`
	IgnorePatterns   []string        `json:"ignore_patterns"`
	SidebarWidth     float32         `json:"sidebar_width"`
	TableColumns     []string        `json:"table_columns,omitempty"`
}

// DefaultIgnorePatterns returns sensible default ignore patterns for Windows 11 folders.
func DefaultIgnorePatterns() []string {
	return []string{
		".synct*",
		"*.tmp",
		"*.temp",
		"~$*",
		"Thumbs.db",
		"desktop.ini",
		".DS_Store",
		".git",
		"node_modules",
	}
}

// PresetIgnoreSuggestions lists common patterns users may pick in the TokenField.
func PresetIgnoreSuggestions() []string {
	return []string{
		".git",
		"node_modules",
		"*.tmp",
		"*.log",
		"*.bak",
		"Thumbs.db",
		"desktop.ini",
		".DS_Store",
		"~$*",
		"dist",
		"build",
		".idea",
		".vscode",
		"__pycache__",
	}
}

// GenerateToken creates a random 12-byte hex authentication token.
func GenerateToken() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "synct-default-token-2026"
	}
	return "st_" + hex.EncodeToString(buf)
}

// PortableRoot returns the portable root directory next to the executable,
// or SYNCT_DATA_DIR if set (e.g. in tests).
func PortableRoot() string {
	if env := os.Getenv("SYNCT_DATA_DIR"); env != "" {
		return env
	}
	exe, err := os.Executable()
	if err == nil {
		dir := filepath.Dir(exe)
		// When running under `go run` or `go test`, os.Executable() is in Temp;
		// prefer current working directory in that case so portable files stay in project folder.
		lower := strings.ToLower(dir)
		if !strings.Contains(lower, "go-build") && !strings.Contains(lower, "\\temp\\") {
			return dir
		}
	}
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// DataDir returns the portable data directory (`<PortableRoot>/data`).
func DataDir() string {
	dir := filepath.Join(PortableRoot(), "data")
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

// BinDir returns the portable binary directory (`<PortableRoot>/bin`).
func BinDir() string {
	dir := filepath.Join(PortableRoot(), "bin")
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

// DefaultSyncDir returns the default portable sync folder (`<PortableRoot>/SyncFolder`).
func DefaultSyncDir() string {
	dir := filepath.Join(PortableRoot(), "SyncFolder")
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

// ConfigPath returns the path to `data/config.json`.
func ConfigPath() string {
	return filepath.Join(DataDir(), "config.json")
}

// StatePath returns the path to `data/state.json`.
func StatePath() string {
	return filepath.Join(DataDir(), "state.json")
}

// LocalCertPaths returns the portable paths for the local `https://localhost:18920` TLS cert and key.
func LocalCertPaths() (certPath, keyPath string) {
	d := DataDir()
	return filepath.Join(d, "localhost-cert.pem"), filepath.Join(d, "localhost-key.pem")
}

// Default returns a new Config pre-configured for the user's dedicated setup:
// System already-running Cloudflared (Token Tunnel) -> synctreei.suosmanthus.cyou -> https://localhost:18920,
// with User-privilege restraint enabled.
func Default() Config {
	return Config{
		SyncDir:          DefaultSyncDir(),
		Role:             RoleHost,
		TunnelMode:       TunnelSystem,
		CustomPublicURL:  DefaultPublicURL,
		LocalPort:        DefaultLocalPort,
		LocalTLS:         true,
		LowPrivilegeMode: true,
		RemoteURL:        DefaultPublicURL,
		AuthToken:        GenerateToken(),
		SyncDirection:    SyncBidirectional,
		ConflictPolicy:   ConflictKeepBoth,
		SyncDeletions:    true,
		AutoStartSync:    false,
		KeepAwake:        true,
		MinimizeToTray:   true,
		IgnorePatterns:   DefaultIgnorePatterns(),
		SidebarWidth:     220,
	}
}

// Load reads Config from disk or creates a default one if missing.
func Load() (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(ConfigPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_ = Save(cfg)
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	cfg.Normalize()
	return cfg, nil
}

// Normalize fills in any missing defaults after loading or editing.
func (c *Config) Normalize() {
	defDir := DefaultSyncDir()
	c.SyncDir = strings.TrimSpace(c.SyncDir)
	if c.SyncDir == "" && len(c.SyncFolders) > 0 {
		c.SyncDir = strings.TrimSpace(c.SyncFolders[0].LocalPath)
	}
	if c.SyncDir == "" {
		c.SyncDir = defDir
	}

	if len(c.SyncFolders) == 0 {
		c.SyncFolders = []FolderMapping{
			{
				ID:        DefaultFolderID,
				Label:     "主同步目录",
				LocalPath: c.SyncDir,
				Enabled:   true,
			},
		}
	} else {
		firstLocal := strings.TrimSpace(c.SyncFolders[0].LocalPath)
		if firstLocal == "" {
			c.SyncFolders[0].LocalPath = c.SyncDir
		} else if c.SyncDir != firstLocal {
			if c.SyncDir == defDir {
				c.SyncDir = firstLocal
			} else {
				c.SyncFolders[0].LocalPath = c.SyncDir
			}
		}

		seen := make(map[string]bool, len(c.SyncFolders))
		for i := range c.SyncFolders {
			id := NormalizeFolderID(c.SyncFolders[i].ID)
			if i == 0 && strings.TrimSpace(c.SyncFolders[i].ID) == "" {
				id = DefaultFolderID
			}
			if seen[id] {
				id = fmt.Sprintf("%s-%d", id, i+1)
			}
			seen[id] = true
			c.SyncFolders[i].ID = id
			c.SyncFolders[i].LocalPath = strings.TrimSpace(c.SyncFolders[i].LocalPath)
			c.SyncFolders[i].RemotePath = strings.TrimSpace(c.SyncFolders[i].RemotePath)
			c.SyncFolders[i].Label = strings.TrimSpace(c.SyncFolders[i].Label)
			if c.SyncFolders[i].Label == "" {
				if id == DefaultFolderID {
					c.SyncFolders[i].Label = "主同步目录"
				} else if c.SyncFolders[i].LocalPath != "" {
					c.SyncFolders[i].Label = filepath.Base(c.SyncFolders[i].LocalPath)
				} else {
					c.SyncFolders[i].Label = id
				}
			}
		}
		if c.SyncFolders[0].LocalPath != "" {
			c.SyncDir = c.SyncFolders[0].LocalPath
		}
	}

	if c.Role != RoleHost && c.Role != RoleClient {
		c.Role = RoleHost
	}
	if c.TunnelMode != TunnelSystem && c.TunnelMode != TunnelQuick && c.TunnelMode != TunnelToken && c.TunnelMode != TunnelDirect {
		c.TunnelMode = TunnelSystem
	}
	// If saved config had "token" mode without an explicit token, reuse the system's already-running Cloudflared
	if c.TunnelMode == TunnelToken && strings.TrimSpace(c.TunnelToken) == "" {
		c.TunnelMode = TunnelSystem
	}
	if strings.TrimSpace(c.CustomPublicURL) == "" {
		c.CustomPublicURL = DefaultPublicURL
	} else {
		c.CustomPublicURL = NormalizeRemoteURL(c.CustomPublicURL)
	}
	if strings.TrimSpace(c.RemoteURL) == "" {
		c.RemoteURL = DefaultPublicURL
	} else {
		c.RemoteURL = NormalizeRemoteURL(c.RemoteURL)
	}
	if c.LocalPort <= 0 || c.LocalPort > 65535 {
		c.LocalPort = DefaultLocalPort
	}
	if strings.TrimSpace(c.AuthToken) == "" {
		c.AuthToken = GenerateToken()
	}
	if c.SyncDirection != SyncBidirectional && c.SyncDirection != SyncPushOnly && c.SyncDirection != SyncPullOnly {
		c.SyncDirection = SyncBidirectional
	}
	if c.ConflictPolicy != ConflictKeepBoth && c.ConflictPolicy != ConflictNewerWins {
		c.ConflictPolicy = ConflictKeepBoth
	}
	if len(c.IgnorePatterns) == 0 {
		c.IgnorePatterns = DefaultIgnorePatterns()
	}
	if c.SidebarWidth < 160 || c.SidebarWidth > 420 {
		c.SidebarWidth = 220
	}
}

// EnabledFolders returns all enabled folder mappings that have a non-empty LocalPath.
func (c Config) EnabledFolders() []FolderMapping {
	var out []FolderMapping
	for _, f := range c.SyncFolders {
		if f.Enabled && strings.TrimSpace(f.LocalPath) != "" {
			id := NormalizeFolderID(f.ID)
			f.ID = id
			if f.Label == "" {
				if id == DefaultFolderID {
					f.Label = "主同步目录"
				} else {
					f.Label = filepath.Base(f.LocalPath)
				}
			}
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		dir := strings.TrimSpace(c.SyncDir)
		if dir == "" {
			dir = DefaultSyncDir()
		}
		out = append(out, FolderMapping{
			ID:        DefaultFolderID,
			Label:     "主同步目录",
			LocalPath: dir,
			Enabled:   true,
		})
	}
	return out
}

// FindFolder looks up a folder mapping by ID (treating empty string as "default").
func (c Config) FindFolder(id string) (FolderMapping, bool) {
	id = NormalizeFolderID(id)
	for _, f := range c.EnabledFolders() {
		if NormalizeFolderID(f.ID) == id {
			return f, true
		}
	}
	for _, f := range c.SyncFolders {
		if NormalizeFolderID(f.ID) == id {
			return f, true
		}
	}
	if id == DefaultFolderID && strings.TrimSpace(c.SyncDir) != "" {
		return FolderMapping{
			ID:        DefaultFolderID,
			Label:     "主同步目录",
			LocalPath: c.SyncDir,
			Enabled:   true,
		}, true
	}
	return FolderMapping{}, false
}

// UpsertFolder adds or updates a folder mapping by ID and keeps SyncDir synchronized with the primary folder.
func (c *Config) UpsertFolder(m FolderMapping) {
	m.ID = NormalizeFolderID(m.ID)
	m.LocalPath = strings.TrimSpace(m.LocalPath)
	m.RemotePath = strings.TrimSpace(m.RemotePath)
	m.Label = strings.TrimSpace(m.Label)
	if m.Label == "" {
		if m.ID == DefaultFolderID {
			m.Label = "主同步目录"
		} else if m.LocalPath != "" {
			m.Label = filepath.Base(m.LocalPath)
		} else {
			m.Label = m.ID
		}
	}
	for i := range c.SyncFolders {
		if NormalizeFolderID(c.SyncFolders[i].ID) == m.ID {
			c.SyncFolders[i] = m
			if i == 0 && m.LocalPath != "" {
				c.SyncDir = m.LocalPath
			}
			return
		}
	}
	c.SyncFolders = append(c.SyncFolders, m)
	if len(c.SyncFolders) == 1 && m.LocalPath != "" {
		c.SyncDir = m.LocalPath
	}
}

// RemoveFolder removes a folder mapping by ID (the primary folder at index 0 is never removed).
func (c *Config) RemoveFolder(id string) bool {
	id = NormalizeFolderID(id)
	for i := range c.SyncFolders {
		if i > 0 && NormalizeFolderID(c.SyncFolders[i].ID) == id {
			c.SyncFolders = append(c.SyncFolders[:i], c.SyncFolders[i+1:]...)
			return true
		}
	}
	return false
}

// LocalListenURL returns the formatted local listen URL (e.g. `https://localhost:18920`).
func (c Config) LocalListenURL(actualPort int) string {
	port := actualPort
	if port <= 0 {
		port = c.LocalPort
	}
	if port <= 0 {
		port = DefaultLocalPort
	}
	scheme := "https"
	if !c.LocalTLS {
		scheme = "http"
	}
	return fmt.Sprintf("%s://localhost:%d", scheme, port)
}

// Save writes the Config atomically to `data/config.json`.
func Save(c Config) error {
	c.Normalize()
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	path := ConfigPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// SharePayload represents the encoded share URI payload.
type SharePayload struct {
	URL   string `json:"u"`
	Token string `json:"t"`
}

// EncodeShareCode encodes a remote URL and auth token into a portable `synct://` connection code.
func EncodeShareCode(remoteURL, token string) string {
	remoteURL = strings.TrimSpace(remoteURL)
	token = strings.TrimSpace(token)
	if remoteURL == "" {
		remoteURL = DefaultPublicURL
	}
	payload, _ := json.Marshal(SharePayload{
		URL:   remoteURL,
		Token: token,
	})
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return "synct://connect/" + encoded
}

// DecodeShareCode parses either a `synct://connect/<base64>`, `synct://?url=...&token=...`,
// a plain `st_...` token (automatically paired with `https://synctreei.suosmanthus.cyou`),
// or a plain `http(s)://` URL into (remoteURL, token, error).
func DecodeShareCode(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", errors.New("连接码不能为空")
	}
	if strings.HasPrefix(input, "synct://connect/") {
		raw := strings.TrimPrefix(input, "synct://connect/")
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return "", "", fmt.Errorf("无效的连接码编码: %w", err)
		}
		var p SharePayload
		if err := json.Unmarshal(data, &p); err != nil {
			return "", "", fmt.Errorf("无效的连接码内容: %w", err)
		}
		if p.URL == "" {
			p.URL = DefaultPublicURL
		}
		return NormalizeRemoteURL(p.URL), p.Token, nil
	}
	if strings.HasPrefix(input, "synct://") {
		u, err := url.Parse(input)
		if err != nil {
			return "", "", err
		}
		q := u.Query()
		remote := q.Get("url")
		tok := q.Get("token")
		if remote == "" {
			remote = DefaultPublicURL
		}
		return NormalizeRemoteURL(remote), tok, nil
	}
	// Allow pasting a raw AuthToken (e.g. "st_...") on the work PC to immediately pair with DefaultPublicURL
	if strings.HasPrefix(input, "st_") && !strings.ContainsAny(input, " /:\\") {
		return DefaultPublicURL, input, nil
	}
	if strings.HasPrefix(input, "http://") || strings.HasPrefix(input, "https://") {
		return NormalizeRemoteURL(input), "", nil
	}
	if strings.Contains(input, ".trycloudflare.com") || strings.Contains(input, DefaultPublicDomain) {
		return NormalizeRemoteURL("https://" + input), "", nil
	}
	return "", "", errors.New("无法识别的连接码或隧道 URL 格式")
}

// NormalizeRemoteURL trims trailing slashes and ensures an http/https scheme.
func NormalizeRemoteURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "/")
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		if strings.HasPrefix(raw, "127.0.0.1") || strings.HasPrefix(raw, "192.168.") || strings.HasPrefix(raw, "10.") {
			raw = "http://" + raw
		} else {
			raw = "https://" + raw
		}
	}
	return raw
}
