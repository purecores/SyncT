package config

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// Default local and optional public endpoints for SyncT.
const (
	DefaultPublicDomain   = ""
	DefaultPublicURL      = ""
	DefaultLocalPort      = 18920
	DefaultLocalListenURL = "https://localhost:18920"
)

// Role defines whether SyncT acts as the tunnel host (this machine) or connects as a client (work PC).
type Role string

const (
	RoleHost   Role = "host"   // 本机服务端: 监听 https://localhost:18920 并通过 Cloudflare Tunnel 发布至公网域名
	RoleClient Role = "client" // 工作电脑客户端 (User受限权限): 零监听端口、零子进程，仅通过 HTTPS 443 连接远端公网域名
)

// TunnelMode defines how cloudflared is used in Host mode.
type TunnelMode string

const (
	TunnelSystem TunnelMode = "system" // Reuse the system's already-running cloudflared service/process (public hostname -> https://localhost:18920)
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

// passwordSalt is a fixed application salt for password-derived keys. A fixed
// salt is deliberate: both computers must derive the same key from the same
// password without exchanging a salt first, and the derived key never leaves
// the machine except in TLS to the paired peer.
const passwordSalt = "SyncT/pairing-2026"

// DerivedTokenPrefix marks an AuthToken produced from a user password. A value
// without it is treated as a raw token, so existing installs keep working.
const DerivedTokenPrefix = "pwd1:"

// DeriveToken turns a user password into the shared authentication key. The
// same password yields the same token on both computers, and the password
// itself is never stored or transmitted.
func DeriveToken(password string) string {
	password = strings.TrimSpace(password)
	if password == "" {
		return ""
	}
	key, err := pbkdf2.Key(sha256.New, password, []byte(passwordSalt), 120000, 32)
	if err != nil {
		// pbkdf2.Key only fails on an invalid hash/parameters, which are fixed
		// here; fall back to a single SHA-256 so the button still works.
		sum := sha256.Sum256([]byte(passwordSalt + password))
		key = sum[:]
	}
	return DerivedTokenPrefix + base64.RawURLEncoding.EncodeToString(key)
}

// IsDerivedToken reports whether a token came from DeriveToken.
func IsDerivedToken(token string) bool {
	return strings.HasPrefix(token, DerivedTokenPrefix)
}

// DeriveTokenFromInput accepts either a raw shared token (st_...) or a
// password, and returns the token to store. Pasting an existing synct:// code
// or st_ token keeps working; anything else is treated as a password.
func DeriveTokenFromInput(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	if strings.HasPrefix(input, "st_") || IsDerivedToken(input) {
		return input
	}
	return DeriveToken(input)
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

// Clone returns a deep copy so concurrent readers and the engine never share
// backing arrays for SyncFolders/IgnorePatterns/TableColumns.
func (c Config) Clone() Config {
	out := c
	out.SyncFolders = append([]FolderMapping(nil), c.SyncFolders...)
	out.IgnorePatterns = append([]string(nil), c.IgnorePatterns...)
	out.TableColumns = append([]string(nil), c.TableColumns...)
	return out
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
	SyncDirection       SyncDirection   `json:"sync_direction"`
	ConflictPolicy      ConflictPolicy  `json:"conflict_policy"`
	SyncDeletions       bool            `json:"sync_deletions"`
	AllowPeerRemotePath bool            `json:"allow_peer_remote_path,omitempty"`
	AutoStartSync       bool            `json:"auto_start_sync"`
	OpenAtLogin      bool            `json:"open_at_login"`
	KeepAwake        bool            `json:"keep_awake"`
	MinimizeToTray   bool            `json:"minimize_to_tray"`
	// TrayCloseNotice shows a desktop notification the first time the main
	// window is closed to the tray. Off by default: closing the window is a
	// deliberate background action and should not also raise a message.
	TrayCloseNotice bool     `json:"tray_close_notice"`
	CloudflaredPath string   `json:"cloudflared_path"`
	IgnorePatterns  []string `json:"ignore_patterns"`
	SidebarWidth    float32  `json:"sidebar_width"`
	TableColumns    []string `json:"table_columns,omitempty"`
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
		"*.conflict-*", // keep local conflict backups from bouncing between peers
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

// PreferredSyncDir is an optional default sync directory override. Left empty
// by default so installations use `<PortableRoot>/SyncFolder` unless
// SYNCT_DEFAULT_SYNC_DIR is set.
const PreferredSyncDir = ""

// DefaultSyncDir returns SYNCT_DEFAULT_SYNC_DIR (or PreferredSyncDir if set)
// when it exists or can be created, and otherwise the portable
// `<PortableRoot>/SyncFolder`. Falling back keeps a machine without that drive
// working instead of failing at start.
func DefaultSyncDir() string {
	preferred := strings.TrimSpace(os.Getenv("SYNCT_DEFAULT_SYNC_DIR"))
	if preferred == "" {
		preferred = PreferredSyncDir
	}
	if preferred != "" {
		if info, err := os.Stat(preferred); err == nil && info.IsDir() {
			return preferred
		}
		if err := os.MkdirAll(preferred, 0o755); err == nil {
			return preferred
		}
		// Do not keep recreating a path the user clearly does not have: remember
		// that it was unavailable and use the portable folder for this session.
		preferredUnavailable.Store(true)
	}
	dir := filepath.Join(PortableRoot(), "SyncFolder")
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

var preferredUnavailable atomic.Bool

// PreferredSyncDirUnavailable reports whether DefaultSyncDir had to fall back.
func PreferredSyncDirUnavailable() bool { return preferredUnavailable.Load() }

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

// Default returns a new Config with portable, low-privilege defaults.
// SYNCT_PUBLIC_URL may be set to pre-populate CustomPublicURL and RemoteURL.
func Default() Config {
	publicURL := NormalizeRemoteURL(os.Getenv("SYNCT_PUBLIC_URL"))
	if publicURL == "" {
		publicURL = DefaultPublicURL
	}
	return Config{
		SyncDir:          DefaultSyncDir(),
		Role:             RoleHost,
		TunnelMode:       TunnelSystem,
		CustomPublicURL:  publicURL,
		LocalPort:        DefaultLocalPort,
		LocalTLS:         true,
		LowPrivilegeMode: true,
		RemoteURL:        publicURL,
		AuthToken:        GenerateToken(),
		SyncDirection:    SyncBidirectional,
		ConflictPolicy:   ConflictKeepBoth,
		SyncDeletions:    true,
		AutoStartSync:    true,
		OpenAtLogin:      false,
		KeepAwake:        true,
		MinimizeToTray:   true,
		IgnorePatterns:   DefaultIgnorePatterns(),
		SidebarWidth:     220,
	}
}

// Load reads Config from disk or creates a default one if missing.
// A damaged config is preserved as config.json.corrupt-<timestamp> before a
// clean default is written, so a later save cannot destroy the user's settings.
func Load() (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(ConfigPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if saveErr := Save(cfg); saveErr != nil {
				return cfg, fmt.Errorf("创建默认配置失败: %w", saveErr)
			}
			return cfg, nil
		}
		return cfg, fmt.Errorf("读取配置文件失败: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		backup := ConfigPath() + ".corrupt-" + time.Now().Format("20060102-150405")
		_ = os.Rename(ConfigPath(), backup)
		cfg = Default()
		if saveErr := Save(cfg); saveErr != nil {
			return cfg, fmt.Errorf("配置文件损坏且无法写入默认配置: %w", saveErr)
		}
		return cfg, fmt.Errorf("配置文件损坏，已备份至 %s: %w", filepath.Base(backup), err)
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
	c.AuthToken = strings.TrimSpace(c.AuthToken)
	if c.AuthToken == "" {
		c.AuthToken = GenerateToken()
	}
	if c.SyncDirection != SyncBidirectional && c.SyncDirection != SyncPushOnly && c.SyncDirection != SyncPullOnly {
		c.SyncDirection = SyncBidirectional
	}
	if c.ConflictPolicy != ConflictKeepBoth && c.ConflictPolicy != ConflictNewerWins {
		c.ConflictPolicy = ConflictKeepBoth
	}
	c.IgnorePatterns = normalizePatterns(c.IgnorePatterns)
	if c.SidebarWidth < 160 || c.SidebarWidth > 420 {
		c.SidebarWidth = 220
	}
}

// normalizePatterns trims, drops empties and de-duplicates ignore rules so the
// scan matcher does not evaluate the same pattern twice. Windows paths are
// case-insensitive, so duplicates are compared case-insensitively there.
func normalizePatterns(patterns []string) []string {
	if len(patterns) == 0 {
		return DefaultIgnorePatterns()
	}
	out := make([]string, 0, len(patterns))
	seen := make(map[string]struct{}, len(patterns))
	for _, raw := range patterns {
		pat := strings.TrimSpace(raw)
		if pat == "" {
			continue
		}
		key := pat
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, pat)
	}
	if len(out) == 0 {
		return DefaultIgnorePatterns()
	}
	return out
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

// EnabledFolderCount returns how many folder mappings are active without
// allocating a slice (used on every UI frame).
func (c Config) EnabledFolderCount() int {
	count := 0
	for _, f := range c.SyncFolders {
		if f.Enabled && strings.TrimSpace(f.LocalPath) != "" {
			count++
		}
	}
	if count == 0 {
		return 1
	}
	return count
}

// FindFolder looks up a folder mapping by ID (treating empty string as "default").
// It iterates the mappings directly instead of calling EnabledFolders, because
// this runs once per transferred file and per server request.
func (c Config) FindFolder(id string) (FolderMapping, bool) {
	id = NormalizeFolderID(id)
	for i := range c.SyncFolders {
		f := c.SyncFolders[i]
		if NormalizeFolderID(f.ID) != id {
			continue
		}
		if f.Enabled && strings.TrimSpace(f.LocalPath) != "" {
			f.ID = id
			if f.Label == "" {
				if id == DefaultFolderID {
					f.Label = "主同步目录"
				} else {
					f.Label = filepath.Base(f.LocalPath)
				}
			}
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

// FindEnabledFolder returns the effective enabled mapping for a folder ID.
// Unlike FindFolder (which is also used to display disabled mappings), it never
// returns a disabled folder, so file transfers cannot write into a mapping the
// user turned off. When every mapping is disabled, EnabledFolders() falls back
// to SyncDir as the primary folder, and this method mirrors that fallback.
func (c Config) FindEnabledFolder(id string) (FolderMapping, bool) {
	id = NormalizeFolderID(id)
	for i := range c.SyncFolders {
		f := c.SyncFolders[i]
		if NormalizeFolderID(f.ID) != id || !f.Enabled || strings.TrimSpace(f.LocalPath) == "" {
			continue
		}
		f.ID = id
		if f.Label == "" {
			if id == DefaultFolderID {
				f.Label = "主同步目录"
			} else {
				f.Label = filepath.Base(f.LocalPath)
			}
		}
		return f, true
	}
	if id != DefaultFolderID {
		return FolderMapping{}, false
	}
	for i := range c.SyncFolders {
		if c.SyncFolders[i].Enabled && strings.TrimSpace(c.SyncFolders[i].LocalPath) != "" {
			// Another mapping is enabled, so EnabledFolders() does not apply the
			// primary fallback; the disabled default must stay unwritable.
			return FolderMapping{}, false
		}
	}
	dir := strings.TrimSpace(c.SyncDir)
	if dir == "" {
		dir = DefaultSyncDir()
	}
	return FolderMapping{
		ID:        DefaultFolderID,
		Label:     "主同步目录",
		LocalPath: dir,
		Enabled:   true,
	}, true
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
		return fmt.Errorf("编码配置失败: %w", err)
	}
	path := ConfigPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("写入配置失败: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("提交配置失败: %w", err)
	}
	return nil
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
// a plain `st_...` token (paired with DefaultPublicURL if configured),
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
	// Allow pasting a raw AuthToken (e.g. "st_...") on the work PC to pair with DefaultPublicURL
	if strings.HasPrefix(input, "st_") && !strings.ContainsAny(input, " /:\\") {
		return DefaultPublicURL, input, nil
	}
	if strings.HasPrefix(input, "http://") || strings.HasPrefix(input, "https://") {
		return NormalizeRemoteURL(input), "", nil
	}
	if strings.Contains(input, ".trycloudflare.com") || (DefaultPublicDomain != "" && strings.Contains(input, DefaultPublicDomain)) {
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
