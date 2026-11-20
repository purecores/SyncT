package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNormalizeDefaults(t *testing.T) {
	var c Config
	c.Normalize()

	if c.SyncDir == "" {
		t.Fatal("SyncDir should be defaulted")
	}
	if len(c.SyncFolders) != 1 || c.SyncFolders[0].ID != DefaultFolderID || !c.SyncFolders[0].Enabled {
		t.Fatalf("SyncFolders = %+v", c.SyncFolders)
	}
	if c.SyncFolders[0].Label != "主同步目录" {
		t.Fatalf("default label = %q", c.SyncFolders[0].Label)
	}
	if c.Role != RoleHost || c.TunnelMode != TunnelSystem {
		t.Fatalf("role/tunnel = %s/%s", c.Role, c.TunnelMode)
	}
	if c.LocalPort != DefaultLocalPort {
		t.Fatalf("port = %d", c.LocalPort)
	}
	if !strings.HasPrefix(c.AuthToken, "st_") {
		t.Fatalf("token = %q", c.AuthToken)
	}
	if c.SyncDirection != SyncBidirectional || c.ConflictPolicy != ConflictKeepBoth {
		t.Fatalf("sync defaults = %s/%s", c.SyncDirection, c.ConflictPolicy)
	}
	if d := Default(); !d.SyncDeletions || !d.AutoStartSync {
		t.Fatalf("Default() deletion/autostart = %v/%v", d.SyncDeletions, d.AutoStartSync)
	}
	if d := Default(); d.OpenAtLogin {
		t.Fatal("Default() must not register a login item without the user opting in")
	}
	if len(c.IgnorePatterns) == 0 {
		t.Fatal("ignore patterns should default")
	}
	if c.SidebarWidth != 220 {
		t.Fatalf("sidebar width = %v", c.SidebarWidth)
	}
}

func TestNormalizeDuplicateFolderIDs(t *testing.T) {
	c := Default()
	c.SyncFolders = []FolderMapping{
		{ID: "dup", Label: "one", LocalPath: "/a", Enabled: true},
		{ID: "dup", Label: "two", LocalPath: "/b", Enabled: true},
		{ID: "", Label: "three", LocalPath: "/c", Enabled: true},
		{ID: "@proj", Label: "four", LocalPath: "/d", Enabled: true},
	}
	c.Normalize()

	seen := make(map[string]bool)
	for _, f := range c.SyncFolders {
		if seen[f.ID] {
			t.Fatalf("duplicate folder ID after Normalize: %q in %+v", f.ID, c.SyncFolders)
		}
		seen[f.ID] = true
		if f.Label == "" {
			t.Fatalf("folder label should be filled: %+v", f)
		}
	}
	if !seen[DefaultFolderID] || !seen["proj"] {
		t.Fatalf("expected default and normalized @proj IDs, got %v", seen)
	}
}

func TestEnabledFoldersFindFolderAndCount(t *testing.T) {
	c := Default()
	c.SyncDir = "/main"
	c.SyncFolders = []FolderMapping{
		{ID: DefaultFolderID, LocalPath: "/main", Enabled: true},
		{ID: "off", LocalPath: "/off", Enabled: false},
		{ID: "nopath", LocalPath: "", Enabled: true},
	}
	enabled := c.EnabledFolders()
	if len(enabled) != 1 || enabled[0].ID != DefaultFolderID || enabled[0].Label != "主同步目录" {
		t.Fatalf("EnabledFolders = %+v", enabled)
	}
	if got := c.EnabledFolderCount(); got != 1 {
		t.Fatalf("EnabledFolderCount = %d", got)
	}

	folder, ok := c.FindFolder("")
	if !ok || folder.ID != DefaultFolderID || folder.LocalPath != "/main" {
		t.Fatalf("FindFolder(default) = %+v, ok=%v", folder, ok)
	}
	if folder, ok = c.FindFolder("off"); !ok || folder.Enabled {
		t.Fatalf("FindFolder(off) = %+v, ok=%v", folder, ok)
	}
	if _, ok = c.FindFolder("missing"); ok {
		t.Fatal("unknown folder should not be found")
	}

	empty := Config{}
	if got := empty.EnabledFolderCount(); got != 1 {
		t.Fatalf("fallback EnabledFolderCount = %d; want 1", got)
	}
}

func TestFindEnabledFolder(t *testing.T) {
	c := Default()
	c.SyncFolders = []FolderMapping{
		{ID: DefaultFolderID, LocalPath: "/main", Enabled: true},
		{ID: "off", LocalPath: "/off", Enabled: false},
		{ID: "nopath", LocalPath: "", Enabled: true},
	}
	if f, ok := c.FindEnabledFolder(""); !ok || f.ID != DefaultFolderID || f.LocalPath != "/main" {
		t.Fatalf("FindEnabledFolder(default) = %+v, ok=%v", f, ok)
	}
	if _, ok := c.FindEnabledFolder("off"); ok {
		t.Fatal("disabled mapping must not be returned by FindEnabledFolder")
	}
	if _, ok := c.FindEnabledFolder("nopath"); ok {
		t.Fatal("mapping without a local path must not be returned")
	}
	if _, ok := c.FindEnabledFolder("missing"); ok {
		t.Fatal("unknown mapping must not be found")
	}

	// Disabled default with another enabled mapping: no primary fallback.
	c = Default()
	c.SyncFolders = []FolderMapping{
		{ID: DefaultFolderID, LocalPath: "/main", Enabled: false},
		{ID: "other", LocalPath: "/other", Enabled: true},
	}
	if _, ok := c.FindEnabledFolder(DefaultFolderID); ok {
		t.Fatal("disabled default must stay unavailable while another mapping is enabled")
	}

	// Every mapping disabled: mirror EnabledFolders' SyncDir fallback.
	c = Default()
	c.SyncDir = "/fallback"
	c.SyncFolders = []FolderMapping{{ID: DefaultFolderID, LocalPath: "/main", Enabled: false}}
	f, ok := c.FindEnabledFolder(DefaultFolderID)
	if !ok || f.LocalPath != "/fallback" {
		t.Fatalf("all-disabled fallback = %+v, ok=%v; want /fallback", f, ok)
	}

	c = Default()
	c.SyncFolders = nil
	if f, ok := c.FindEnabledFolder(DefaultFolderID); !ok || f.LocalPath != c.SyncDir {
		t.Fatalf("empty mappings fallback = %+v, ok=%v", f, ok)
	}
}

func TestUpsertRemoveFolder(t *testing.T) {
	c := Default()
	c.UpsertFolder(FolderMapping{ID: DefaultFolderID, LocalPath: "/new-main", Enabled: true})
	if c.SyncDir != "/new-main" || len(c.SyncFolders) != 1 {
		t.Fatalf("primary upsert: dir=%q folders=%+v", c.SyncDir, c.SyncFolders)
	}
	c.UpsertFolder(FolderMapping{ID: "docs", LocalPath: "/docs", Enabled: true})
	if len(c.SyncFolders) != 2 {
		t.Fatalf("secondary upsert folders=%+v", c.SyncFolders)
	}
	if !c.RemoveFolder("docs") {
		t.Fatal("RemoveFolder(docs) should succeed")
	}
	if c.RemoveFolder(DefaultFolderID) {
		t.Fatal("the primary folder must not be removable")
	}
	if len(c.SyncFolders) != 1 {
		t.Fatalf("folders after remove = %+v", c.SyncFolders)
	}
}

func TestShareCodeParsing(t *testing.T) {
	url := "https://sync.example.com"
	token := "st_roundtrip"
	code := EncodeShareCode(url, token)
	gotURL, gotToken, err := DecodeShareCode(code)
	if err != nil || gotURL != url || gotToken != token {
		t.Fatalf("round trip = (%q, %q, %v)", gotURL, gotToken, err)
	}

	gotURL, gotToken, err = DecodeShareCode("st_raw_token_123")
	if err != nil || gotURL != DefaultPublicURL || gotToken != "st_raw_token_123" {
		t.Fatalf("raw token = (%q, %q, %v)", gotURL, gotToken, err)
	}

	gotURL, _, err = DecodeShareCode("https://demo.trycloudflare.com")
	if err != nil || gotURL != "https://demo.trycloudflare.com" {
		t.Fatalf("plain URL = (%q, %v)", gotURL, err)
	}

	gotURL, gotToken, err = DecodeShareCode("synct://?url=https%3A%2F%2Fexample.com&token=t1")
	if err != nil || gotURL != "https://example.com" || gotToken != "t1" {
		t.Fatalf("query code = (%q, %q, %v)", gotURL, gotToken, err)
	}

	gotURL, _, err = DecodeShareCode("amber-sky-8492.trycloudflare.com")
	if err != nil || gotURL != "https://amber-sky-8492.trycloudflare.com" {
		t.Fatalf("bare tunnel host = (%q, %v)", gotURL, err)
	}

	for _, bad := range []string{"", "   ", "not a code", "synct://connect/!!!not-base64!!!"} {
		if _, _, err := DecodeShareCode(bad); err == nil {
			t.Errorf("DecodeShareCode(%q) should fail", bad)
		}
	}
}

func TestNormalizeRemoteURL(t *testing.T) {
	cases := map[string]string{
		"https://example.com/":      "https://example.com",
		"  https://example.com/// ": "https://example.com",
		"example.com":               "https://example.com",
		"127.0.0.1:18920":           "http://127.0.0.1:18920",
		"192.168.1.5:8080":          "http://192.168.1.5:8080",
		"10.0.0.2:9000":             "http://10.0.0.2:9000",
		"":                          "",
	}
	for in, want := range cases {
		if got := NormalizeRemoteURL(in); got != want {
			t.Errorf("NormalizeRemoteURL(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestSaveLoadAndCorruptBackup(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", dir)

	c := Default()
	c.SyncDir = filepath.Join(dir, "sync")
	c.AuthToken = "st_saved_token"
	c.IgnorePatterns = []string{"*.tmp", " *.tmp ", " ", ".git"}
	if err := Save(c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.AuthToken != "st_saved_token" || loaded.SyncDir != c.SyncDir {
		t.Fatalf("loaded cfg = token %q dir %q", loaded.AuthToken, loaded.SyncDir)
	}
	if len(loaded.IgnorePatterns) != 2 {
		t.Fatalf("normalized patterns = %v; want 2 unique entries", loaded.IgnorePatterns)
	}

	// Damaged config: preserved as a backup and replaced with defaults.
	if err := os.WriteFile(ConfigPath(), []byte("{broken json"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := Load()
	if err == nil {
		t.Fatal("Load should report a corrupt config")
	}
	if !strings.Contains(err.Error(), "备份") {
		t.Fatalf("error should mention the backup: %v", err)
	}
	if recovered.AuthToken == "" || recovered.Role != RoleHost {
		t.Fatalf("recovered config should be valid defaults: %+v", recovered)
	}
	backups, _ := filepath.Glob(ConfigPath() + ".corrupt-*")
	if len(backups) == 0 {
		t.Fatal("expected a config.json.corrupt-* backup")
	}
	if _, err := Load(); err != nil {
		t.Fatalf("config should be readable after recovery: %v", err)
	}
}

func TestCloneDeepCopy(t *testing.T) {
	c := Default()
	c.Normalize()
	c.SyncFolders[0].LocalPath = "/original"
	c.IgnorePatterns[0] = "original"
	c.TableColumns = []string{"a"}

	clone := c.Clone()
	clone.SyncFolders[0].LocalPath = "/changed"
	clone.IgnorePatterns[0] = "changed"
	clone.TableColumns[0] = "changed"

	if c.SyncFolders[0].LocalPath != "/original" ||
		c.IgnorePatterns[0] != "original" ||
		c.TableColumns[0] != "a" {
		t.Fatalf("Clone shares backing arrays with the original: %+v", c)
	}
}

func TestIgnorePatternNormalization(t *testing.T) {
	c := Default()
	c.IgnorePatterns = []string{"A.tmp", "a.tmp", "", "  ", ".git", " .git "}
	c.Normalize()
	letCount := len(c.IgnorePatterns)
	if runtime.GOOS == "windows" {
		if letCount != 2 {
			t.Fatalf("Windows should de-duplicate case-insensitively, got %v", c.IgnorePatterns)
		}
	} else if letCount != 3 {
		t.Fatalf("non-Windows de-dup = %v; want 3", c.IgnorePatterns)
	}
}

func TestSharePayloadJSONShape(t *testing.T) {
	payload, err := json.Marshal(SharePayload{URL: "u", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"u":"u","t":"t"}` {
		t.Fatalf("SharePayload json = %s", payload)
	}
}

func TestDefaultSyncDirAndEmptyPublicDomain(t *testing.T) {
	portableRoot := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", portableRoot)
	t.Setenv("SYNCT_DEFAULT_SYNC_DIR", "")
	t.Setenv("SYNCT_PUBLIC_URL", "")

	wantPortable := filepath.Join(portableRoot, "SyncFolder")
	if got := DefaultSyncDir(); got != wantPortable {
		t.Fatalf("DefaultSyncDir() with unset env = %q; want %q", got, wantPortable)
	}
	if info, err := os.Stat(wantPortable); err != nil || !info.IsDir() {
		t.Fatalf("expected portable SyncFolder to exist: info=%v err=%v", info, err)
	}

	customDir := filepath.Join(t.TempDir(), "custom-sync")
	t.Setenv("SYNCT_DEFAULT_SYNC_DIR", customDir)
	if got := DefaultSyncDir(); got != customDir {
		t.Fatalf("DefaultSyncDir() with SYNCT_DEFAULT_SYNC_DIR = %q; want %q", got, customDir)
	}
	if info, err := os.Stat(customDir); err != nil || !info.IsDir() {
		t.Fatalf("expected custom sync dir to be created: info=%v err=%v", info, err)
	}

	if DefaultPublicDomain != "" {
		t.Fatalf("DefaultPublicDomain = %q; want empty", DefaultPublicDomain)
	}
	if _, _, err := DecodeShareCode("not a code"); err == nil {
		t.Fatal(`DecodeShareCode("not a code") must fail when DefaultPublicDomain is empty`)
	}

	t.Setenv("SYNCT_PUBLIC_URL", "sync.example.com/")
	d := Default()
	if d.CustomPublicURL != "https://sync.example.com" || d.RemoteURL != "https://sync.example.com" {
		t.Fatalf("Default() with SYNCT_PUBLIC_URL = (%q, %q); want https://sync.example.com", d.CustomPublicURL, d.RemoteURL)
	}
}
