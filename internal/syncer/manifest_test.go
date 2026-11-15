package syncer

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func TestScanFoldersIncrementalDiffAndCounts(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "a.txt"), []byte("alpha"))
	mustWriteFile(t, filepath.Join(root, "sub", "b.txt"), []byte("bravo"))
	mustWriteFile(t, filepath.Join(root, "sub", "c.txt"), []byte("charlie"))

	store := NewIndexStore(filepath.Join(t.TempDir(), "state.json"))
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}

	changed, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if len(changed) != 3 || len(deleted) != 0 {
		t.Fatalf("first scan = %d changed, %d deleted; want 3, 0", len(changed), len(deleted))
	}
	count, total := store.Counts()
	wantBytes := int64(len("alpha") + len("bravo") + len("charlie"))
	if count != 3 || total != wantBytes {
		t.Fatalf("counts = %d files / %d bytes; want 3 / %d", count, total, wantBytes)
	}
	entries := store.Entries()
	if len(entries) != 3 {
		t.Fatalf("entries = %d; want 3", len(entries))
	}
	for i := 1; i < len(entries); i++ {
		if entries[i-1].RelPath > entries[i].RelPath {
			t.Fatalf("entries not sorted: %q > %q", entries[i-1].RelPath, entries[i].RelPath)
		}
	}

	// An unchanged rescan must not report changes and must reuse cached hashes.
	changed, deleted, err = store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("steady-state scan = %d changed, %d deleted, err=%v; want 0,0,nil", len(changed), len(deleted), err)
	}

	// Updating a file reports exactly that file and adjusts totals.
	newContent := []byte("bravo updated with more bytes")
	mustWriteFile(t, filepath.Join(root, "sub", "b.txt"), newContent)
	changed, deleted, err = store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf("update scan = %d changed, %d deleted, err=%v; want 1,0,nil", len(changed), len(deleted), err)
	}
	if changed[0].RelPath != "sub/b.txt" || changed[0].SHA256 != sha256Hex(newContent) {
		t.Fatalf("updated meta = %+v", changed[0])
	}
	count, total = store.Counts()
	wantBytes = int64(len("alpha") + len(newContent) + len("charlie"))
	if count != 3 || total != wantBytes {
		t.Fatalf("counts after update = %d / %d; want 3 / %d", count, total, wantBytes)
	}

	// Deleting a file creates a tombstone.
	if err := os.Remove(filepath.Join(root, "sub", "c.txt")); err != nil {
		t.Fatal(err)
	}
	changed, deleted, err = store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(changed) != 0 || len(deleted) != 1 {
		t.Fatalf("delete scan = %d changed, %d deleted, err=%v; want 0,1,nil", len(changed), len(deleted), err)
	}
	if deleted[0].RelPath != "sub/c.txt" {
		t.Fatalf("tombstone = %+v", deleted[0])
	}
	if _, ok := store.Snapshot().Tombstones["sub/c.txt"]; !ok {
		t.Fatal("expected tombstone for sub/c.txt")
	}
	if count, _ = store.Counts(); count != 2 {
		t.Fatalf("count after delete = %d; want 2", count)
	}

	// Re-creating a file with a newer mtime clears the tombstone.
	recreated := []byte("charlie reborn")
	mustWriteFile(t, filepath.Join(root, "sub", "c.txt"), recreated)
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(root, "sub", "c.txt"), future, future); err != nil {
		t.Fatal(err)
	}
	changed, deleted, err = store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf("recreate scan = %d changed, %d deleted, err=%v; want 1,0,nil", len(changed), len(deleted), err)
	}
	if _, ok := store.Snapshot().Tombstones["sub/c.txt"]; ok {
		t.Fatal("tombstone should be cleared after re-creating the file")
	}
}

func TestScanMissingRootDoesNotTombstone(t *testing.T) {
	root := filepath.Join(t.TempDir(), "folder")
	mustWriteFile(t, filepath.Join(root, "keep.txt"), []byte("keep me"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}

	// A temporarily unavailable folder must never look like a mass deletion.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, root, []byte("now this path is a file, not a folder"))
	_, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err == nil {
		t.Fatal("expected an error when the root is not a directory")
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted = %d; want 0 while the root is unavailable", len(deleted))
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want index untouched (1)", count)
	}
	if tombs := store.Snapshot().Tombstones; len(tombs) != 0 {
		t.Fatalf("tombstones = %v; want none", tombs)
	}
}

func TestVanishedRootPreservesIndex(t *testing.T) {
	root := filepath.Join(t.TempDir(), "watched")
	mustWriteFile(t, filepath.Join(root, "important.txt"), []byte("do not delete on peer"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}

	// Simulate a renamed/moved folder or a temporarily unavailable mount.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	_, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err == nil {
		t.Fatal("expected an error for a vanished root that still has indexed files")
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted = %d; a vanished root must not tombstone indexed files", len(deleted))
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; index must be preserved", count)
	}
	if tombs := store.Snapshot().Tombstones; len(tombs) != 0 {
		t.Fatalf("tombstones = %v; want none", tombs)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("a scan must not recreate a vanished indexed root, stat err=%v", statErr)
	}

	// When the folder comes back, no deletion is emitted.
	mustWriteFile(t, filepath.Join(root, "important.txt"), []byte("do not delete on peer"))
	changed, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(deleted) != 0 {
		t.Fatalf("restored root: changed=%d deleted=%d err=%v", len(changed), len(deleted), err)
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count after restore = %d; want 1", count)
	}
}

func TestScanCreatesMissingRootForNewMapping(t *testing.T) {
	root := filepath.Join(t.TempDir(), "brand-new-folder")
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: "new", LocalPath: root, Enabled: true}}
	changed, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil {
		t.Fatalf("new mapping scan: %v", err)
	}
	if len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("new mapping scan = %d changed / %d deleted", len(changed), len(deleted))
	}
	if info, statErr := os.Stat(root); statErr != nil || !info.IsDir() {
		t.Fatalf("new folder root should be created: %v", statErr)
	}
}

func TestUnavailableRootIsErrorWithoutIndexChange(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "file.txt"), []byte("content"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}

	blocker := filepath.Join(t.TempDir(), "blocker")
	mustWriteFile(t, blocker, []byte("x"))
	folders[0].LocalPath = filepath.Join(blocker, "child")
	if _, deleted, err := store.ScanFoldersIncremental(folders, nil); err == nil {
		t.Fatal("expected error for a path nested under a regular file")
	} else if len(deleted) != 0 {
		t.Fatalf("deleted = %d; want 0", len(deleted))
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want 1", count)
	}
}

func TestScanIgnorePatternsAndDropWithoutTombstone(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "keep.txt"), []byte("keep"))
	mustWriteFile(t, filepath.Join(root, "skip.tmp"), []byte("tmp"))
	mustWriteFile(t, filepath.Join(root, "node_modules", "pkg.js"), []byte("js"))
	mustWriteFile(t, filepath.Join(root, ".git", "config"), []byte("git"))
	mustWriteFile(t, filepath.Join(root, "Thumbs.db"), []byte("thumb"))
	mustWriteFile(t, filepath.Join(root, ".synct.tmp.abc"), []byte("temp"))
	mustWriteFile(t, filepath.Join(root, "report.conflict-20260101-120000.000.docx"), []byte("backup"))

	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	changed, _, err := store.ScanFoldersIncremental(folders, config.DefaultIgnorePatterns())
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0].RelPath != "keep.txt" {
		t.Fatalf("changed = %+v; want only keep.txt", changed)
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want 1", count)
	}

	// Making an indexed file newly ignored drops it silently (no peer deletion).
	if _, deleted, err := store.ScanFoldersIncremental(folders, append(config.DefaultIgnorePatterns(), "keep.txt")); err != nil {
		t.Fatal(err)
	} else if len(deleted) != 0 {
		t.Fatalf("deleted = %d; want 0 for newly ignored file", len(deleted))
	}
	if _, ok := store.Snapshot().Tombstones["keep.txt"]; ok {
		t.Fatal("newly ignored file must not produce a tombstone")
	}
	if count, _ := store.Counts(); count != 0 {
		t.Fatalf("count = %d; want 0", count)
	}
}

func TestScanPendingWriteProtection(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "inflight.txt")
	mustWriteFile(t, target, []byte("v1"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}

	// Simulate the window between registering a download in the index and
	// renaming the temporary file into place.
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	store.BeginWrite("inflight.txt")
	_, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted = %d; persisted entry must survive an in-flight write", len(deleted))
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want 1", count)
	}

	// The guard must survive any number of scans while the transfer runs; it is
	// released only by EndWrite, because a slow download may take an hour.
	for i := 0; i < 3; i++ {
		if _, deleted, err = store.ScanFoldersIncremental(folders, nil); err != nil || len(deleted) != 0 {
			t.Fatalf("scan %d during in-flight write: deleted=%d err=%v", i, len(deleted), err)
		}
	}

	store.EndWrite("inflight.txt")
	_, deleted, err = store.ScanFoldersIncremental(folders, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || deleted[0].RelPath != "inflight.txt" {
		t.Fatalf("deleted = %+v; want inflight.txt after write ended", deleted)
	}
}

func TestTombstoneRetentionAndCap(t *testing.T) {
	store := NewIndexStore("")
	now := time.Now().UTC().UnixMilli()
	old := now - int64(tombstoneRetention/time.Millisecond) - 1000

	store.mu.Lock()
	store.tombstones["old"] = Tombstone{RelPath: "old", DeletedAt: old}
	store.tombstones["recent"] = Tombstone{RelPath: "recent", DeletedAt: now}
	store.pruneTombstonesLocked(now)
	store.mu.Unlock()

	if _, ok := store.tombstones["old"]; ok {
		t.Fatal("expired tombstone should be pruned")
	}
	if _, ok := store.tombstones["recent"]; !ok {
		t.Fatal("recent tombstone should be kept")
	}

	store.mu.Lock()
	for i := 0; i < tombstoneLimit+10; i++ {
		rel := fmt.Sprintf("t%05d", i)
		store.tombstones[rel] = Tombstone{RelPath: rel, DeletedAt: now + int64(i)}
	}
	store.pruneTombstonesLocked(now)
	kept := len(store.tombstones)
	_, oldestKept := store.tombstones["t00000"]
	_, newestKept := store.tombstones[fmt.Sprintf("t%05d", tombstoneLimit+9)]
	store.mu.Unlock()

	if kept != tombstoneLimit {
		t.Fatalf("tombstones kept = %d; want %d", kept, tombstoneLimit)
	}
	if oldestKept {
		t.Fatal("oldest tombstone should have been dropped by the cap")
	}
	if !newestKept {
		t.Fatal("newest tombstone should have been kept")
	}
}

func TestSaveLoadRoundTripAndCorruption(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "one.txt"), []byte("one"))
	mustWriteFile(t, filepath.Join(root, "dir", "two.txt"), []byte("two two"))
	statePath := filepath.Join(t.TempDir(), "state.json")

	store := NewIndexStore(statePath)
	if !strings.HasPrefix(store.NodeID(), "node-") {
		t.Fatalf("node id = %q", store.NodeID())
	}
	ids := map[string]bool{}
	for i := 0; i < 5; i++ {
		ids[NewIndexStore("").NodeID()] = true
	}
	if len(ids) < 2 {
		t.Fatal("new node IDs should not collide across instances")
	}

	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}
	store.RequestSave()
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("state file not written: %v", err)
	}

	loaded := NewIndexStore(statePath)
	if err := loaded.LoadError(); err != nil {
		t.Fatalf("load error: %v", err)
	}
	if loaded.NodeID() != store.NodeID() {
		t.Fatalf("node id = %q; want %q", loaded.NodeID(), store.NodeID())
	}
	if c1, b1 := loaded.Counts(); c1 != 2 || b1 != int64(len("one")+len("two two")) {
		t.Fatalf("loaded counts = %d / %d", c1, b1)
	}
	if changed, _, err := loaded.ScanFoldersIncremental(folders, nil); err != nil || len(changed) != 0 {
		t.Fatalf("rescan after load = %d changed, err=%v; want cached 0,nil", len(changed), err)
	}

	// A corrupt state file is preserved, not silently overwritten.
	if err := os.WriteFile(statePath, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	corrupt := NewIndexStore(statePath)
	if corrupt.LoadError() == nil {
		t.Fatal("expected LoadError for corrupt state")
	}
	if count, _ := corrupt.Counts(); count != 0 {
		t.Fatalf("corrupt state count = %d; want 0", count)
	}
	backups, _ := filepath.Glob(statePath + ".corrupt-*")
	if len(backups) == 0 {
		t.Fatal("expected a preserved .corrupt-* backup of the damaged state file")
	}
	corrupt.MarkFileSynced(FileMeta{RelPath: "manual.txt", Size: 3, SHA256: "abc"}, StatusSynced, "本地")
	if err := corrupt.Flush(); err != nil {
		t.Fatalf("flush after corrupt backup: %v", err)
	}
	if count, _ := NewIndexStore(statePath).Counts(); count != 1 {
		t.Fatalf("recovered state count = %d; want 1", count)
	}
}

func TestCleanEmptyParentsBoundary(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	sibling := filepath.Join(base, "root2")
	mustWriteFile(t, filepath.Join(root, "sub", "deep", "file.txt"), []byte("x"))
	mustWriteFile(t, filepath.Join(sibling, "keep.txt"), []byte("keep"))

	if err := os.Remove(filepath.Join(root, "sub", "deep", "file.txt")); err != nil {
		t.Fatal(err)
	}
	cleanEmptyParents(root, filepath.Join(root, "sub", "deep"))
	if _, err := os.Stat(filepath.Join(root, "sub")); !os.IsNotExist(err) {
		t.Fatalf("empty child directories should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("root must be preserved: %v", err)
	}

	// A sibling whose name shares the root prefix must never be removed.
	if err := os.Remove(filepath.Join(sibling, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	cleanEmptyParents(root, sibling)
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("sibling directory outside the root must be preserved: %v", err)
	}
}

func TestResolveFolderFullPath(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "main")
	proj := filepath.Join(base, "projects")
	cfg := config.Default()
	cfg.SyncDir = root
	cfg.SyncFolders = []config.FolderMapping{
		{ID: config.DefaultFolderID, LocalPath: root, Enabled: true},
		{ID: "proj", LocalPath: proj, Enabled: true},
		{ID: "off", LocalPath: proj, Enabled: false},
	}

	cases := []struct {
		rel  string
		ok   bool
		want string
	}{
		{"a.txt", true, filepath.Join(root, "a.txt")},
		{"sub/a.txt", true, filepath.Join(root, "sub", "a.txt")},
		{"@proj/docs/b.md", true, filepath.Join(proj, "docs", "b.md")},
		{"../escape.txt", false, ""},
		{"a/../../escape.txt", false, ""},
		{"a:b.txt", false, ""},
		{"@unknown/file.txt", false, ""},
		{"@off/should-not-write", false, ""},
		{"", false, ""},
	}
	for _, tc := range cases {
		_, _, got, ok := ResolveFolderFullPath(cfg, tc.rel)
		if ok != tc.ok {
			t.Errorf("ResolveFolderFullPath(%q) ok=%v; want %v", tc.rel, ok, tc.ok)
			continue
		}
		if ok && !strings.EqualFold(filepath.Clean(got), filepath.Clean(tc.want)) {
			t.Errorf("ResolveFolderFullPath(%q) = %q; want %q", tc.rel, got, tc.want)
		}
	}
}

func TestIgnoreMatcher(t *testing.T) {
	patterns := []string{"*.tmp", ".git", "node_modules", "~$*", "build"}
	cases := []struct {
		rel string
		dir bool
		hit bool
	}{
		{"report.txt", false, false},
		{"draft.tmp", false, true},
		{"sub/draft.tmp", false, true},
		{".git/config", false, true},
		{"a/.git", true, true},
		{"node_modules/pkg/index.js", false, true},
		{"x/node_modules", true, true},
		{"~$report.docx", false, true},
		{"build", true, true},
		{"src/build/output.bin", false, true},
		{".synct.tmp.123", false, true},
		{".synct-anything", false, true},
		{"", false, false},
	}
	matcher := newIgnoreMatcher(patterns)
	for _, tc := range cases {
		if got := matcher.Match(tc.rel, tc.dir); got != tc.hit {
			t.Errorf("Match(%q, dir=%v) = %v; want %v", tc.rel, tc.dir, got, tc.hit)
		}
	}

	if runtime.GOOS == "windows" {
		if !newIgnoreMatcher([]string{"Thumbs.db"}).Match("thumbs.db", false) {
			t.Error("Windows ignore matching should be case-insensitive")
		}
	}
}

func TestScanFolderSymlinkRootFollowsTarget(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	link := filepath.Join(base, "link")
	mustWriteFile(t, filepath.Join(target, "inside.txt"), []byte("inside"))
	if err := os.Symlink(target, link); err != nil {
		// Windows without SeCreateSymbolicLinkPrivilege can still create a
		// directory junction without elevation.
		cmd := exec.Command("cmd", "/c", "mklink", "/J", link, target)
		if out, jerr := cmd.CombinedOutput(); jerr != nil {
			t.Skipf("symlink/junction roots unavailable (symlink: %v; junction: %v: %s)", err, jerr, strings.TrimSpace(string(out)))
		}
		t.Cleanup(func() { _ = os.Remove(link) })
	}

	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: link, Enabled: true}}
	changed, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil {
		t.Fatalf("scan through symlink root: %v", err)
	}
	if len(changed) != 1 || changed[0].RelPath != "inside.txt" || len(deleted) != 0 {
		t.Fatalf("scan through symlink root = changed %+v deleted %+v", changed, deleted)
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want 1", count)
	}
}

func TestScanFolderIncrementalPreservesOtherFolders(t *testing.T) {
	base := t.TempDir()
	dirA := filepath.Join(base, "a")
	dirB := filepath.Join(base, "b")
	mustWriteFile(t, filepath.Join(dirA, "a.txt"), []byte("a"))
	mustWriteFile(t, filepath.Join(dirB, "b.txt"), []byte("b"))

	store := NewIndexStore("")
	folders := []config.FolderMapping{
		{ID: "alpha", LocalPath: dirA, Enabled: true},
		{ID: "beta", LocalPath: dirB, Enabled: true},
	}
	if changed, _, err := store.ScanFoldersIncremental(folders, nil); err != nil || len(changed) != 2 {
		t.Fatalf("initial scan = %d changed, err=%v", len(changed), err)
	}

	// Rescanning one folder must neither drop nor re-report the other folder.
	updated := []byte("b updated with more bytes")
	mustWriteFile(t, filepath.Join(dirB, "b.txt"), updated)
	changed, deleted, err := store.ScanFolderIncremental(folders[1], nil)
	if err != nil || len(deleted) != 0 {
		t.Fatalf("single-folder scan deleted=%d err=%v", len(deleted), err)
	}
	if len(changed) != 1 || changed[0].RelPath != "@beta/b.txt" {
		t.Fatalf("single-folder changed = %+v", changed)
	}
	if _, ok := store.FileMeta("@alpha/a.txt"); !ok {
		t.Fatal("file in the untouched folder disappeared from the index")
	}
	if count, bytes := store.Counts(); count != 2 || bytes != int64(len("a")+len(updated)) {
		t.Fatalf("counts = %d / %d", count, bytes)
	}

	// A full scan with the second mapping removed drops its entries.
	changed, _, err = store.ScanFoldersIncremental(folders[:1], nil)
	if err != nil || len(changed) != 0 {
		t.Fatalf("removal scan changed=%d err=%v", len(changed), err)
	}
	if _, ok := store.FileMeta("@beta/b.txt"); ok {
		t.Fatal("entries of a removed folder should be dropped")
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count after folder removal = %d; want 1", count)
	}
}

func TestInvalidateHashForcesRehash(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "hash.txt"), []byte("hash me"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}
	before, ok := store.FileMeta("hash.txt")
	if !ok || before.SHA256 == "" {
		t.Fatalf("expected cached hash, got %+v", before)
	}
	store.InvalidateHash("hash.txt")
	afterInvalidate, _ := store.FileMeta("hash.txt")
	if afterInvalidate.SHA256 != "" {
		t.Fatalf("hash should be cleared, got %q", afterInvalidate.SHA256)
	}
	changed, deleted, err := store.ScanFoldersIncremental(folders, nil)
	if err != nil || len(deleted) != 0 {
		t.Fatalf("rescan err=%v deleted=%d", err, len(deleted))
	}
	if len(changed) != 1 || changed[0].SHA256 != before.SHA256 {
		t.Fatalf("rescan should re-hash and report the file: %+v", changed)
	}
	if count, _ := store.Counts(); count != 1 {
		t.Fatalf("count = %d; want 1", count)
	}
}

func TestHotReadPathsAreAllocationFree(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "x.txt"), []byte("x"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		_, _ = store.Counts()
		_ = store.Generation()
		_ = store.Entries()
	}); allocs != 0 {
		t.Fatalf("cached count/entries reads allocate %.2f times per run; want 0", allocs)
	}

	t.Setenv("SYNCT_DATA_DIR", filepath.Join(t.TempDir(), "portable"))
	cfg := config.Default()
	cfg.SyncDir = root
	engine := NewEngine(cfg, filepath.Join(t.TempDir(), "state.json"), nil)
	defer engine.Stop()
	_ = engine.Stats()
	if allocs := testing.AllocsPerRun(200, func() { _ = engine.Stats() }); allocs > 1 {
		t.Fatalf("Stats allocates %.2f times per call; want <= 1", allocs)
	}
}

func TestEntriesCacheInvalidation(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "cache.txt"), []byte("cache"))
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		t.Fatal(err)
	}

	e1 := store.Entries()
	e2 := store.Entries()
	if reflect.ValueOf(e1).Pointer() != reflect.ValueOf(e2).Pointer() {
		t.Fatal("Entries() should reuse the cached slice for an unchanged generation")
	}
	versioned, gen := store.EntriesWithVersion()
	if gen != store.Generation() {
		t.Fatalf("EntriesWithVersion version = %d; Generation = %d", gen, store.Generation())
	}
	if reflect.ValueOf(versioned).Pointer() != reflect.ValueOf(e2).Pointer() {
		t.Fatal("EntriesWithVersion should return the same cached slice")
	}
	if e1[0].Direction != "本地" {
		t.Fatalf("direction = %q; want 本地", e1[0].Direction)
	}

	store.SetFileStatus("cache.txt", StatusSynced, "双向一致")
	e3 := store.Entries()
	if reflect.ValueOf(e3).Pointer() == reflect.ValueOf(e2).Pointer() {
		t.Fatal("Entries() should rebuild after a status/direction change")
	}
	if e3[0].Direction != "双向一致" {
		t.Fatalf("direction = %q; want 双向一致", e3[0].Direction)
	}
	store.SetFileStatus("cache.txt", StatusSynced, "双向一致")
	e4 := store.Entries()
	if reflect.ValueOf(e3).Pointer() != reflect.ValueOf(e4).Pointer() {
		t.Fatal("identical SetFileStatus should not invalidate the cache")
	}
}

func TestJoinSplitFolderRelPath(t *testing.T) {
	if got := JoinFolderRelPath("default", "/a/b.txt"); got != "a/b.txt" {
		t.Fatalf("default join = %q", got)
	}
	if got := JoinFolderRelPath("projects", "a/b.txt"); got != "@projects/a/b.txt" {
		t.Fatalf("mapped join = %q", got)
	}
	id, inner := SplitFolderRelPath("@projects/a/b.txt")
	if id != "projects" || inner != "a/b.txt" {
		t.Fatalf("split = (%q, %q)", id, inner)
	}
	id, inner = SplitFolderRelPath("plain.txt")
	if id != config.DefaultFolderID || inner != "plain.txt" {
		t.Fatalf("default split = (%q, %q)", id, inner)
	}
}

func BenchmarkScanFoldersIncremental2000(b *testing.B) {
	root := b.TempDir()
	for d := 0; d < 20; d++ {
		for f := 0; f < 100; f++ {
			path := filepath.Join(root, fmt.Sprintf("dir%02d", d), fmt.Sprintf("file%03d.txt", f))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				b.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("benchmark content"), 0o644); err != nil {
				b.Fatal(err)
			}
		}
	}
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	patterns := config.DefaultIgnorePatterns()
	if _, _, err := store.ScanFoldersIncremental(folders, patterns); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := store.ScanFoldersIncremental(folders, patterns); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexStats2000(b *testing.B) {
	root := b.TempDir()
	for f := 0; f < 2000; f++ {
		path := filepath.Join(root, fmt.Sprintf("file%04d.txt", f))
		if err := os.WriteFile(path, []byte("stats"), 0o644); err != nil {
			b.Fatal(err)
		}
	}
	store := NewIndexStore("")
	folders := []config.FolderMapping{{ID: config.DefaultFolderID, LocalPath: root, Enabled: true}}
	if _, _, err := store.ScanFoldersIncremental(folders, nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n, bytes := store.Counts(); n != 2000 || bytes <= 0 {
			b.Fatalf("unexpected counts %d / %d", n, bytes)
		}
	}
}
