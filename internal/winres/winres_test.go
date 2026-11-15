package winres

import (
	"os"
	"path/filepath"
	"testing"

	appui "synct/internal/ui"
)

func TestGenerateWindowsSysoAndICO(t *testing.T) {
	pngData := appui.RawIconPNG()
	if len(pngData) == 0 {
		t.Fatal("expected non-empty embedded bailian-color.png")
	}

	sysoData, err := BuildSyso(pngData)
	if err != nil {
		t.Fatalf("BuildSyso failed: %v", err)
	}
	if len(sysoData) < 1024 {
		t.Fatalf("unexpectedly small syso output: %d bytes", len(sysoData))
	}

	icoData, err := BuildICO(pngData)
	if err != nil {
		t.Fatalf("BuildICO failed: %v", err)
	}
	if len(icoData) < 1024 {
		t.Fatalf("unexpectedly small ico output: %d bytes", len(icoData))
	}

	// Write rsrc_windows_amd64.syso into project root so `go build` embeds the icon and asInvoker manifest
	rootSyso := filepath.Join("..", "..", "rsrc_windows_amd64.syso")
	if err := os.WriteFile(rootSyso, sysoData, 0o644); err != nil {
		t.Fatalf("write %s failed: %v", rootSyso, err)
	}
}
