package ui

import (
	"bytes"
	_ "embed"
	"image"
	"image/color"
	"image/png"

	"github.com/egoist/mygo/ui"
)

//go:embed app_icon.png
var appIconPNG []byte

// AppLogoBitmap is the decoded bitmap of bailian-color.png for native UI display.
var AppLogoBitmap, _ = ui.DecodeBitmap(appIconPNG)

// Embedded vector icons parsed once with ui.MustParseSVG for use across the native UI and menus.
var (
	IconCloud = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17.5 19H9a7 7 0 1 1 6.71-9h1.79a4.5 4.5 0 1 1 0 9Z"/></svg>`))

	IconFolder = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z"/></svg>`))

	IconActivity = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>`))

	IconSettings = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>`))

	IconSync = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg>`))

	IconShield = ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/></svg>`))
)

// RawIconPNG returns the raw embedded PNG bytes of bailian-color.png.
func RawIconPNG() []byte {
	return appIconPNG
}

// TrayIconPNG generates a 64x64 scaled PNG icon from bailian-color.png for the Windows 11 system tray.
func TrayIconPNG() []byte {
	srcImg, err := png.Decode(bytes.NewReader(appIconPNG))
	if err != nil {
		return appIconPNG
	}
	scaled := scaleSquare(srcImg, 64)
	var buf bytes.Buffer
	if err := png.Encode(&buf, scaled); err != nil {
		return appIconPNG
	}
	return buf.Bytes()
}

func scaleSquare(img image.Image, size int) *image.NRGBA {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	dst := image.NewNRGBA(image.Rect(0, 0, size, size))
	if w == 0 || h == 0 {
		return dst
	}
	sx, sy := float64(w)/float64(size), float64(h)/float64(size)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			x0, x1 := int(float64(x)*sx), max(int(float64(x+1)*sx), int(float64(x)*sx)+1)
			y0, y1 := int(float64(y)*sy), max(int(float64(y+1)*sy), int(float64(y)*sy)+1)
			var r, g, bl, a, n float64
			for yy := y0; yy < y1; yy++ {
				for xx := x0; xx < x1; xx++ {
					c := color.NRGBAModel.Convert(img.At(b.Min.X+min(max(xx, 0), w-1), b.Min.Y+min(max(yy, 0), h-1))).(color.NRGBA)
					fa := float64(c.A)
					r += float64(c.R) * fa
					g += float64(c.G) * fa
					bl += float64(c.B) * fa
					a += fa
					n++
				}
			}
			if a > 0 && n > 0 {
				dst.SetNRGBA(x, y, color.NRGBA{
					R: uint8(r/a + 0.5),
					G: uint8(g/a + 0.5),
					B: uint8(bl/a + 0.5),
					A: uint8(a/n + 0.5),
				})
			}
		}
	}
	return dst
}
