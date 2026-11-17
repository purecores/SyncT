package winres

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"slices"
	"unicode/utf16"
)

// AppVersion is the version stamped into the executable's resources. Bump it
// with the release tag so Windows' file properties match the published build.
const AppVersion = "1.2.0"

var windowsIconSizes = []int{16, 24, 32, 48, 64, 128, 256}

// BuildSyso generates a Windows COFF (.syso) object file for amd64 containing:
// 1. Multi-resolution RT_ICON + RT_GROUP_ICON from `pngBytes` (bailian-color.png)
// 2. RT_MANIFEST with `requestedExecutionLevel level="asInvoker"` (User privilege restraint), PerMonitorV2 DPI, and UTF-8
// 3. RT_VERSION with SyncT version metadata
func BuildSyso(pngBytes []byte) ([]byte, error) {
	const (
		machine     uint16 = 0x8664 // AMD64
		relocation  uint16 = 0x0003 // IMAGE_REL_AMD64_ADDR32NB
		rtIcon      uint16 = 3
		rtGroupIcon uint16 = 14
		rtVersion   uint16 = 16
		rtManifest  uint16 = 24
		langID      uint16 = 0x0409 // en-US / standard resource language
	)

	images, err := renderIconSizes(pngBytes)
	if err != nil {
		return nil, fmt.Errorf("render icon sizes: %w", err)
	}

	res := map[uint16]map[uint16][]byte{
		rtIcon:     {},
		rtVersion:  {1: buildVersionInfo()},
		rtManifest: {1: []byte(buildAsInvokerManifest())},
	}

	var group bytes.Buffer
	_ = binary.Write(&group, binary.LittleEndian, [3]uint16{0, 1, uint16(len(images))})
	for i, img := range images {
		id := uint16(i + 1)
		res[rtIcon][id] = img
		dim := byte(windowsIconSizes[i])
		if windowsIconSizes[i] >= 256 {
			dim = 0
		}
		group.Write([]byte{dim, dim, 0, 0})
		_ = binary.Write(&group, binary.LittleEndian, [2]uint16{1, 32})
		_ = binary.Write(&group, binary.LittleEndian, uint32(len(img)))
		_ = binary.Write(&group, binary.LittleEndian, id)
	}
	res[rtGroupIcon] = map[uint16][]byte{1: group.Bytes()}

	types := sortedKeys(res)
	dirSize := func(n int) int { return 16 + 8*n }
	size := dirSize(len(types))
	for _, t := range types {
		size += dirSize(len(res[t])) + len(res[t])*dirSize(1)
	}
	entriesAt := size
	count := 0
	for _, t := range types {
		count += len(res[t])
	}
	dataAt := entriesAt + 16*count

	raw := make([]byte, dataAt)
	le := binary.LittleEndian
	dir := func(at, n int) {
		le.PutUint16(raw[at+14:], uint16(n))
	}
	var relocs []uint32
	var data bytes.Buffer
	dir(0, len(types))
	next := dirSize(len(types))
	entry := entriesAt
	for i, t := range types {
		le.PutUint32(raw[16+8*i:], uint32(t))
		le.PutUint32(raw[16+8*i+4:], uint32(next)|0x80000000)
		ids := sortedKeys(res[t])
		idDir := next
		dir(idDir, len(ids))
		next += dirSize(len(ids))
		for j, id := range ids {
			le.PutUint32(raw[idDir+16+8*j:], uint32(id))
			le.PutUint32(raw[idDir+16+8*j+4:], uint32(next)|0x80000000)
			dir(next, 1)
			le.PutUint32(raw[next+16:], uint32(langID))
			le.PutUint32(raw[next+20:], uint32(entry))
			next += dirSize(1)

			for data.Len()%8 != 0 {
				data.WriteByte(0)
			}
			le.PutUint32(raw[entry:], uint32(dataAt+data.Len()))
			le.PutUint32(raw[entry+4:], uint32(len(res[t][id])))
			relocs = append(relocs, uint32(entry))
			entry += 16
			data.Write(res[t][id])
		}
	}
	raw = append(raw, data.Bytes()...)
	for len(raw)%4 != 0 {
		raw = append(raw, 0)
	}

	var out bytes.Buffer
	const headerSize, sectionHeaderSize = 20, 40
	rawAt := headerSize + sectionHeaderSize
	relocAt := rawAt + len(raw)
	symbolsAt := relocAt + 10*len(relocs)
	_ = binary.Write(&out, le, struct {
		Machine, Sections                   uint16
		Time, Symbols, SymbolCount          uint32
		OptionalHeaderSize, Characteristics uint16
	}{machine, 1, 0, uint32(symbolsAt), 1, 0, 0})
	_ = binary.Write(&out, le, struct {
		Name                                        [8]byte
		VirtualSize, VirtualAddress, RawSize, RawAt uint32
		RelocAt, LineNumbersAt                      uint32
		Relocs, LineNumbers                         uint16
		Characteristics                             uint32
	}{[8]byte{'.', 'r', 's', 'r', 'c'}, 0, 0, uint32(len(raw)), uint32(rawAt), uint32(relocAt), 0, uint16(len(relocs)), 0, 0x40000040})
	out.Write(raw)
	for _, r := range relocs {
		_ = binary.Write(&out, le, r)
		_ = binary.Write(&out, le, uint32(0))
		_ = binary.Write(&out, le, relocation)
	}
	out.Write([]byte{'.', 'r', 's', 'r', 'c', 0, 0, 0})
	_ = binary.Write(&out, le, uint32(0))
	_ = binary.Write(&out, le, uint16(1))
	_ = binary.Write(&out, le, uint16(0))
	out.Write([]byte{3, 0})
	_ = binary.Write(&out, le, uint32(4))
	return out.Bytes(), nil
}

// BuildICO renders a multi-size Windows .ico file from `pngBytes`.
func BuildICO(pngBytes []byte) ([]byte, error) {
	images, err := renderIconSizes(pngBytes)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	_ = binary.Write(&out, binary.LittleEndian, [3]uint16{0, 1, uint16(len(windowsIconSizes))})
	offset := 6 + 16*len(windowsIconSizes)
	for i, s := range windowsIconSizes {
		dim := byte(s)
		if s >= 256 {
			dim = 0
		}
		out.Write([]byte{dim, dim, 0, 0})
		_ = binary.Write(&out, binary.LittleEndian, [2]uint16{1, 32})
		_ = binary.Write(&out, binary.LittleEndian, [2]uint32{uint32(len(images[i])), uint32(offset)})
		offset += len(images[i])
	}
	for _, p := range images {
		out.Write(p)
	}
	return out.Bytes(), nil
}

func renderIconSizes(src []byte) ([][]byte, error) {
	img, err := png.Decode(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	images := make([][]byte, len(windowsIconSizes))
	for i, s := range windowsIconSizes {
		var p bytes.Buffer
		if err := png.Encode(&p, resizeImage(img, s)); err != nil {
			return nil, err
		}
		images[i] = p.Bytes()
	}
	return images, nil
}

func resizeImage(img image.Image, size int) *image.NRGBA {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	dst := image.NewNRGBA(image.Rect(0, 0, size, size))
	if w == 0 || h == 0 {
		return dst
	}
	sx, sy := float64(w)/float64(size), float64(h)/float64(size)
	at := func(x, y int) (float64, float64, float64, float64) {
		c := color.NRGBAModel.Convert(img.At(b.Min.X+min(max(x, 0), w-1), b.Min.Y+min(max(y, 0), h-1))).(color.NRGBA)
		a := float64(c.A)
		return float64(c.R) * a, float64(c.G) * a, float64(c.B) * a, a
	}
	for y := range size {
		for x := range size {
			var r, g, bl, a, n float64
			if sx >= 1 {
				x0, x1 := int(float64(x)*sx), max(int(float64(x+1)*sx), int(float64(x)*sx)+1)
				y0, y1 := int(float64(y)*sy), max(int(float64(y+1)*sy), int(float64(y)*sy)+1)
				for yy := y0; yy < y1; yy++ {
					for xx := x0; xx < x1; xx++ {
						cr, cg, cb, ca := at(xx, yy)
						r, g, bl, a, n = r+cr, g+cg, bl+cb, a+ca, n+1
					}
				}
			} else {
				fx, fy := (float64(x)+0.5)*sx-0.5, (float64(y)+0.5)*sy-0.5
				x0, y0 := int(fx), int(fy)
				dx, dy := fx-float64(x0), fy-float64(y0)
				for _, s := range [4]struct {
					x, y int
					wt   float64
				}{{x0, y0, (1 - dx) * (1 - dy)}, {x0 + 1, y0, dx * (1 - dy)}, {x0, y0 + 1, (1 - dx) * dy}, {x0 + 1, y0 + 1, dx * dy}} {
					cr, cg, cb, ca := at(s.x, s.y)
					r, g, bl, a, n = r+cr*s.wt, g+cg*s.wt, bl+cb*s.wt, a+ca*s.wt, n+s.wt
				}
			}
			if a == 0 || n == 0 {
				continue
			}
			dst.SetNRGBA(x, y, color.NRGBA{
				R: uint8(r/a + 0.5),
				G: uint8(g/a + 0.5),
				B: uint8(bl/a + 0.5),
				A: uint8(a/n + 0.5),
			})
		}
	}
	return dst
}

func buildAsInvokerManifest() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <assemblyIdentity type="win32" name="cyou.suosmanthus.synctreei" version="1.2.0.0" processorArchitecture="*"/>
  <dependency>
    <dependentAssembly>
      <assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls" version="6.0.0.0" processorArchitecture="*" publicKeyToken="6595b64144ccf1df" language="*"/>
    </dependentAssembly>
  </dependency>
  <trustInfo xmlns="urn:schemas-microsoft-com:asm.v3">
    <security>
      <requestedPrivileges>
        <requestedExecutionLevel level="asInvoker" uiAccess="false"/>
      </requestedPrivileges>
    </security>
  </trustInfo>
  <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1">
    <application>
      <supportedOS Id="{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}"/>
    </application>
  </compatibility>
  <application xmlns="urn:schemas-microsoft-com:asm.v3">
    <windowsSettings>
      <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
      <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2</dpiAwareness>
      <activeCodePage xmlns="http://schemas.microsoft.com/SMI/2019/WindowsSettings">UTF-8</activeCodePage>
    </windowsSettings>
  </application>
</assembly>
`
}

type verNode struct {
	key      string
	text     bool
	value    []byte
	valueLen int
	children []verNode
}

func (n verNode) encode() []byte {
	b := make([]byte, 6)
	for _, u := range utf16.Encode([]rune(n.key + "\x00")) {
		b = binary.LittleEndian.AppendUint16(b, u)
	}
	pad := func() {
		for len(b)%4 != 0 {
			b = append(b, 0)
		}
	}
	pad()
	b = append(b, n.value...)
	for _, child := range n.children {
		pad()
		b = append(b, child.encode()...)
	}
	binary.LittleEndian.PutUint16(b[0:], uint16(len(b)))
	binary.LittleEndian.PutUint16(b[2:], uint16(n.valueLen))
	if n.text {
		binary.LittleEndian.PutUint16(b[4:], 1)
	}
	return b
}

func buildVersionInfo() []byte {
	fixed := make([]byte, 52)
	le := binary.LittleEndian
	le.PutUint32(fixed[0:], 0xFEEF04BD)
	le.PutUint32(fixed[4:], 0x00010000)
	le.PutUint32(fixed[8:], 1<<16|0)
	le.PutUint32(fixed[12:], 0)
	copy(fixed[16:24], fixed[8:16])
	le.PutUint32(fixed[24:], 0x3F)
	le.PutUint32(fixed[32:], 0x00040004)
	le.PutUint32(fixed[36:], 1)

	var table []verNode
	for _, s := range [][2]string{
		{"CompanyName", "SyncT Portable"},
		{"FileDescription", "SyncT — Cloudflared Portable Folder Sync (synctreei.suosmanthus.cyou)"},
		{"FileVersion", AppVersion},
		{"InternalName", "SyncT.exe"},
		{"OriginalFilename", "SyncT.exe"},
		{"ProductName", "SyncT"},
		{"ProductVersion", AppVersion},
		{"MyGoIdentifier", "cyou.suosmanthus.synctreei"},
	} {
		text := utf16.Encode([]rune(s[1] + "\x00"))
		value := make([]byte, 2*len(text))
		for i, u := range text {
			le.PutUint16(value[2*i:], u)
		}
		table = append(table, verNode{key: s[0], text: true, value: value, valueLen: len(text)})
	}

	translation := []byte{0x09, 0x04, 0xB0, 0x04}
	root := verNode{
		key:      "VS_VERSION_INFO",
		value:    fixed,
		valueLen: len(fixed),
		children: []verNode{
			{key: "StringFileInfo", text: true, children: []verNode{{key: "040904b0", text: true, children: table}}},
			{key: "VarFileInfo", text: true, children: []verNode{{key: "Translation", value: translation, valueLen: len(translation)}}},
		},
	}
	return root.encode()
}

func sortedKeys[V any](m map[uint16]V) []uint16 {
	keys := make([]uint16, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
