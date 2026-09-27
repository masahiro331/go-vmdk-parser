package vmdk_test

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

func TestParseDiskDescriptor(t *testing.T) {
	tests := []struct {
		name      string
		inputFile string
		want      vmdk.DiskDescriptor
		wantErr   string
	}{
		{
			// Built by "qemu-img create -f vmdk vmdk-streamoptimized.img -o subformat=streamOptimized  65536" command
			// Check by "qemu-img info vmdk-streamoptimized" or "strings vmdk-streamoptimized"
			name:      "happy path for stream optimized",
			inputFile: "testdata/vmdk-streamoptimized.img",
			want: vmdk.DiskDescriptor{
				Version:    1,
				CID:        "8bc8c866", // CID is random string
				ParentCID:  "ffffffff", // No parent 0xFFFFFFFFFFFF
				CreateType: "streamOptimized",
				Extents: []vmdk.ExtentDescription{
					{
						Mode: "RW",
						Size: 128, // Size divided by sector size (65536 / 512)
						Type: "SPARSE",
						Name: "vmdk-streamoptimized.img",
					},
				},
			},
		},
		{
			// Built by "qemu-img create -f vmdk vmdk-monolith.img 65536" command
			// Check by "qemu-img info vmdk-monolith" or "strings vmdk-monolith"
			name:      "happy path for monolith",
			inputFile: "testdata/vmdk-monolith.img",
			want: vmdk.DiskDescriptor{
				Version:    1,
				ParentCID:  "ffffffff",
				CID:        "ba26f75f",
				CreateType: "monolithicSparse",
				Extents: []vmdk.ExtentDescription{
					{
						Mode: "RW",
						Size: 128, // Size divided by sector size (65536 / 512)
						Type: "SPARSE",
						Name: "vmdk-monolith.img",
					},
				},
			},
		},
		{
			name:      "sad path, no extent description",
			inputFile: "testdata/vmdk-invalid.img",
			wantErr:   "invalid descriptor",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.inputFile)
			if err != nil {
				t.Fatal(err)
			}
			header, err := vmdk.ParseHeader(f)
			if err != nil {
				t.Fatal(err)
			}

			got, err := vmdk.ParseDiskDescriptor(f, header)
			if (err != nil) && tt.wantErr == "" {
				t.Errorf("ParseDiskDescriptor() error = %v, wantErr %v", err, tt.wantErr)
			}
			if (err != nil) && tt.wantErr != "" {
				if err.Error() != tt.wantErr {
					t.Errorf("ParseDiskDescriptor() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseDiskDescriptor() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenMonolithicSparse(t *testing.T) {
	// Built by "qemu-img create -f vmdk vmdk-monolith.img 65536" command
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Disk is 128 sectors = 65536 bytes
	if sr.Size() != 65536 {
		t.Errorf("Size() = %d, want 65536", sr.Size())
	}

	// The disk is empty (all sparse), so reading should return zeros
	buf := make([]byte, 512)
	n, err := sr.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 512 {
		t.Errorf("ReadAt() n = %d, want 512", n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("ReadAt() byte[%d] = %d, want 0", i, b)
			break
		}
	}
}

func TestOpenMonolithicSparseZeroFill(t *testing.T) {
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-fill buffer with non-zero data to verify sparse regions are zeroed
	buf := make([]byte, 512)
	for i := range buf {
		buf[i] = 0xFF
	}

	n, err := sr.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 512 {
		t.Errorf("ReadAt() n = %d, want 512", n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("ReadAt() byte[%d] = 0x%02x, want 0x00 (sparse region must be zero-filled)", i, b)
			break
		}
	}
}

func TestReadAtVariousBufferSizes(t *testing.T) {
	// vmdk-monolith.img is a 65536-byte (128 sectors) empty monolithicSparse image
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	sizes := []int{1, 256, 512, 1024, 4096, 65536}
	for _, size := range sizes {
		buf := make([]byte, size)
		// Pre-fill with non-zero to verify zero-fill
		for i := range buf {
			buf[i] = 0xFF
		}

		n, err := sr.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt(size=%d) err = %v", size, err)
		}
		if n != size {
			t.Fatalf("ReadAt(size=%d) n = %d, want %d", size, n, size)
		}
		for i, b := range buf {
			if b != 0 {
				t.Errorf("ReadAt(size=%d) byte[%d] = 0x%02x, want 0x00", size, i, b)
				break
			}
		}
	}
}

func TestReadAtPartialEOF(t *testing.T) {
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read 1024 bytes starting at offset 65024 (512 bytes before EOF)
	buf := make([]byte, 1024)
	n, err := sr.ReadAt(buf, 65024)
	if err != io.EOF {
		t.Fatalf("ReadAt(65024) err = %v, want io.EOF", err)
	}
	if n != 512 {
		t.Fatalf("ReadAt(65024) n = %d, want 512", n)
	}
}

func TestReadAtBeyondEOF(t *testing.T) {
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 512)
	n, err := sr.ReadAt(buf, 65536)
	if err != io.EOF {
		t.Fatalf("ReadAt(65536) err = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Fatalf("ReadAt(65536) n = %d, want 0", n)
	}
}

func TestReadAll(t *testing.T) {
	f, err := os.Open("testdata/vmdk-monolith.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sr, err := vmdk.Open(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(io.NewSectionReader(sr, 0, sr.Size()))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 65536 {
		t.Fatalf("ReadAll() len = %d, want 65536", len(data))
	}
	for i, b := range data {
		if b != 0 {
			t.Errorf("ReadAll() byte[%d] = 0x%02x, want 0x00", i, b)
			break
		}
	}
}

// makeHeader creates a binary-encoded VMDK header for testing.
func makeHeader(h vmdk.Header) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, h)
	return buf.Bytes()
}

func TestParseHeaderIncompatFlags(t *testing.T) {
	validHeader := vmdk.Header{
		Signature: 0x564d444b, // KDMV
		Version:   1,
		GrainSize: 128,
	}

	tests := []struct {
		name    string
		flag    int32
		wantErr string
	}{
		{
			name: "no flags",
			flag: 0,
		},
		{
			name: "compressed flag only",
			flag: int32(0x00010000),
		},
		{
			name: "compressed and embedded_lba",
			flag: int32(0x00010000 | 0x00020000),
		},
		{
			name:    "unknown incompatible flag",
			flag:    int32(0x00040000),
			wantErr: "unknown incompatible flags",
		},
		{
			name:    "embedded_lba without compressed",
			flag:    int32(0x00020000),
			wantErr: "EMBEDDED_LBA flag requires COMPRESSED flag",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := validHeader
			h.Flag = tt.flag
			_, err := vmdk.ParseHeader(bytes.NewReader(makeHeader(h)))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseHeader() unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("ParseHeader() should return error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error should contain %q, got: %v", tt.wantErr, err)
				}
			}
		})
	}
}

func TestNewMonolithicSparseImageInvalidHeader(t *testing.T) {
	tests := []struct {
		name   string
		header vmdk.Header
	}{
		{
			name:   "GrainSize is zero",
			header: vmdk.Header{GrainSize: 0, NumGTEsPerGT: 512, GdOffset: 1, Capacity: 128},
		},
		{
			name:   "NumGTEsPerGT is zero",
			header: vmdk.Header{GrainSize: 128, NumGTEsPerGT: 0, GdOffset: 1, Capacity: 128},
		},
		{
			name:   "GdOffset is zero",
			header: vmdk.Header{GrainSize: 128, NumGTEsPerGT: 512, GdOffset: 0, Capacity: 128},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vmdk.VMDK{Header: tt.header}
			_, err := vmdk.NewMonolithicSparseImage(v)
			if err == nil {
				t.Fatal("NewMonolithicSparseImage() should return error for invalid header")
			}
		})
	}
}

// The data images hold 4 grains (256KiB) of known content:
//
//	grain 0: 0xab
//	grain 1: unallocated (reads as zero)
//	grain 2: byte i%251 at in-grain offset i
//	grain 3: 0xcd
//
// testdata/vmdk-monolith-data.img is regenerated with qemu-img
// (e.g. inside `docker run --rm -it -v "$PWD:/d" -w /d alpine sh`, `apk add qemu-img`):
//
//	dd if=/dev/zero of=raw.img bs=65536 count=4
//	printf "%0.s\253" $(seq 1 65536) | dd of=raw.img bs=65536 seek=0 conv=notrunc
//	awk 'BEGIN{for(i=0;i<65536;i++) printf "%c", i%251}' LC_ALL=C | dd of=raw.img bs=65536 seek=2 conv=notrunc
//	printf "%0.s\315" $(seq 1 65536) | dd of=raw.img bs=65536 seek=3 conv=notrunc
//	qemu-img convert -f raw -O vmdk -o subformat=monolithicSparse raw.img vmdk-monolith-data.img
//
// The stream-optimized image is built in memory by buildStreamOptimized instead,
// because qemu-img writes the grain directory right after the header and no footer,
// while this parser reads the VMware layout (GD_AT_END with a footer).
const (
	dataFixtureGrain = 65536
	dataFixtureSize  = 4 * dataFixtureGrain
)

// buildStreamOptimized lays out a VMware stream-optimized extent:
// header, descriptor, grain markers, GT marker + GT, GD marker + GD,
// footer marker + footer, end-of-stream marker.
// Grain 2 is stored without compression so that its marker is followed by
// more sectors, and all-zero grains are left unallocated.
func buildStreamOptimized(t *testing.T, content []byte) []byte {
	t.Helper()
	const (
		sector       = 512
		grainSectors = dataFixtureGrain / sector
		gtes         = 512
		descSectors  = 20
		overhead     = 128 // first grain starts at sector 128
	)
	const flags = 0x00000001 | 0x00010000 | 0x00020000 // valid NL test, compressed, markers
	gdAtEnd := int64(-1)

	var out bytes.Buffer
	pad := func() {
		if r := out.Len() % sector; r != 0 {
			out.Write(make([]byte, sector-r))
		}
	}
	curSector := func() int64 { return int64(out.Len() / sector) }
	writeMarker := func(value uint64, size, typ uint32) {
		m := make([]byte, sector)
		binary.LittleEndian.PutUint64(m[0:8], value)
		binary.LittleEndian.PutUint32(m[8:12], size)
		binary.LittleEndian.PutUint32(m[12:16], typ)
		out.Write(m)
	}
	sparseHeader := func(gdOffset int64) []byte {
		var b bytes.Buffer
		binary.Write(&b, binary.LittleEndian, vmdk.SparseExtentHeader{
			MagicNumber:        vmdk.KDMV,
			Version:            3,
			Flags:              flags,
			Capacity:           uint64(len(content) / sector),
			GrainSize:          grainSectors,
			DescriptorOffset:   1,
			DescriptorSize:     descSectors,
			NumberGTEsPerGT:    gtes,
			GdOffset:           uint64(gdOffset),
			OverHead:           overhead,
			SingleEndLineChar:  '\n',
			NonEndLineChar:     ' ',
			DoubleEndLineChar1: '\r',
			DoubleEndLineChar2: '\n',
			CompressAlgorithm:  1,
		})
		return append(b.Bytes(), make([]byte, sector-b.Len())...)
	}

	out.Write(sparseHeader(gdAtEnd))
	out.WriteString("# Disk DescriptorFile\nversion=1\nCID=12345678\nparentCID=ffffffff\ncreateType=\"streamOptimized\"\n\n" +
		"# Extent description\nRW 512 SPARSE \"vmdk-streamoptimized-data.img\"\n\n# The Disk Data Base\n#DDB\n\nddb.virtualHWVersion = \"4\"\n")
	out.Write(make([]byte, (overhead*sector)-out.Len()))

	gt := make([]uint32, gtes)
	for g := 0; g*dataFixtureGrain < len(content); g++ {
		data := content[g*dataFixtureGrain : (g+1)*dataFixtureGrain]
		if bytes.Equal(data, make([]byte, dataFixtureGrain)) {
			continue
		}
		level := zlib.DefaultCompression
		if g == 2 {
			level = zlib.NoCompression
		}
		var z bytes.Buffer
		zw, err := zlib.NewWriterLevel(&z, level)
		if err != nil {
			t.Fatal(err)
		}
		zw.Write(data)
		zw.Close()

		gt[g] = uint32(curSector())
		var m [12]byte
		binary.LittleEndian.PutUint64(m[0:8], uint64(g*grainSectors))
		binary.LittleEndian.PutUint32(m[8:12], uint32(z.Len()))
		out.Write(m[:])
		out.Write(z.Bytes())
		pad()
	}

	gtSectors := uint64(gtes * 4 / sector)
	writeMarker(gtSectors, 0, vmdk.MARKER_GT)
	gtOffset := curSector()
	binary.Write(&out, binary.LittleEndian, gt)

	writeMarker(1, 0, vmdk.MARKER_GD)
	gdOffset := curSector()
	binary.Write(&out, binary.LittleEndian, []uint32{uint32(gtOffset)})
	pad()

	writeMarker(1, 0, 3) // footer marker
	out.Write(sparseHeader(gdOffset))
	out.Write(make([]byte, sector)) // end-of-stream marker
	return out.Bytes()
}

func dataFixtureContent() []byte {
	b := make([]byte, dataFixtureSize)
	for i := range b {
		switch i / dataFixtureGrain {
		case 0:
			b[i] = 0xab
		case 2:
			b[i] = byte((i % dataFixtureGrain) % 251)
		case 3:
			b[i] = 0xcd
		}
	}
	return b
}

// readSeekerOnly hides io.ReaderAt so that Open falls back to Seek+Read.
type readSeekerOnly struct {
	rs io.ReadSeeker
}

func (r *readSeekerOnly) Read(p []byte) (int, error) { return r.rs.Read(p) }

func (r *readSeekerOnly) Seek(offset int64, whence int) (int64, error) {
	return r.rs.Seek(offset, whence)
}

func openDataFixture(t *testing.T, image func(*testing.T) []byte, readerAt bool) *io.SectionReader {
	t.Helper()
	var rs io.ReadSeeker = bytes.NewReader(image(t))
	if !readerAt {
		rs = &readSeekerOnly{rs: rs}
	}
	sr, err := vmdk.Open(rs, nil)
	if err != nil {
		t.Fatalf("Open() err = %v", err)
	}
	if sr.Size() != dataFixtureSize {
		t.Fatalf("Size() = %d, want %d", sr.Size(), dataFixtureSize)
	}
	return sr
}

func readerMode(readerAt bool) string {
	if readerAt {
		return "ReaderAt"
	}
	return "Seek+Read"
}

var dataFixtures = []struct {
	name  string
	image func(*testing.T) []byte
}{
	{
		name: "stream optimized",
		image: func(t *testing.T) []byte {
			return buildStreamOptimized(t, dataFixtureContent())
		},
	},
	{
		name: "monolithic sparse",
		image: func(t *testing.T) []byte {
			b, err := os.ReadFile("testdata/vmdk-monolith-data.img")
			if err != nil {
				t.Fatal(err)
			}
			return b
		},
	},
}

func TestReadAllWithData(t *testing.T) {
	want := dataFixtureContent()
	for _, fx := range dataFixtures {
		for _, readerAt := range []bool{true, false} {
			t.Run(fx.name+"/"+readerMode(readerAt), func(t *testing.T) {
				sr := openDataFixture(t, fx.image, readerAt)
				got, err := io.ReadAll(io.NewSectionReader(sr, 0, sr.Size()))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("ReadAll() content mismatch (readerAt=%v)", readerAt)
				}
			})
		}
	}
}

func TestConcurrentReadAt(t *testing.T) {
	want := dataFixtureContent()
	// Reads that start in, end in or span every grain, including the unallocated one.
	reads := []struct {
		off  int64
		size int
	}{
		{0, 512},
		{100, 4096},
		{dataFixtureGrain - 10, 20},
		{dataFixtureGrain, dataFixtureGrain},
		{2*dataFixtureGrain + 1234, 777},
		{2*dataFixtureGrain - 100, dataFixtureGrain + 200},
		{3 * dataFixtureGrain, dataFixtureGrain},
		{0, dataFixtureSize},
	}

	const (
		iterations = 20
		workers    = 16
	)
	for _, fx := range dataFixtures {
		for _, readerAt := range []bool{true, false} {
			t.Run(fx.name+"/"+readerMode(readerAt), func(t *testing.T) {
				for it := 0; it < iterations; it++ {
					// Open a fresh image each iteration so that workers race on the
					// first fill of the grain table cache.
					sr := openDataFixture(t, fx.image, readerAt)

					var wg sync.WaitGroup
					start := make(chan struct{})
					errs := make(chan string, workers*len(reads))
					for w := 0; w < workers; w++ {
						wg.Add(1)
						go func(w int) {
							defer wg.Done()
							<-start
							for i := range reads {
								r := reads[(i+w)%len(reads)]
								buf := make([]byte, r.size)
								n, err := sr.ReadAt(buf, r.off)
								if err != nil {
									errs <- err.Error()
									return
								}
								if n != r.size || !bytes.Equal(buf, want[r.off:r.off+int64(r.size)]) {
									errs <- "content mismatch"
									return
								}
							}
						}(w)
					}
					close(start)
					wg.Wait()
					close(errs)
					for e := range errs {
						t.Fatalf("ReadAt() (readerAt=%v): %s", readerAt, e)
					}
				}
			})
		}
	}
}
