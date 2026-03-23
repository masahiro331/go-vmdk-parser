package vmdk_test

import (
	"os"
	"reflect"
	"strings"
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

func TestParseTextDescriptorWithWhitespace(t *testing.T) {
	// Descriptor with leading/trailing whitespace on lines
	// See: https://github.com/masahiro331/go-vmdk-parser/pull/7
	// See: https://github.com/aquasecurity/trivy/discussions/10323
	descriptor := "  # Disk DescriptorFile  \n" +
		"  version=1  \n" +
		"  CID=12345678  \n" +
		"  parentCID=ffffffff  \n" +
		"  createType=\"monolithicSparse\"  \n" +
		"\n" +
		"  # Extent description  \n" +
		"  RW 128 SPARSE \"test.vmdk\"  \n" +
		"\n" +
		"  # The Disk Data Base  \n" +
		"  ddb.virtualHWVersion = \"4\"  \n"

	dd, err := vmdk.ParseTextDescriptor(strings.NewReader(descriptor))
	if err != nil {
		t.Fatal(err)
	}

	if dd.Version != 1 {
		t.Errorf("Version = %d, want 1", dd.Version)
	}
	if dd.CID != "12345678" {
		t.Errorf("CID = %s, want 12345678", dd.CID)
	}
	if dd.CreateType != "monolithicSparse" {
		t.Errorf("CreateType = %s, want monolithicSparse", dd.CreateType)
	}
	if len(dd.Extents) != 1 {
		t.Fatalf("len(Extents) = %d, want 1", len(dd.Extents))
	}
	ext := dd.Extents[0]
	if ext.Mode != "RW" {
		t.Errorf("Extent.Mode = %s, want RW", ext.Mode)
	}
	if ext.Size != 128 {
		t.Errorf("Extent.Size = %d, want 128", ext.Size)
	}
	if ext.Type != "SPARSE" {
		t.Errorf("Extent.Type = %s, want SPARSE", ext.Type)
	}
	if ext.Name != "test.vmdk" {
		t.Errorf("Extent.Name = %s, want test.vmdk", ext.Name)
	}
}
