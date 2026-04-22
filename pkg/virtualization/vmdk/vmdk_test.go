package vmdk_test

import (
	"os"
	"reflect"
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
