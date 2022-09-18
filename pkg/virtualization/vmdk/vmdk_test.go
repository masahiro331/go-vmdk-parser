package vmdk_test

import (
	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
	"os"
	"reflect"
	"testing"
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
