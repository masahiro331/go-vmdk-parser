package vmdk

import (
	"encoding/binary"
	"io"
	"log"
	"strings"

	"golang.org/x/xerrors"
)

var Sector = 0x200

type Header struct {
	Signature          uint32
	Version            uint32
	Flag               uint32
	Capacity           uint32
	GrainSize          uint32
	DescriptorOffset   uint32
	DescriptorSize     uint32
	NumGTEsPerGT       uint32
	RgdOffset          uint32
	GdOffset           uint32
	OverHead           uint32
	UncleanShutdown    byte
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
	Padding            [433]byte
}

// Expected CompressedGrainData or GrainData
type VMDK interface {
	io.ReadCloser
}

// WARN: NewVMDKReader read Header
func NewVMDKReader(r io.Reader) (io.ReadCloser, error) {
	sector := make([]byte, Sector)
	if _, err := r.Read(sector); err != nil {
		return nil, xerrors.Errorf("failed to new vmdk reader error: %w", err)
	}

	var header Header
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, xerrors.Errorf("failed to read binary error: %w", err)
	}

	// TODO: RAW extent data file is unsupported
	if header.DescriptorOffset == 0 {
		return nil, xerrors.New("Unsupported vmdk format")
	}

	var embDescriptor string
	for i := uint32(0); i < header.DescriptorSize; i++ {
		if _, err := r.Read(sector); err != nil {
			log.Fatal(err)
		}
		embDescriptor = embDescriptor + string(sector)
	}

	extent, err := newExtent(embDescriptor, header, r)
	if err != nil {
		return nil, xerrors.Errorf("failed to new vmdk reader: %w", err)
	}

	return extent, nil
}

// TODO: Parse EmbededDescriptor
// https://www.vmware.com/support/developer/vddk/vmdk_50_technote.pdf
func newExtent(s string, header Header, r io.Reader) (VMDK, error) {
	if strings.Contains(s, `createType="streamOptimized"`) {
		return &StreamOptimizedExtent{
			header: header,
			reader: r,
		}, nil
	} else {
		return nil, xerrors.New("Unsupported createType")
	}
}
