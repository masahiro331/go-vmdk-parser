package vmdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk"
	"golang.org/x/xerrors"
)

var Sector = 0x200

type Header struct {
	Signature          uint32
	Version            uint32
	Flag               uint32
	Capacity           uint64
	GrainSize          uint64
	DescriptorOffset   uint64
	DescriptorSize     uint64
	NumGTEsPerGT       uint32
	RgdOffset          uint64
	GdOffset           uint64
	OverHead           uint64
	UncleanShutdown    byte
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
	Padding            [433]byte
}

type FileMap map[string][][]byte

// Expected CompressedGrainData or GrainData
type VMDK interface {
	// Extract(ctx context.Context, imageName string, filenames []string) (FileMap, error)
	ExtractFromFile(r io.Reader, filenames []string) (FileMap, error)
}

type Reader interface {
	io.ReadCloser
	Next() (*disk.Partition, error)
}

func NewReader(r io.Reader, dict []byte) (Reader, error) {
	var header Header
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, xerrors.Errorf("failed to read binary error: %w", err)
	}

	// TODO: RAW extent data file is unsupported
	if header.DescriptorOffset == 0 {
		return nil, xerrors.New("Unsupported vmdk format")
	}

	sector := make([]byte, Sector)
	var embDescriptor string
	for i := uint64(0); i < header.DescriptorSize; i++ {
		if _, err := r.Read(sector); err != nil {
			log.Fatal(err)
		}
		embDescriptor = embDescriptor + string(sector)
	}
	fmt.Println(embDescriptor)

	readerFunc, err := newReaderFunc(embDescriptor, header)
	if err != nil {
		return nil, xerrors.Errorf("failed to new vmdk reader: %w", err)
	}

	return readerFunc(r, dict, header)
}

// WARN: NewVMDKReader read Header
func NewExtractor(r io.Reader) (VMDK, error) {
	var header Header
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, xerrors.Errorf("failed to read binary error: %w", err)
	}

	// TODO: RAW extent data file is unsupported
	if header.DescriptorOffset == 0 {
		return nil, xerrors.New("Unsupported vmdk format")
	}

	sector := make([]byte, Sector)
	var embDescriptor string
	for i := uint64(0); i < header.DescriptorSize; i++ {
		if _, err := r.Read(sector); err != nil {
			log.Fatal(err)
		}
		embDescriptor = embDescriptor + string(sector)
	}
	fmt.Println(embDescriptor)

	extent, err := newExtent(embDescriptor, header)
	if err != nil {
		return nil, xerrors.Errorf("failed to new vmdk reader: %w", err)
	}

	return extent, nil
}

// TODO: Parse EmbededDescriptor
// https://www.vmware.com/support/developer/vddk/vmdk_50_technote.pdf
func newExtent(s string, header Header) (VMDK, error) {
	if strings.Contains(s, `createType="streamOptimized"`) {
		return &StreamOptimizedExtent{
			header: header,
		}, nil
	} else {
		return nil, xerrors.New("Unsupported createType")
	}
}

func newReaderFunc(s string, header Header) (func(r io.Reader, dict []byte, header Header) (Reader, error), error) {
	if strings.Contains(s, `createType="streamOptimized"`) {
		return NewStreamOptimizedReader, nil
	} else {
		return nil, xerrors.New("Unsupported createType")
	}
}
