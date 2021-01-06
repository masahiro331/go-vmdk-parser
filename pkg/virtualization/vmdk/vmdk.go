package vmdk

import (
	"encoding/binary"
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

type Reader interface {
	io.ReadCloser
	Next() (*disk.Partition, error)
}

func NewReader(r io.Reader) (Reader, error) {
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

	readerFunc, err := newReaderFunc(embDescriptor, header)
	if err != nil {
		return nil, xerrors.Errorf("failed to new vmdk reader: %w", err)
	}

	return readerFunc(r, header)
}

func newReaderFunc(s string, header Header) (func(r io.Reader, header Header) (Reader, error), error) {
	if strings.Contains(s, `createType="streamOptimized"`) {
		return NewStreamOptimizedReader, nil
	} else {
		return nil, xerrors.New("Unsupported createType")
	}
}
