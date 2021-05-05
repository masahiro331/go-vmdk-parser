package disk

import (
	"io"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk/gpt"
	"github.com/masahiro331/go-vmdk-parser/pkg/disk/mbr"
	"github.com/masahiro331/go-vmdk-parser/pkg/disk/types"
	"golang.org/x/xerrors"
)

type Driver interface {
	GetPartitions() []types.Partition
}

func NewDriver(reader io.Reader) (Driver, error) {
	m, err := mbr.NewMasterBootRecord(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to new MBR: %w", err)
	}

	g, err := gpt.NewGUIDPartitionTable(reader)
	if err != nil {
		if m.UniqueMBRDiskSignature != [4]byte{0x00, 0x00, 0x00, 0x00} {
			return m, nil
		}

		return nil, xerrors.Errorf("failed to parse GUID partition table: %w", err)
	}

	return g, nil
}
