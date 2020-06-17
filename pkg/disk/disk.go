package disk

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/xerrors"
)

const (
	SIGNATURE = 0xAA55
)

/*
# Master Boot Record Spec
Master Boot Record always 512 bytes.
+-------------------------------+
|         Name           | Byte |
+------------------------+------+
| Bootstrap Code Area    | 446  |
| Partion 1              | 16   |
| Partion 2              | 16   |
| Partion 3              | 16   |
| Partion 4              | 16   |
| Boot Recore Sigunature | 2    |
+-------------------------------+

# Partion Spec
+-------------------+------+----------------------------------------------------------+
|        Name       | Byte |                        Description                       |
+-------------------+------+----------------------------------------------------------+
| Boot Indicator    | 1    | Boot Partion                                             |
| Staring CHS value | 3    | Starting sector of the partition in Cylinder Head Sector |
| Partition type    | 1    | FileSystem used by the partition	                      |
| Ending CHS values | 3    | Ending sector of the partition in Cylinder Head Sector   |
| Starting Sector   | 4    | Starting sector of the active partition                  |
| Partition Size    | 4    | Represents partition size in sectors                     |
+-------------------+------+----------------------------------------------------------+


ref: https://www.ijais.org/research/volume10/number8/sadi-2016-ijais-451541.pdf
*/
type MasterBootRecord struct {
	BootCodeArea []byte
	Partitions   []Partition
	Signature    uint16
}

// type Partition []byte

type Partition struct {
	Boot        bool
	StartCHS    [3]byte
	Type        byte
	EndCHS      [3]byte
	StartSector uint32
	Size        uint32
}

func (p Partition) GetStartSector() uint32 {
	return p.StartSector
}

func (p Partition) GetSize() uint32 {
	return p.Size
}

func NewMasterBootRecord(reader io.Reader) (*MasterBootRecord, error) {
	buf := make([]byte, 512)
	size, err := reader.Read(buf)
	if err != nil {
		return nil, xerrors.Errorf("failed to read mbr error: %w")
	}
	if size != 512 {
		return nil, xerrors.New("Invalid binary")
	}
	signature := binary.LittleEndian.Uint16(buf[510:])
	if signature != SIGNATURE {
		return nil, xerrors.New("Invalid master boot record signature")
	}

	// TODO: refactoring
	var p1 Partition
	if err := binary.Read(bytes.NewReader(buf[446:462]), binary.LittleEndian, &p1); err != nil {
		return nil, xerrors.Errorf("Invalid partition1 format: %w", err)
	}
	var p2 Partition
	if err := binary.Read(bytes.NewReader(buf[462:478]), binary.LittleEndian, &p2); err != nil {
		return nil, xerrors.Errorf("Invalid partition2 format: %w", err)
	}
	var p3 Partition
	if err := binary.Read(bytes.NewReader(buf[478:494]), binary.LittleEndian, &p3); err != nil {
		return nil, xerrors.Errorf("Invalid partition3 format: %w", err)
	}
	var p4 Partition
	if err := binary.Read(bytes.NewReader(buf[494:510]), binary.LittleEndian, &p4); err != nil {
		return nil, xerrors.Errorf("Invalid partition4 format: %w", err)
	}

	return &MasterBootRecord{
		BootCodeArea: buf[:446],
		Partitions:   []Partition{p1, p2, p3, p4},
		Signature:    signature,
	}, nil
}
