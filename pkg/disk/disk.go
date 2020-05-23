package disk

import (
	"encoding/binary"
	"errors"
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
type Partition []byte

func (p Partition) GetStartSector() uint32 {
	return binary.LittleEndian.Uint32(p[8:12])
}

func (p Partition) GetSize() uint32 {
	return binary.LittleEndian.Uint32(p[12:])
}

func (p Partition) Exist() bool {
	if p[4] == 0x00 {
		return false
	}
	return true
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
		return nil, errors.New("Invalid master boot record signature")
	}

	return &MasterBootRecord{
		BootCodeArea: buf[:446],
		Partitions: []Partition{
			buf[446:462],
			buf[462:478],
			buf[478:494],
			buf[494:510],
		},
		Signature: signature,
	}, nil
}
