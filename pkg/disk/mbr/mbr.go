package mbr

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk/types"
	"golang.org/x/xerrors"
)

const (
	SIGNATURE = 0xAA55
	Sector    = 512
)

/*
# Master Boot Record Spec
https://uefi.org/sites/default/files/resources/UEFI%20Spec%202.8B%20May%202020.pdf
p. 112
Master Boot Record always 512 bytes.
+-------------------------------+
|         Name           | Byte |
+------------------------+------+
| Bootstrap Code Area    | 440  |
| UniqueMBRDiskSignature | 4    |
| Unknown                | 2    |
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
var InvalidSignature = xerrors.New("Invalid master boot record signature")

type MasterBootRecord struct {
	BootCodeArea           [440]byte
	UniqueMBRDiskSignature [4]byte
	Unknown                [2]byte
	Partitions             [4]Partition
	Signature              uint16
	count                  int
}

type Partition struct {
	Boot     bool
	StartCHS [3]byte
	Type     byte
	EndCHS   [3]byte

	StartSector uint32
	Size        uint32
}

func (m *MasterBootRecord) GetPartitions() []types.Partition {
	var ps []types.Partition
	for _, p := range m.Partitions {
		var i types.Partition
		i = p
		ps = append(ps, i)
	}
	return ps
}

func (p Partition) Name() string {
	// TODO: return number of partition index
	return strconv.Itoa(int(p.StartSector))
}

func (p Partition) GetStartSector() uint64 {
	return uint64(p.StartSector)
}

func (p Partition) Bootable() bool {
	return p.Boot
}

func (p Partition) GetSize() uint64 {
	return uint64(p.Size)
}

func NewMasterBootRecord(reader io.Reader) (*MasterBootRecord, error) {
	buf := make([]byte, Sector)
	size, err := reader.Read(buf)
	if err != nil {
		return nil, xerrors.Errorf("failed to read mbr error: %w")
	}
	if size != Sector {
		return nil, xerrors.New("binary size error")
	}

	r := bytes.NewReader(buf)
	var mbr MasterBootRecord

	if err := binary.Read(r, binary.LittleEndian, &mbr.UniqueMBRDiskSignature); err != nil {
		return nil, xerrors.Errorf("failed to parse unique MBR disk signature: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &mbr.Unknown); err != nil {
		return nil, xerrors.Errorf("failed to parse unknown: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &mbr.BootCodeArea); err != nil {
		return nil, xerrors.Errorf("failed to parse boot code: %w", err)
	}

	for i := 0; i < len(mbr.Partitions); i++ {
		if err := binary.Read(r, binary.LittleEndian, &mbr.Partitions[i]); err != nil {
			return nil, xerrors.Errorf("failed to parse partition[%d]: %w", i, err)
		}
	}

	if err := binary.Read(r, binary.LittleEndian, &mbr.Signature); err != nil {
		return nil, xerrors.Errorf("failed to parse signature: %w", err)
	}
	if mbr.Signature != SIGNATURE {
		return nil, InvalidSignature
	}

	return &mbr, nil
}
