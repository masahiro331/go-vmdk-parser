package gpt

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk/types"
	"golang.org/x/xerrors"
)

const (
	Sector     = 0x200
	Signature  = "EFI PART"
	BootTypeID = "Hah!IdontNeedEFI"
)

type GUIDPartitionTable struct {
	Header  Header
	Entries []PartitionEntry
}

func (gpt *GUIDPartitionTable) GetPartitions() []types.Partition {
	var ps []types.Partition
	for _, p := range gpt.Entries {
		var i types.Partition
		i = p
		ps = append(ps, i)
	}

	sort.Slice(ps, func(i, j int) bool {
		return ps[i].GetStartSector() < ps[j].GetStartSector()
	})
	return ps
}

func (pe PartitionEntry) Name() string {
	return trimNullByte(pe.PartitionName[:])
}

func trimNullByte(bytes []byte) string {
	var bs []byte
	for _, b := range bytes {
		if b == 0x00 {
			continue
		}
		bs = append(bs, b)
	}

	return string(bs)
}

func (pe PartitionEntry) GetStartSector() uint64 {
	return pe.StartingLBA
}

func (pe PartitionEntry) GetSize() uint64 {
	return pe.EndingLBA - pe.StartingLBA + 1
}

func (pe PartitionEntry) Bootable() bool {
	if string(pe.PartitionTypeGUID[:]) == BootTypeID {
		return true
	}
	return false
}

type Header struct {
	Signature                [8]byte
	Revision                 [4]byte
	HeaderSize               uint32
	HeaderCRC                [4]byte
	Reserved                 [4]byte
	MyLBA                    uint64
	AlternateLBA             uint64
	FirstUsableLBA           uint64
	LastUsableLBA            uint64
	DiskGUID                 [16]byte
	PartitionEntryLBA        uint64
	NumberOfPartitionEntries uint32
	SizeOfPartitionEntry     uint32
	PartitionEntryArrayCRC32 [4]byte
	ReservedPadding          [420]byte
}

type PartitionEntry struct {
	PartitionTypeGUID   [16]byte
	UniquePartitionGUID [16]byte
	StartingLBA         uint64
	EndingLBA           uint64
	Attributes          uint64
	PartitionName       [72]byte
}

func (g *PartitionEntry) isUsed() bool {
	for _, b := range g.PartitionTypeGUID {
		if b == 0x00 {
			continue
		}
		return true
	}
	return false
}

func NewGUIDPartitionTable(r io.Reader) (*GUIDPartitionTable, error) {
	var gpt GUIDPartitionTable
	if err := binary.Read(r, binary.LittleEndian, &gpt.Header); err != nil {
		return nil, xerrors.Errorf("failed to binary read: %w", err)
	}

	if gpt.Header.SizeOfPartitionEntry != 128 {
		return nil, xerrors.New("not support GPT format error, must be 128 byte")
	}

	if Sector-len(gpt.Header.ReservedPadding) != int(gpt.Header.HeaderSize) {
		return nil, xerrors.New("invalid header size error")
	}

	if string(gpt.Header.Signature[:]) != Signature {
		return nil, xerrors.Errorf("invalid GPT signature: %s", gpt.Header.Signature)
	}

	for i := 0; i < int(gpt.Header.NumberOfPartitionEntries); i++ {
		var partitionEntry PartitionEntry
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d]: %w", i, err)
		}
		if partitionEntry.isUsed() {
			gpt.Entries = append(gpt.Entries, partitionEntry)
		}
	}

	return &gpt, nil
}
