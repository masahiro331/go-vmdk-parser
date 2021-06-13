package gpt

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk/types"
	"golang.org/x/xerrors"
)

const (
	Sector     = 0x200
	Signature  = "EFI PART"
	BootTypeID = "Hah!IdontNeedEFI"
)

const (
	// Common
	UnUsed       = "00000000-0000-0000-0000-000000000000"
	MBR          = "024DEE41-33E7-11D3-9D69-0008C781F39F"
	EFI          = "C12A7328-F81F-11D2-BA4B-00A0C93EC93B"
	GrubBIOSBoot = "21686148-6449-6E6F-744E-656564454649"

	// Linux
	Data               = "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
	RAID               = "A19D880F-05FC-4D3B-A006-743F0F84911E"
	Swap               = "0657FD6D-A4AB-43C4-84E5-0933C84B4F4F"
	LVM                = "E6D6D379-F507-44C2-A23C-238F2A3DF928"
	Home               = "933AC7E1-2EB4-4F13-B844-0E14E2AEF915"
	Srv                = "3B8F8425-20E0-4F3B-907F-1A25A76F98E8"
	Var                = "4D21B016-B534-45C2-A9FB-5C16E091FD2D"
	Tmp                = "7EC6F557-3BC5-4ACA-B293-16EF5DF639D1"
	Boot               = "BC13C2FF-59E6-4262-A352-B275FD6F7172"
	Reserved           = "8DA63339-0007-60C0-C436-083AC8230908"
	RootIntelx86       = "44479540-F297-41B2-9AF7-D131D5F0458A"
	RootIntelx64       = "4F68BCE3-E8CD-4DB1-96E7-FBCAF984B709"
	RootARMx86         = "69DAD710-2CE4-4E3C-B16C-21A1D49ABED3"
	RootARMx64         = "B921B045-1DF0-41C3-AF44-4C6F280D3FAE"
	RootIAx64          = "993D8D3D-F80E-4225-855A-9DAF8ED7EA97"
	RootRISCVx86       = "60D5A7FE-8E7D-435C-B714-3DD8162144E1"
	RootRISCVx64       = "72EC70A6-CF74-40E6-BD49-4BDA08E8F224"
	RootVerifyIntelx86 = "D13C5D3B-B5D1-422A-B29F-9454FDC89D76"
	RootVerifyIntelx64 = "2C7357ED-EBD2-46D9-AEC1-23D437EC2BF5"
	RootVerifyARMx86   = "7386CDF2-203C-47A9-A498-F2ECCE45A2D6"
	RootVerifyARMx64   = "DF3300CE-D69F-4C92-978C-9BFB0F38D820"
	RootVerifyIAx64    = "86ED10D5-B607-45BB-8957-D350F23D0571"
	RootVerifyRISCVx86 = "AE0253BE-1167-4007-AC68-43926C14C5DE"
	RootVerifyRISCVx64 = "B6ED5582-440B-4209-B8DA-5FF7C419EA3D"
	UsrIntelx86        = "75250D76-8CC6-458E-BD66-BD47CC81A812"
	UsrIntelx64        = "8484680C-9521-48C6-9C11-B0720656F69E"
	UsrARMx86          = "7D0359A3-02B3-4F0A-865C-654403E70625"
	UsrARMx64          = "B0E01050-EE5F-4390-949A-9101B17104E9"
	UsrIAx64           = "4301D2A6-4E3B-4B2A-BB94-9E0B2C4225EA"
	UsrRISCVx86        = "B933FB22-5C3F-4F91-AF90-E2BB0FA50702"
	UsrRISCVx64        = "BEAEC34B-8442-439B-A40B-984381ED097D"
	UsrVerifyIntelx86  = "8F461B0D-14EE-4E81-9AA9-049B6FB97ABD"
	UsrVerifyIntelx64  = "77FF5F63-E7B6-4633-ACF4-1565B864C0E6"
	UsrVerifyARMx86    = "C215D751-7BCD-4649-BE90-6627490A4C05"
	UsrVerifyARMx64    = "6E11A4E7-FBCA-4DED-B9E9-E1A512BB664E"
	UsrVerifyIAx64     = "6A491E03-3BE7-4545-8E38-83320E0EA880"
	UsrVerifyRISCVx86  = "CB1EE4E3-8CD0-4136-A0A4-AA61A32E8730"
	UsrVerifyRISCVx64  = "8F1056BE-9B05-47C4-81D6-BE53128E5B54"
)

type GUIDPartitionTable struct {
	Header  Header
	Entries []PartitionEntry
}

func (gpt *GUIDPartitionTable) GetPartitions() []types.Partition {
	var ps []types.Partition
	for _, p := range gpt.Entries {
		var i types.Partition = p
		ps = append(ps, i)
	}

	sort.Slice(ps, func(i, j int) bool {
		return ps[i].GetStartSector() < ps[j].GetStartSector()
	})
	return ps
}

func (pe PartitionEntry) Name() string {
	name := trimNullByte(pe.PartitionName[:])

	switch name {
	case "/":
		return "ROOT"
	case "":
		return strconv.Itoa(pe.index)
	}

	return name
}

func (pe PartitionEntry) Index() int {
	return pe.index
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

func (pe PartitionEntry) GetType() []byte {
	return pe.PartitionTypeGUID[:]
}

func (pe PartitionEntry) Bootable() bool {
	guid := pe.PartitionTypeGUID.String()
	return guid == MBR ||
		guid == EFI ||
		guid == GrubBIOSBoot
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
	PartitionTypeGUID   GUID
	UniquePartitionGUID GUID
	StartingLBA         uint64
	EndingLBA           uint64
	Attributes          uint64
	PartitionName       [72]byte

	index int
}

type GUID [16]byte

func (guid GUID) String() string {
	return strings.ToUpper(fmt.Sprintf("%x-%x-%x-%x-%x", reverse(guid[0:4]), reverse(guid[4:6]), reverse(guid[6:8]), guid[8:10], guid[10:]))
}

func reverse(bs []byte) []byte {
	var ret []byte
	for i := len(bs) - 1; i > -1; i-- {
		ret = append(ret, bs[i])
	}
	return ret
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
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.PartitionTypeGUID); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] PartitionTypeGUID: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.UniquePartitionGUID); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] UniquePartitionGUID: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.StartingLBA); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] StartingLBA: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.EndingLBA); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] EndingLBA: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.Attributes); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] Attributes: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &partitionEntry.PartitionName); err != nil {
			return nil, xerrors.Errorf("failed to parse GPT partition entry[%d] PartitionName: %w", i, err)
		}
		partitionEntry.index = i

		if partitionEntry.isUsed() {
			gpt.Entries = append(gpt.Entries, partitionEntry)
		}
	}

	return &gpt, nil
}

func (pe PartitionEntry) IsSupported() bool {
	return true
}
