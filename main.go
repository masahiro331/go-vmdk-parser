package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
)

type Sector []byte
type CompressedGrainData []byte

type Marker struct {
	Value uint64
	Size  uint32
	Type  uint32
	Data  []byte
}

/*
### Marker Specs ( 512 bytes )
+--------+------+-------------+
| Offset | Size | Description |
+--------+------+-------------+
| 0      | 8    | Value       |
| 8      | 4    | Data Size   |
| 12     | 4    | Marker Type |
| 16     | 496  | Padding     |
+--------+------+-------------+
| if marker size > 0          |
| 12     | ...  | GrainData   |
+--------+------+-------------+
*/
func (sector Sector) GetMarker() *Marker {
	size := binary.LittleEndian.Uint32(sector[8:12])
	if size == 0 {
		return &Marker{
			Value: binary.LittleEndian.Uint64(sector[:8]),
			Size:  size,
			Type:  binary.LittleEndian.Uint32(sector[12:16]),
		}
	} else {
		return &Marker{
			Value: binary.LittleEndian.Uint64(sector[:8]),
			Size:  size,
			Type:  MARKER_GRAIN,
			Data:  sector[12:],
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("required [vmdk] arguments")
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	reader := bufio.NewReader(f)

	// Get VMDK Header
	var header Header
	if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", header)

	sector := NewSector()
	// Get VMDK embedded descriptor
	var embDescriptor string
	for i := SectorType(0); i < header.DescriptorSize; i++ {
		if _, err := reader.Read(sector); err != nil {
			log.Fatal(err)
		}
		embDescriptor = embDescriptor + string(sector)
	}
	// TODO: Parse Descriptor
	fmt.Println(strings.TrimSpace(embDescriptor))

	// Trim vmdk head Metadata
	for i := SectorType(0); i < (header.OverHead - header.DescriptorOffset - header.DescriptorSize); i++ {
		if _, err := reader.Read(sector); err != nil {
			log.Fatal(err)
		}
	}

	// Read Master Boot Record
	var mbr MasterBootRecord
	var fileBuffer []byte
	for {
		_, err = reader.Read(sector)
		if err != nil {
			log.Fatal(err)
		}
		m := sector.GetMarker()
		if m.Type != MARKER_GRAIN {
			log.Fatal("Unsupported vmdk create file type")
		}

		// file, err := os.Create(fmt.Sprintf("data/file%04d.zlib", count))
		if err != nil {
			log.Fatal(err)
		}

		if m.Size < 500 {
			fileBuffer = append(fileBuffer, m.Data[:m.Size]...)
			break
		}

		var gd []byte
		gd = append(gd, m.Data...)
		limit := uint64(math.Ceil(float64(m.Size-500) / float64(SECTOR_SYZE)))
		for i := uint64(0); i < limit; i++ {
			_, err := reader.Read(sector)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			gd = append(gd, sector...)
		}
		fileBuffer = append(fileBuffer, gd[:m.Size]...)

		r, err := zlib.NewReader(bytes.NewReader(fileBuffer))
		if err != nil {
			log.Fatal(err)
		}

		if err := binary.Read(r, binary.BigEndian, &mbr); err != nil {
			log.Fatal(err)
		}
		if mbr.Partition1.Exist() {
			fmt.Printf("start sector: %d\n", mbr.Partition1.GetStartSector())
			fmt.Printf("sector size: %d\n", mbr.Partition1.GetSize())
		}
		if mbr.Partition2.Exist() {
			fmt.Printf("start sector: %d\n", mbr.Partition2.GetStartSector())
			fmt.Printf("sector size: %d\n", mbr.Partition2.GetSize())
		}
		break
	}

	img0, _ := os.Create("extfile/0.img")
	img1, _ := os.Create("extfile/1.img")
	defer img0.Close()
	defer img1.Close()
	ext4s := []*os.File{img0, img1}
	var count int

	for {

		_, err := reader.Read(sector)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		m := sector.GetMarker()
		if m.Value == uint64(mbr.Partition2.GetStartSector()) {
			fmt.Println(m.Value)
			count = count + 1
			fmt.Println(count)
		}

		switch m.Type {
		case MARKER_GRAIN:
			if count == 0 {
				m.Value = m.Value - uint64(mbr.Partition1.GetStartSector())
			} else if count == 1 {
				m.Value = m.Value - uint64(mbr.Partition2.GetStartSector())
			}

			ext4s[count].Seek(int64(m.Value*512), 0)

			var gd []byte
			var fileBuffer []byte
			// file, err := os.Create(fmt.Sprintf("data/file%04d.zlib", count))
			if err != nil {
				log.Fatal(err)
			}

			if m.Size < 500 {
				fileBuffer = append(fileBuffer, m.Data[:m.Size]...)
				bReader := bytes.NewReader(fileBuffer)
				zReader, err := zlib.NewReader(bReader)
				if err != nil {
					log.Fatal(err)
				}
				io.Copy(ext4s[count], zReader)
				break
			}

			gd = append(gd, m.Data...)
			limit := uint64(math.Ceil(float64(m.Size-500) / float64(SECTOR_SYZE)))
			for i := uint64(0); i < limit; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
				gd = append(gd, sector...)
			}
			fileBuffer = append(fileBuffer, gd[:m.Size]...)
			bReader := bytes.NewReader(fileBuffer)
			zReader, err := zlib.NewReader(bReader)
			if err != nil {
				log.Fatal(err)
			}
			io.Copy(ext4s[count], zReader)

		case MARKER_EOS:
		case MARKER_GT:
			// GRAIN TABLE always 512 entries
			// GRAIN TABLE ENTRY is 32bit
			// GRAIN TABLE is 2KB
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		case MARKER_GD:
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		case MARKER_FOOTER:
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		default:
			log.Fatal("unexpected error")
		}

	}
}

type Extent struct {
	File       *os.File
	pos        int64
	compressed bool
	isClose    bool
}

func NewExtent(filename string, pos int64, comporessed bool) (*Extent, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &Extent{
		File:       file,
		pos:        pos,
		compressed: comporessed,
	}, nil
}

func (e *Extent) Write(p []byte) (int, error) {
	if e.compressed {
		r, err := zlib.NewReader(bytes.NewReader(p))
		if err != nil {
			log.Fatal(err)
		}
		n, err := io.Copy(e.File, r)
		if err != nil {
			return 0, err
		}
		return int(n), nil
	} else {
		n, err := e.File.Write(p)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
}

// TODO: Not Support Read
func (e *Extent) Read(p []byte) (int, error) {
	return 0, errors.New("Unsupported Read")
}

// Not Support whenece 2
func (e *Extent) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case 0:
		e.pos = offset
	case 1:
		e.pos += offset
		// f.pos = f.GetSize() - offset
	default:
		return 0, errors.New("Unsupported whence")
	}

	return e.pos, nil
}

func (e *Extent) Close() error {
	if err := e.File.Close(); err != nil {
		return err
	}
	e.isClose = true
	return nil
}

func (e *Extent) IsClose() bool {
	return e.isClose
}

func NewSector() Sector {
	return make(Sector, SECTOR_SYZE)
}

type SectorType uint64

func NewVMDK(reader *io.Reader) (*VMDK, error) {
	return nil, nil
}

type VMDK struct {
	header             Header
	embededDescription EmbededDescription
	reader             *io.Reader
}

type EmbededDescription struct {
	Version    int
	CID        string
	ParentCID  string
	CreateType CreateType
	ExtentType ExtentType
	Capacity   uint64
	FileName   string
}

type Header struct {
	Signature          uint32
	Version            uint32
	Flag               uint32
	Capacity           SectorType
	GrainSize          SectorType
	DescriptorOffset   SectorType // Descriptor sector numberh
	DescriptorSize     SectorType
	NumGTEsPerGT       uint32
	RgdOffset          SectorType
	GdOffset           SectorType
	OverHead           SectorType
	UncleanShutdown    byte
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
	Padding            [433]byte
}

func (h *Header) CheckSignature() bool {
	return h.Signature == 0x564d444b
}

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
	BootCodeArea [446]byte
	Partition1   Partision
	Partition2   Partision
	Partition3   Partision
	Partition4   Partision
	Signature    uint16
}
type Partision [16]byte

func (p *Partision) GetStartSector() uint32 {
	return binary.LittleEndian.Uint32(p[8:12])
}

func (p *Partision) GetSize() uint32 {
	return binary.LittleEndian.Uint32(p[12:])
}

func (p *Partision) Exist() bool {
	if p[4] == 0x00 {
		return false
	}
	return true
}
