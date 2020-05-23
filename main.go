package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
)

const (
	GD_AT_END   = 0xffffffffffffffff
	SECTOR_SYZE = uint32(0x200)

	MARKER_EOS    = uint32(0x00000000)
	MARKER_GT     = uint32(0x00000001)
	MARKER_GD     = uint32(0x00000002)
	MARKER_FOOTER = uint32(0x00000003)
	MARKER_GRAIN  = uint32(0xffffffff)

	EMBEDDED_DESCRIPTOR_SIZE = 0xfe00 // 0x10000 - 0x200(VMDK HEADER)
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
| Offset | Size | Description |
| 0      | 8    | Value       |
| 8      | 4    | Data Size   |

if marker DataSize == 0
| 12     | 4    | Marker Type |
| 16     | 496  | Padding     |

if marker size > 0
| 12     | ...  | GrainData   |
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

var count int

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
	var header VMDKHeader
	if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", header)

	sector := NewSector()

	// Get VMDK embedded descriptor
	var embDescriptor string
	for i := 0; i < 2; i++ {
		if _, err := reader.Read(sector); err != nil {
			log.Fatal(err)
		}
		embDescriptor = embDescriptor + string(sector)
	}
	fmt.Println(embDescriptor)

	var fileBuffer []byte
	for {
		_, err := reader.Read(sector)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		m := sector.GetMarker()

		switch m.Type {
		case MARKER_GRAIN:
			// fmt.Println("=== GRAIN ===")
			var gd []byte

			if m.Size < 500 {
				fileBuffer = append(fileBuffer, m.Data[:m.Size]...)
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

		case MARKER_GT:
			// fmt.Println("====== GRAIN TABLE ======")
			// GRAIN TABLE always 512 entries
			// GRAIN TABLE ENTRY is 32bit
			// GRAIN TABLE is 2KB

			// ** test code ** //
			file, err := os.Create(fmt.Sprintf("data/file%02d.zlib", count))
			if err != nil {
				log.Fatal(err)
			}
			file.Write(fileBuffer)
			fileBuffer = []byte{}

			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
			count = count + 1
		case MARKER_GD:
			// fmt.Println("====== GRAIN DIRECTORY ======")
			// fmt.Printf("%+v\n", m)
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		case MARKER_FOOTER:
			// fmt.Println("====== GRAIN FOOTER ======")
			// fmt.Printf("%+v\n", m)
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		case MARKER_EOS:
		default:
			log.Fatal("unexpected error")
		}
	}

}

func NewSector() Sector {
	return make(Sector, SECTOR_SYZE)
}

type SectorType uint64

type VMDKHeader struct {
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

func (h *VMDKHeader) CheckSignature() bool {
	return h.Signature == 0x564d444b
}
