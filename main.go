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

const (
	GD_AT_END   = 0xffffffffffffffff
	SECTOR_SYZE = uint32(0x200)

	MARKER_EOS    = uint32(0x00000000)
	MARKER_GT     = uint32(0x00000001)
	MARKER_GD     = uint32(0x00000002)
	MARKER_FOOTER = uint32(0x00000003)
	MARKER_GRAIN  = uint32(0xffffffff)

	COWD = uint32(0x434f5744)

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

	// ext4, err := os.Create("extfile/0.img")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer ext4.Close()

	// var extents []*Extent

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
			// if len(extents) == 0 {
			// 	ext, err := NewExtent(fmt.Sprintf("%d.img", len(extents)), int64(m.Value), true)
			// 	if err != nil {
			// 		log.Fatal(err)
			// 	}
			// 	extents[0] = ext
			// }

			// if m.Value > 48331 {
			// 	break L
			// }
			// if m.Value < 2048 {
			// 	continue
			// }
			// m.Value = m.Value - 2048

			// ext4.Seek(int64(m.Value*512), 0)

			fmt.Println("=== GRAIN ===")
			var gd []byte
			var fileBuffer []byte
			count = count + 1
			// file, err := os.Create(fmt.Sprintf("data/file%04d.zlib", count))
			if err != nil {
				log.Fatal(err)
			}

			if m.Size < 500 {
				fileBuffer = append(fileBuffer, m.Data[:m.Size]...)
				// bReader := bytes.NewReader(fileBuffer)
				// zReader, err := zlib.NewReader(bReader)
				// if err != nil {
				// 	log.Fatal(err)
				// }
				// io.Copy(ext4, zReader)

				// file.Write(fileBuffer)
				// file.Close()
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
			// bReader := bytes.NewReader(fileBuffer)
			// zReader, err := zlib.NewReader(bReader)
			// if err != nil {
			// 	log.Fatal(err)
			// }
			// io.Copy(ext4, zReader)

			// file.Write(fileBuffer)
			// file.Close()
		case MARKER_EOS:
			fmt.Println("=== EOS ===")
			// if len(extents) != 0 {
			// 	err := extents[len(extents)-1].Close()
			// 	if err != nil {
			// 		log.Fatal(err)
			// 	}
			// }
		case MARKER_GT:
			fmt.Println("=== TABLE ===")
			// GRAIN TABLE always 512 entries
			// GRAIN TABLE ENTRY is 32bit
			// GRAIN TABLE is 2KB
			// ** test code ** //
			for i := uint64(0); i < m.Value; i++ {
				_, err := reader.Read(sector)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}
		case MARKER_GD:
			fmt.Println("=== DIREC ===")
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
			fmt.Println("=== FOOTER ===")
			// fmt.Printf("%+v\n", m)
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
