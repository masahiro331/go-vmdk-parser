package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk"
	"golang.org/x/xerrors"
)

type StreamOptimizedExtent struct {
	header Header
}

func (s *StreamOptimizedExtent) ExtractFromFile(r io.Reader, filenames []string) (FileMap, error) {
	sector := make([]byte, Sector)

	// Trim vmdk head Metadata
	overHeadOffset := s.header.OverHead - s.header.DescriptorOffset - s.header.DescriptorSize
	for i := uint64(0); i < (overHeadOffset); i++ {
		if _, err := r.Read(sector); err != nil {
			return nil, xerrors.Errorf("failed to read overhead error: %w", err)
		}
	}

	filemap := make(map[string][][]byte)

	var partitionOffset = -1
	var mbr *disk.MasterBootRecord
	for {
		//

		if _, err := r.Read(sector); err != nil {
			return nil, xerrors.Errorf("failed to read marker error: %w", err)
		}

		m := parseMarker(sector)
		// Skip Disk Metadata
		// if uint64(mbr.Partitions[partitionOffset].GetStartSector()) < m.Value {
		// 	continue
		// }

		// Check Partision
		// if partitionOffset != len(mbr.Partitions)-1 && m.Type == MARKER_GRAIN &&
		// 	m.Value == uint64(mbr.Partitions[partitionOffset+1].GetStartSector()) {
		// }

		fn := fmt.Sprintf("%d.img", partitionOffset)

		switch m.Type {
		case MARKER_GRAIN:

			buf := new(bytes.Buffer)
			if m.Size < 500 {
				// buf = append(buf, m.Data[:m.Size]...)
				buf.Write(m.Data[:m.Size])
			} else {
				// buf = append(buf, m.Data...)
				buf.Write(m.Data)
				limit := uint64(math.Ceil(float64(m.Size-500) / float64(Sector)))
				for i := uint64(0); i < limit; i++ {
					if _, err := r.Read(sector); err != nil {
						return nil, xerrors.Errorf("failed to read Grain Data error: %w", err)
					}
					// あまりでやらないとミスってる気がする...?
					// buf = append(buf, sector...)
					buf.Write(sector)
				}
			}

			if partitionOffset < 0 {
				zr, err := zlib.NewReader(buf)
				if err != nil {
					return nil, xerrors.Errorf("failed to read zlib error: %w", err)
				}

				// Read Master Boot Record
				// TODO: Support GPT disk type
				mbr, err = disk.NewMasterBootRecord(zr)
				// mbr, err = disk.NewMasterBootRecord(buf2)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse disk error: %w", err)
				}
				for i, p := range mbr.Partitions {
					fmt.Printf("%d: ss(%d), size(%d)\n", i, p.GetStartSector(), p.GetSize())
				}

				partitionOffset = partitionOffset + 1

			} else {
				// Check Partision
				if m.Value == uint64((mbr.Partitions[partitionOffset].GetSize() + mbr.Partitions[partitionOffset].GetStartSector())) {
					partitionOffset = partitionOffset + 1
				}

				filemap[fn] = append(filemap[fn], buf.Bytes())
			}

		case MARKER_EOS:
		case MARKER_GT:
			// GRAIN TABLE always 512 entries
			// GRAIN TABLE ENTRY is 32bit
			// GRAIN TABLE is 2KB
			for i := uint64(0); i < m.Value; i++ {
				if _, err := r.Read(sector); err != nil {
					return nil, xerrors.Errorf("failed to read Grain Table error: %w", err)
				}
			}
		case MARKER_GD:
			for i := uint64(0); i < m.Value; i++ {
				if _, err := r.Read(sector); err != nil {
					return nil, xerrors.Errorf("failed to read Grain Directory error: %w", err)
				}
			}
		case MARKER_FOOTER:
			return filemap, nil
		default:
			return nil, xerrors.New("Invalid Marker Type")
		}
	}
}

func parseMarker(sector []byte) *Marker {
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
type Marker struct {
	Value uint64
	Size  uint32
	Type  uint32
	Data  []byte
}
