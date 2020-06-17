package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"log"
	"math"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk"
	"golang.org/x/xerrors"
)

const (
	CLUSTER_SIZE = 128
	SECOTR_SIZE  = 512
)

type StreamOptimizedExtent struct {
	header Header
}

type streamOptimizedExtentReader struct {
	r io.Reader

	err           error
	header        Header
	buffer        *bytes.Buffer
	secondbuffer  *bytes.Buffer
	sectorPos     uint64
	fileSectorPos uint64
	mbr           *disk.MasterBootRecord
	partition     *disk.Partition

	// sectorPos uint64
}

// Read '0x100000' bytes in NewReader for get master record
func NewStreamOptimizedReader(r io.Reader, dict []byte, header Header) (Reader, error) {
	// Trim vmdk head Metadata
	sector := make([]byte, Sector)

	overHeadOffset := header.OverHead - header.DescriptorOffset - header.DescriptorSize
	for i := uint64(0); i < (overHeadOffset); i++ {
		if _, err := r.Read(sector); err != nil {
			return nil, xerrors.Errorf("failed to read overhead error: %w", err)
		}
	}

	// TODO: Read Master record
	reader := streamOptimizedExtentReader{
		buffer:       bytes.NewBuffer([]byte{}),
		secondbuffer: bytes.NewBuffer([]byte{}),
		header:       header,
		r:            r,
	}

	_, err := reader.readGrainData()
	reader.mbr, err = disk.NewMasterBootRecord(reader.buffer)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse disk error: %w", err)
	}
	reader.buffer.Reset()

	return &reader, nil
}

func (s *streamOptimizedExtentReader) Read(p []byte) (int, error) {
	// if s.partition != nil {
	// 	return 0, io.EOF
	// }

	if len(p) == 0 {
		// TODO
	}

	for {
		if s.buffer.Len() == 0 {
			s.fileSectorPos = s.sectorPos + CLUSTER_SIZE
			_, err := s.readGrainData()
			if err != nil {
				if err != io.EOF {
					log.Fatalf("unknown err %s", err)
				}
				return 0, err
			}

			if s.partition != nil && s.sectorPos == uint64(s.partition.StartSector+s.partition.Size) {
				return 0, io.EOF
			}

			continue
		}
		if s.fileSectorPos == s.sectorPos {
			i, err := s.buffer.Read(p)
			if err != nil {
				if err != io.EOF {
					log.Fatalf("unknown err %s", err)
				}
				return i, err
			}
			return i, err
		} else {
			if s.secondbuffer.Len() == 0 {
				s.fileSectorPos = s.fileSectorPos + CLUSTER_SIZE
				s.secondbuffer.Write(make([]byte, SECOTR_SIZE*CLUSTER_SIZE))
			}

			i, err := s.secondbuffer.Read(p)
			if err != nil {
				if err != io.EOF {
					log.Fatalf("unknown err %s", err)
				}
				return i, err
			}
			return i, err
		}
	}

}

func (s *streamOptimizedExtentReader) Next() (*disk.Partition, error) {
	if s.partition == nil {
		s.partition = &s.mbr.Partitions[0]
	} else {
		for _, p := range s.mbr.Partitions {
			if p.StartSector > s.partition.StartSector {
				s.partition = &p
				break
			} else if p.StartSector == 0 {
				return nil, io.EOF
			}
		}
	}
	if uint64(s.partition.StartSector) == s.sectorPos {
		// s.fileSectorPos = 0
		s.fileSectorPos = s.sectorPos
		return s.partition, nil
	}

	var err error
	startSector := uint64(s.partition.StartSector)
	for {
		if startSector > s.sectorPos {
			s.sectorPos, err = s.readGrainData()
			if err != nil {
				return nil, xerrors.Errorf("failed to next error: %w", err)
			}
			if startSector == s.sectorPos {
				// s.fileSectorPos = 0
				s.fileSectorPos = s.sectorPos
				return s.partition, nil
			}
		}
		s.buffer.Reset()
	}
}

func (s *streamOptimizedExtentReader) readGrainData() (uint64, error) {
	sector := make([]byte, Sector)
	for {
		if _, err := s.r.Read(sector); err != nil {
			return 0, xerrors.Errorf("failed to read marker error: %w", err)
		}
		m := parseMarker(sector)
		switch m.Type {
		case MARKER_GRAIN:
			s.sectorPos = m.Value
			buf := new(bytes.Buffer)
			if m.Size < 500 {
				buf.Write(m.Data[:m.Size])
			} else {
				buf.Write(m.Data)
				limit := uint64(math.Ceil(float64(m.Size-500) / float64(Sector)))
				for i := uint64(0); i < limit; i++ {
					if _, err := s.r.Read(sector); err != nil {
						return 0, xerrors.Errorf("failed to read Grain Data error: %w", err)
					}
					buf.Write(sector)
				}
			}
			zr, err := zlib.NewReader(buf)
			if err != nil {
				return 0, xerrors.Errorf("failed to read zlib error: %w", err)
			}
			defer zr.Close()

			_, err = io.Copy(s.buffer, zr)
			if err != nil {
				return 0, xerrors.Errorf("failed to decompress deflate error: %w", err)
			}

			return m.Value, nil

		case MARKER_EOS:
			// Do not use end of stream
		case MARKER_GT:
			// Do not use grain tables data
			for i := uint64(0); i < m.Value; i++ {
				if _, err := s.r.Read(sector); err != nil {
					return 0, xerrors.Errorf("failed to read Grain Table error: %w", err)
				}
			}
		case MARKER_GD:
			// Do not use grain directries data
			for i := uint64(0); i < m.Value; i++ {
				if _, err := s.r.Read(sector); err != nil {
					return 0, xerrors.Errorf("failed to read Grain Directory error: %w", err)
				}
			}
		case MARKER_FOOTER:
			return 0, io.EOF
		default:
			return 0, xerrors.New("Invalid Marker Type")
		}
	}
}

func (s *streamOptimizedExtentReader) Close() (err error) {
	// TODO: close
	return nil
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
