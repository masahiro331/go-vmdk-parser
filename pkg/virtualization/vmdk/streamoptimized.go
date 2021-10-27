package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"log"
	"math"

	"github.com/masahiro331/go-vmdk-parser/pkg/disk"
	"github.com/masahiro331/go-vmdk-parser/pkg/disk/types"
	"golang.org/x/xerrors"
)

const (
	CLUSTER_SIZE = 128
	SECOTR_SIZE  = 512
)

type streamOptimizedExtentReader struct {
	r io.Reader

	header        Header
	buffer        *bytes.Buffer
	secondbuffer  *bytes.Buffer
	sectorPos     uint64
	fileSectorPos uint64
	writeSize     uint64
	diskDriver    disk.Driver
	partition     types.Partition
}

// Read '0x100000' bytes in NewReader for get master record
func NewStreamOptimizedReader(r io.Reader, header Header) (Reader, error) {
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
		buffer:       &bytes.Buffer{},
		secondbuffer: &bytes.Buffer{},
		header:       header,
		r:            r,
	}

	_, err := reader.readGrainData()
	if err != nil {
		return nil, xerrors.Errorf("failed to read tail data: %w", err)
	}

	reader.diskDriver, err = disk.NewDriver(reader.buffer)
	if err != nil {
		return nil, xerrors.Errorf("failed to new driver: %w", err)
	}
	reader.buffer.Reset()

	return &reader, nil
}

func (s *streamOptimizedExtentReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, xerrors.New("invalid byte size")
	}
	for {
		if s.partition != nil &&
			s.fileSectorPos == s.partition.GetStartSector()+s.partition.GetSize() {

			if s.secondbuffer.Len() == 0 {
				return 0, io.EOF
			}

			// ref :117
			i, err := s.writeReaderFromSecondBuffer(p)
			if err != nil {
				if err != io.EOF {
					log.Fatalf("unknown err %s", err)
				}
				return i, err
			}
			return i, err
		}

		if s.buffer.Len() == 0 {
			s.fileSectorPos = s.sectorPos + CLUSTER_SIZE
			_, err := s.readGrainData()
			if err != nil {
				if err != io.EOF {
					log.Fatalf("unknown err %s", err)
				}
				return 0, err
			}

			continue
		}

		if s.fileSectorPos == s.sectorPos && s.secondbuffer.Len() == 0 {
			i, err := s.writeReaderFromBuffer(p)
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
				_, err := s.secondbuffer.Write(make([]byte, SECOTR_SIZE*CLUSTER_SIZE))
				if err != nil {
					return 0, xerrors.Errorf("failed to write second buffer: %w", err)
				}
			}

			i, err := s.writeReaderFromSecondBuffer(p)
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

func (s *streamOptimizedExtentReader) writeReaderFromBuffer(p []byte) (int, error) {
	if (s.writeSize + uint64(len(p))) > (s.partition.GetSize() * SECOTR_SIZE) {
		ws := (s.partition.GetSize() * SECOTR_SIZE) - s.writeSize
		s.writeSize += ws
		i, _ := s.buffer.Read(p[:ws])
		return i, io.EOF
	}

	s.writeSize += uint64(len(p))
	return s.buffer.Read(p)
}
func (s *streamOptimizedExtentReader) writeReaderFromSecondBuffer(p []byte) (int, error) {
	if (s.writeSize + uint64(len(p))) > (s.partition.GetSize() * SECOTR_SIZE) {
		ws := (s.partition.GetSize() * SECOTR_SIZE) - s.writeSize
		s.writeSize += ws
		i, _ := s.buffer.Read(p[:ws])
		return i, io.EOF
	}

	s.writeSize += uint64(len(p))
	return s.secondbuffer.Read(p)
}

func (s *streamOptimizedExtentReader) Next() (types.Partition, error) {
	s.secondbuffer.Reset()
	// s.buffer.Reset()
	s.writeSize = 0
	partitions := s.diskDriver.GetPartitions()
	if s.partition == nil {
		p := s.diskDriver.GetPartitions()[0]
		s.partition = p
	} else {
		for _, p := range partitions {
			if p.GetStartSector() > s.partition.GetStartSector() {
				s.partition = p
				break
			} else if p.GetStartSector() == 0 ||
				partitions[len(partitions)-1].GetStartSector() == p.GetStartSector() {
				return nil, io.EOF
			}
		}
	}
	if s.partition.GetStartSector() == s.sectorPos {
		s.fileSectorPos = s.sectorPos
		return s.partition, nil
	} else {
		s.buffer.Reset()
	}

	var err error
	for {
		if s.partition.GetStartSector() > s.sectorPos {
			s.sectorPos, err = s.readGrainData()
			if err != nil {
				return nil, xerrors.Errorf("failed to next error: %w", err)
			}
			if s.partition.GetStartSector() <= s.sectorPos {
				s.fileSectorPos = s.sectorPos
				return s.partition, nil
			}
		} else {
			s.fileSectorPos = s.sectorPos
			return s.partition, nil
		}
		s.buffer.Reset()
	}
}

// TODO: return read size
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
				_, err := buf.Write(m.Data[:m.Size])
				if err != nil {
					return 0, xerrors.Errorf("failed to write data: %w", err)
				}
			} else {
				_, err := buf.Write(m.Data)
				if err != nil {
					return 0, xerrors.Errorf("failed to write data: %w", err)
				}
				limit := uint64(math.Ceil(float64(m.Size-500) / float64(Sector)))
				for i := uint64(0); i < limit; i++ {
					if _, err := s.r.Read(sector); err != nil {
						return 0, xerrors.Errorf("failed to read Grain Data error: %w", err)
					}
					_, err := buf.Write(sector)
					if err != nil {
						return 0, xerrors.Errorf("failed to write data: %w", err)
					}
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
