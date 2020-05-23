package vmdk

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/xerrors"
)

type StreamOptimizedExtent struct {
	header Header
	reader io.Reader
	buffer bytes.Buffer
}

func (s *StreamOptimizedExtent) Read(p []byte) (n int, err error) {
	sector := make([]byte, Sector)
	if _, err := s.reader.Read(sector); err != nil {
		return 0, xerrors.Errorf("failed to stream optimized extent read error: %w", err)
	}

	m := parseMarker(sector)
	if m.Type != MARKER_GRAIN {
		return 0, xerrors.Errorf("failed to stream optimized extent read error: %w", err)
	}

	return 0, nil
}

func (s *StreamOptimizedExtent) Seek(offset int64, whence int) (n int64, err error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
	case io.SeekEnd:
	}
	return 0, nil
}

func (s *StreamOptimizedExtent) Close() (err error) {
	return nil
}

func parseMarker(sector []byte) *Marker {
	_ = sector[511]

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
