package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"golang.org/x/xerrors"
	"io"
	"unsafe"
)

type Marker struct {
	Value uint64
	Size  uint32
	Type  uint32
	Data  []byte
}

type StreamOptimizedImage struct {
	VMDK

	SparseExtentHeader SparseExtentHeader
	GD                 GrainDirectory
	GT                 GrainTable
	state              State
	sinfo              StateInfo
}

type StateInfo struct {
	batIndex       int
	blockOffset    int64
	fileOffset     int64
	bytesAvailable int64
}

type State struct {
	chunkRatio            uint32
	chunkRatioBits        int
	sectorPerBlock        uint32
	sectorPerBlockBits    int
	logicalSectorSizeBits int
}

type SparseExtentHeader struct {
	MagicNumber        uint32
	Version            uint32
	Flags              uint32
	Capacity           uint64
	GrainSize          uint64
	DescriptorOffset   uint64
	DescriptorSize     uint64
	NumberGTEsPerGT    uint32
	RgdOffset          uint64
	GdOffset           uint64
	OverHead           uint64
	UncleanShutdown    uint8
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  uint16
}

type Entry int32

type GrainDirectory struct {
	Entries []Entry
}

type GrainTable struct {
	Entries []Entry
}

var (
	ErrDataNotPresent = xerrors.New("data not present")
)

func parseSparseExtentHeader(v VMDK) (SparseExtentHeader, error) {
	_, err := v.f.Seek(-1*Sector*2, 2)
	if err != nil {
		return SparseExtentHeader{}, err
	}

	h := SparseExtentHeader{}
	err = binary.Read(v.f, binary.LittleEndian, &h)
	if err != nil {
		return SparseExtentHeader{}, err
	}
	// TODO: check magick number

	return h, nil
}

func (h SparseExtentHeader) parseGrainDirectoryEntries(v VMDK) (GrainDirectory, error) {
	_, err := v.f.Seek(int64(h.GdOffset-1)*Sector, 0)
	if err != nil {
		return GrainDirectory{}, err
	}

	buf := make([]byte, Sector)
	_, err = v.f.Read(buf)
	if err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to read grain directory marker: %w", err)
	}
	marker := parseMarker(buf)
	if marker.Type != MARKER_GD {
		return GrainDirectory{}, xerrors.Errorf("invalid marker: %d, expected: %d", marker.Type, MARKER_GD)
	}

	buf = make([]byte, Sector*int64(marker.Value))
	_, err = v.f.Read(buf)
	if err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to read grain directory: %w", err)
	}

	var gd GrainDirectory
	gd.Entries, err = parseEntries(buf, marker.Value)
	if err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to parse entries: %w", err)
	}

	return gd, nil
}

func (v StreamOptimizedImage) parseGrainTableEntries(gdeOffset int64) (GrainTable, error) {
	var gt GrainTable
	_, err := v.f.Seek((gdeOffset-1)*Sector, 0)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to seek to grain table offset: %w", err)
	}

	buf := make([]byte, Sector)
	_, err = v.f.Read(buf)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to read grain table marker: %w", err)
	}
	marker := parseMarker(buf)
	if marker.Type != MARKER_GT {
		return GrainTable{}, xerrors.Errorf("invalid marker: %d, expected: %d", marker.Type, MARKER_GT)
	}

	buf = make([]byte, Sector*int64(marker.Value))
	_, err = v.f.Read(buf)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to read grain directory: %w", err)
	}

	entries, err := parseEntries(buf, marker.Value)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to parse entries: %w", err)
	}
	gt.Entries = append(gt.Entries, entries...)

	return gt, nil
}

func parseEntries(buf []byte, value uint64) ([]Entry, error) {
	entrySize := int64(unsafe.Sizeof(Entry(0)))
	r := bytes.NewReader(buf)
	var entries []Entry
	for i := int64(0); i < Sector*int64(value)/entrySize; i++ {
		var e Entry
		if err := binary.Read(r, binary.LittleEndian, &e); err != nil {
			return nil, xerrors.Errorf("failed to parse entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func NewStreamOptimizedImage(v VMDK) (*StreamOptimizedImage, error) {
	h, err := parseSparseExtentHeader(v)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse sparse extent header: %w", err)
	}

	gd, err := h.parseGrainDirectoryEntries(v)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse grain directory: %w", err)
	}

	return &StreamOptimizedImage{
		VMDK:               v,
		SparseExtentHeader: h,
		GD:                 gd,
	}, nil
}

func (v *StreamOptimizedImage) Size() int64 {
	var size int64
	for _, extent := range v.DiskDescriptor.Extents {
		size += extent.Size
	}
	return size * Sector
}

func (v *StreamOptimizedImage) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) != int(Sector) {
		return 0, xerrors.Errorf("invalid byte length %d, required %d bytes length", len(p), Sector)
	}
	grainOffset, dataOffset, err := v.TranslateOffset(off)
	if err == ErrDataNotPresent {
		return int(Sector), nil
	} else if err != nil {
		return 0, xerrors.Errorf("failed to translate offset: %w", err)
	}

	b, err := v.readGrain(grainOffset)
	if err != nil {
		return 0, xerrors.Errorf("failed to read grain data: %w", err)
	}
	zr, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return 0, xerrors.Errorf("failed to read zlib error: %w", err)
	}
	defer zr.Close()

	cache := make([]byte, 65536)
	_, err = io.ReadFull(zr, cache)
	if err != nil {
		return 0, xerrors.Errorf("failed to decompress deflate error: %w", err)
	}

	if v.Header.GrainSize*Sector-dataOffset < Sector {
		return copy(p, cache[dataOffset:]), nil
	} else {
		return copy(p, cache[dataOffset:dataOffset+Sector]), nil
	}
}

func (v *StreamOptimizedImage) readGrain(grainOffset int64) ([]byte, error) {
	_, err := v.f.Seek(grainOffset*Sector, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek to grain data offset: %w", err)
	}

	buf := make([]byte, Sector)
	_, err = v.f.Read(buf)
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain marker: %w", err)
	}
	m := parseMarker(buf)
	if m.Type != MARKER_GRAIN {
		return nil, xerrors.Errorf("invalid marker type: %d, expected: %d", m.Type, MARKER_GRAIN)
	}
	if m.Size == 0 {
		return nil, xerrors.Errorf("invalid grain size: %d", m.Size)
	}

	if m.Size < 500 {
		return m.Data[:m.Size], nil
	}
	readAvailable := m.Size - 500

	buf = make([]byte, readAvailable)
	_, err = v.f.Read(buf)
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain data: %w", err)
	}

	return append(m.Data, buf...), nil
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

func (v *StreamOptimizedImage) TranslateOffset(off int64) (int64, int64, error) {
	// grainSize: 128
	// sector: 512
	// grain: 64KB (decompressed deflate)
	grain := v.Header.GrainSize * Sector

	// number GTEs per GT: 512
	// gtSize: 32MB
	gtSize := grain * int64(v.SparseExtentHeader.NumberGTEsPerGT)

	// offset: 32MB + 4KB
	// gtSize: 32MB
	// gtIndex: 1
	gtIndex := off / gtSize
	gtOffset := int64(v.GD.Entries[gtIndex])
	if gtOffset == 0 {
		return 0, 0, ErrDataNotPresent
	}
	gt, err := v.parseGrainTableEntries(gtOffset)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to parse grain table entries: %w", err)
	}

	// logical grain data offset.
	// offset: 32MB + 4KB
	// gtSize: 32MB
	// grain: 64KB
	// entryIndex: 0
	// grainOffset gt[entryOffset]
	entryIndex := off % gtSize / grain
	grainOffset := gt.Entries[entryIndex]
	if grainOffset == 0 {
		return 0, 0, ErrDataNotPresent
	}

	// dataOffset: 4KB
	dataOffset := off % gtSize % grain

	return int64(grainOffset), dataOffset, nil
}
