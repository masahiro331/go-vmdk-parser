package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"golang.org/x/xerrors"
)

var (
	ErrReadSizeFormat   = "failed to read size error: actual(%d), expected(%d)"
	ErrSeekOffsetFormat = "failed to seek offset error: actual(%d), expected(%d)"
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
	GTCache            map[int64]GrainTable
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

func parseSparseExtentHeader(ra io.ReaderAt, fileSize int64) (SparseExtentHeader, error) {
	// Sparse extent header is in the last 1024 bytes.
	if fileSize < 1024 {
		return SparseExtentHeader{}, xerrors.Errorf("file is too small for sparse extent header: %d", fileSize)
	}

	h := SparseExtentHeader{}
	err := binary.Read(io.NewSectionReader(ra, fileSize-1024, 1024), binary.LittleEndian, &h)
	if err != nil {
		return SparseExtentHeader{}, xerrors.Errorf("failed to read binary error: %w", err)
	}
	if h.MagicNumber != KDMV {
		return SparseExtentHeader{}, xerrors.Errorf("invalid magick number: actual(0x%08x), expected(0x%08x)", h.MagicNumber, KDMV)
	}

	if err := validateIncompatFlags(h.Flags); err != nil {
		return SparseExtentHeader{}, err
	}

	return h, nil
}

func (h SparseExtentHeader) parseGrainDirectoryEntries(ra io.ReaderAt) (GrainDirectory, error) {
	offset := int64(h.GdOffset-1) * Sector
	buf, err := readFullAt(ra, offset, Sector)
	if err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to read grain directory marker: %w", err)
	}

	marker := parseMarker(buf)
	if marker.Type != MARKER_GD {
		return GrainDirectory{}, xerrors.Errorf("invalid marker: %d, expected: %d", marker.Type, MARKER_GD)
	}

	dataSize := Sector * int64(marker.Value)
	buf, err = readFullAt(ra, offset+Sector, dataSize)
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

func (v *StreamOptimizedImage) parseGrainTableEntries(gdeOffset int64) (GrainTable, error) {
	var gt GrainTable

	offset := (gdeOffset - 1) * Sector
	buf, err := readFullAt(v.ra, offset, Sector)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to read grain table marker: %w", err)
	}

	marker := parseMarker(buf)
	if marker.Type != MARKER_GT {
		return GrainTable{}, xerrors.Errorf("invalid marker: actual(%d), expected(%d)", marker.Type, MARKER_GT)
	}

	dataSize := Sector * int64(marker.Value)
	buf, err = readFullAt(v.ra, offset+Sector, dataSize)
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
			return nil, xerrors.Errorf("failed to parse entries[%d]: %w", i, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func NewStreamOptimizedImage(v VMDK) (*StreamOptimizedImage, error) {
	h, err := parseSparseExtentHeader(v.ra, v.fileSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse sparse extent header: %w", err)
	}

	gd, err := h.parseGrainDirectoryEntries(v.ra)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse grain directory: %w", err)
	}

	return &StreamOptimizedImage{
		VMDK:               v,
		SparseExtentHeader: h,
		GD:                 gd,
		GTCache:            make(map[int64]GrainTable),
	}, nil
}

func grainOffsetCacheKey(n int64) string {
	return fmt.Sprintf("vmdk:%d", n)
}

func (v *StreamOptimizedImage) read(grainOffset int64) ([]byte, error) {
	cacheKey := grainOffsetCacheKey(grainOffset)
	data, ok := v.cache.Get(cacheKey)
	if ok {
		return data, nil
	}

	b, err := v.readGrain(grainOffset)
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain data: %w", err)
	}
	zr, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, xerrors.Errorf("failed to read zlib error: %w", err)
	}
	defer zr.Close()

	grainDataSize := int64(v.SparseExtentHeader.GrainSize) * Sector
	decompressedData := make([]byte, grainDataSize)
	n, err := io.ReadFull(zr, decompressedData)
	if err != nil {
		return nil, xerrors.Errorf("failed to decompress deflate error: %w", err)
	}
	if int64(n) != grainDataSize {
		return nil, xerrors.Errorf(ErrReadSizeFormat, n, grainDataSize)
	}
	v.cache.Add(cacheKey, decompressedData)
	return decompressedData, nil
}

func (v *StreamOptimizedImage) grainDataSize() int64 {
	return int64(v.SparseExtentHeader.GrainSize) * Sector
}

func (v *StreamOptimizedImage) ReadAt(p []byte, off int64) (int, error) {
	return readAt(v, p, off)
}

func (v *StreamOptimizedImage) readGrain(grainOffset int64) ([]byte, error) {
	offset := grainOffset * Sector
	buf, err := readFullAt(v.ra, offset, Sector)
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

	// grain marker has 500 bytes data
	if m.Size < 500 {
		return m.Data[:m.Size], nil
	}
	readAvailable := m.Size - 500

	buf, err = readFullAt(v.ra, offset+Sector, int64(readAvailable))
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain data: %w", err)
	}

	return append(m.Data, buf...), nil
}

// Marker Specs ( 512 bytes )
// +--------+------+-------------+
// | Offset | Size | Description |
// +--------+------+-------------+
// | 0      | 8    | Value       | is other data
// | 8      | 4    | Data Size   |
// | 12     | 4    | Marker Type |
// | 16     | 496  | Padding     |
// +--------+------+-------------+
// | if marker size > 0          | is grainData
// | 12     | ...  | GrainData   |
// +--------+------+-------------+
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

// TranslateOffset is translates the physical offset of a VMDK into a logical offset.
func (v *StreamOptimizedImage) TranslateOffset(off int64) (int64, int64, error) {
	var err error

	// grainSize: 128
	// sector: 512
	// grain: 64KB (decompressed deflate)
	grain := int64(v.SparseExtentHeader.GrainSize) * Sector

	// number GTEs per GT: 512
	// gtSize: 32MB
	gtSize := grain * int64(v.SparseExtentHeader.NumberGTEsPerGT)

	// offset: 32MB + 4KB
	// gtSize: 32MB
	// gtIndex: 1
	gtIndex := off / gtSize
	if gtIndex >= int64(len(v.GD.Entries)) {
		return 0, 0, ErrDataNotPresent
	}
	gtOffset := int64(v.GD.Entries[gtIndex])
	if gtOffset == 0 {
		return 0, 0, ErrDataNotPresent
	}
	var gt GrainTable
	gt, ok := v.GTCache[gtOffset]
	if !ok {
		gt, err = v.parseGrainTableEntries(gtOffset)
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to parse grain table entries: %w", err)
		}
		v.GTCache[gtOffset] = gt
	}

	// logical grain data offset.
	// offset: 32MB + 4KB
	// gtSize: 32MB
	// grain: 64KB
	// entryIndex: 0
	// grainOffset gt[entryOffset]
	entryIndex := off % gtSize / grain
	if entryIndex >= int64(len(gt.Entries)) {
		return 0, 0, ErrDataNotPresent
	}
	grainOffset := gt.Entries[entryIndex]
	if isGTEAbsent(grainOffset, v.SparseExtentHeader.Flags) {
		return 0, 0, ErrDataNotPresent
	}

	// dataOffset: 4KB
	// dataOffset less than grain(default: 64KB)
	dataOffset := off % gtSize % grain

	return int64(grainOffset), dataOffset, nil
}
