package vmdk

import (
	"encoding/binary"
	"io"

	"golang.org/x/xerrors"
)

var (
	_ sectionReaderInterface = &MonolithicSparseImage{}
)

type MonolithicSparseImage struct {
	VMDK

	GD      GrainDirectory
	GTCache map[int64]GrainTable
}

func NewMonolithicSparseImage(v VMDK) (*MonolithicSparseImage, error) {
	gd, err := parseGrainDirectoryDirect(v.rs, v.Header)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse grain directory: %w", err)
	}

	return &MonolithicSparseImage{
		VMDK:    v,
		GD:      gd,
		GTCache: make(map[int64]GrainTable),
	}, nil
}

// parseGrainDirectoryDirect reads GD entries directly from the offset
// specified in the header, without markers.
func parseGrainDirectoryDirect(rs io.ReadSeeker, header Header) (GrainDirectory, error) {
	offset := header.GdOffset * Sector
	off, err := rs.Seek(offset, io.SeekStart)
	if err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to seek to grain directory: %w", err)
	}
	if off != offset {
		return GrainDirectory{}, xerrors.Errorf(ErrSeekOffsetFormat, off, offset)
	}

	numGDEntries := numGrainDirectoryEntries(header)
	entries := make([]Entry, numGDEntries)
	if err := binary.Read(rs, binary.LittleEndian, entries); err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to read grain directory entries: %w", err)
	}

	return GrainDirectory{Entries: entries}, nil
}

func numGrainDirectoryEntries(header Header) int64 {
	numGrains := (header.Capacity + header.GrainSize - 1) / header.GrainSize
	return (numGrains + int64(header.NumGTEsPerGT) - 1) / int64(header.NumGTEsPerGT)
}

// parseGrainTableDirect reads GT entries directly from the offset, without markers.
func (v *MonolithicSparseImage) parseGrainTableDirect(gtOffset int64) (GrainTable, error) {
	offset := gtOffset * Sector
	off, err := v.rs.Seek(offset, io.SeekStart)
	if err != nil {
		return GrainTable{}, xerrors.Errorf("failed to seek to grain table: %w", err)
	}
	if off != offset {
		return GrainTable{}, xerrors.Errorf(ErrSeekOffsetFormat, off, offset)
	}

	entries := make([]Entry, v.Header.NumGTEsPerGT)
	if err := binary.Read(v.rs, binary.LittleEndian, entries); err != nil {
		return GrainTable{}, xerrors.Errorf("failed to read grain table entries: %w", err)
	}

	return GrainTable{Entries: entries}, nil
}

func (v *MonolithicSparseImage) TranslateOffset(off int64) (int64, int64, error) {
	grain := v.Header.GrainSize * Sector
	gtSize := grain * int64(v.Header.NumGTEsPerGT)

	gtIndex := off / gtSize
	if gtIndex >= int64(len(v.GD.Entries)) {
		return 0, 0, ErrDataNotPresent
	}

	gtOffset := int64(v.GD.Entries[gtIndex])
	if gtOffset == 0 {
		return 0, 0, ErrDataNotPresent
	}

	gt, ok := v.GTCache[gtOffset]
	if !ok {
		var err error
		gt, err = v.parseGrainTableDirect(gtOffset)
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to parse grain table entries: %w", err)
		}
		v.GTCache[gtOffset] = gt
	}

	entryIndex := off % gtSize / grain
	if entryIndex >= int64(len(gt.Entries)) {
		return 0, 0, ErrDataNotPresent
	}

	grainOffset := gt.Entries[entryIndex]
	if grainOffset == 0 {
		return 0, 0, ErrDataNotPresent
	}

	dataOffset := off % gtSize % grain
	return int64(grainOffset), dataOffset, nil
}

func (v *MonolithicSparseImage) read(grainOffset int64) ([]byte, error) {
	cacheKey := grainOffsetCacheKey(grainOffset)
	data, ok := v.cache.Get(cacheKey)
	if ok {
		return data, nil
	}

	offset := grainOffset * Sector
	off, err := v.rs.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek to grain data: %w", err)
	}
	if off != offset {
		return nil, xerrors.Errorf(ErrSeekOffsetFormat, off, offset)
	}

	grainDataSize := v.Header.GrainSize * Sector
	buf := make([]byte, grainDataSize)
	n, err := io.ReadFull(v.rs, buf)
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain data: %w", err)
	}
	if int64(n) != grainDataSize {
		return nil, xerrors.Errorf(ErrReadSizeFormat, n, grainDataSize)
	}

	v.cache.Add(cacheKey, buf)
	return buf, nil
}

func (v *MonolithicSparseImage) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) != int(Sector) {
		return 0, xerrors.Errorf("invalid byte length %d, required %d bytes length", len(p), Sector)
	}
	grainOffset, dataOffset, err := v.TranslateOffset(off)
	if err == ErrDataNotPresent {
		for i := range p {
			p[i] = 0
		}
		return int(Sector), nil
	} else if err != nil {
		return 0, xerrors.Errorf("failed to translate offset: %w", err)
	}

	data, err := v.read(grainOffset)
	if err != nil {
		return 0, xerrors.Errorf("failed to read data: %w", err)
	}

	if v.Header.GrainSize*Sector-dataOffset < Sector {
		return copy(p, data[dataOffset:]), nil
	}
	return copy(p, data[dataOffset:dataOffset+Sector]), nil
}
