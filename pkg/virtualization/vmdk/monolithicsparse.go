package vmdk

import (
	"encoding/binary"
	"io"
	"sync"

	"golang.org/x/xerrors"
)

var (
	_ sectionReaderInterface = &MonolithicSparseImage{}
)

type MonolithicSparseImage struct {
	VMDK

	GD      GrainDirectory
	GTCache map[int64]GrainTable

	// gtMu guards GTCache.
	gtMu sync.Mutex
}

func NewMonolithicSparseImage(v VMDK) (*MonolithicSparseImage, error) {
	gd, err := parseGrainDirectoryDirect(v.ra, v.Header)
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
func parseGrainDirectoryDirect(ra io.ReaderAt, header Header) (GrainDirectory, error) {
	if header.GdOffset <= 0 {
		return GrainDirectory{}, xerrors.Errorf("invalid GdOffset: %d", header.GdOffset)
	}
	numGDEntries, err := numGrainDirectoryEntries(header)
	if err != nil {
		return GrainDirectory{}, err
	}

	entries := make([]Entry, numGDEntries)
	r := io.NewSectionReader(ra, header.GdOffset*Sector, numGDEntries*int64(binary.Size(Entry(0))))
	if err := binary.Read(r, binary.LittleEndian, entries); err != nil {
		return GrainDirectory{}, xerrors.Errorf("failed to read grain directory entries: %w", err)
	}

	return GrainDirectory{Entries: entries}, nil
}

func numGrainDirectoryEntries(header Header) (int64, error) {
	if header.GrainSize == 0 || header.NumGTEsPerGT == 0 {
		return 0, xerrors.Errorf("invalid header: GrainSize=%d NumGTEsPerGT=%d", header.GrainSize, header.NumGTEsPerGT)
	}
	numGrains := (header.Capacity + header.GrainSize - 1) / header.GrainSize
	return (numGrains + int64(header.NumGTEsPerGT) - 1) / int64(header.NumGTEsPerGT), nil
}

// parseGrainTableDirect reads GT entries directly from the offset, without markers.
func (v *MonolithicSparseImage) parseGrainTableDirect(gtOffset int64) (GrainTable, error) {
	entries := make([]Entry, v.Header.NumGTEsPerGT)
	r := io.NewSectionReader(v.ra, gtOffset*Sector, int64(len(entries)*binary.Size(Entry(0))))
	if err := binary.Read(r, binary.LittleEndian, entries); err != nil {
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

	// The grain table is parsed outside the lock. Concurrent misses on the same
	// gtOffset may parse it twice, which is harmless because a GrainTable is never modified.
	v.gtMu.Lock()
	gt, ok := v.GTCache[gtOffset]
	v.gtMu.Unlock()
	if !ok {
		var err error
		gt, err = v.parseGrainTableDirect(gtOffset)
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to parse grain table entries: %w", err)
		}
		v.gtMu.Lock()
		v.GTCache[gtOffset] = gt
		v.gtMu.Unlock()
	}

	entryIndex := off % gtSize / grain
	if entryIndex >= int64(len(gt.Entries)) {
		return 0, 0, ErrDataNotPresent
	}

	grainOffset := gt.Entries[entryIndex]
	if isGTEAbsent(grainOffset, uint32(v.Header.Flag)) {
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

	buf, err := readFullAt(v.ra, grainOffset*Sector, v.grainDataSize())
	if err != nil {
		return nil, xerrors.Errorf("failed to read grain data: %w", err)
	}

	v.cache.Add(cacheKey, buf)
	return buf, nil
}

func (v *MonolithicSparseImage) grainDataSize() int64 {
	return v.Header.GrainSize * Sector
}

func (v *MonolithicSparseImage) ReadAt(p []byte, off int64) (int, error) {
	return readAt(v, p, off)
}
