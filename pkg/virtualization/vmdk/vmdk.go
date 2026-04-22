package vmdk

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"
	"strings"

	"golang.org/x/xerrors"
)

// Header specification https://www.vmware.com/app/vmdk/?src=vmdk
type Header struct {
	Signature          uint32
	Version            int32
	Flag               int32
	Capacity           int64
	GrainSize          int64
	DescriptorOffset   int64
	DescriptorSize     int64
	NumGTEsPerGT       int32
	RgdOffset          int64
	GdOffset           int64
	OverHead           int64
	UncleanShutdown    byte
	SingleEndLineChar  byte
	NonEndLineChar     byte
	DoubleEndLineChar1 byte
	DoubleEndLineChar2 byte
	CompressAlgorithm  int16
	Padding            [433]byte
}

const (
	SectionDiskDescriptorFile       = "disk descriptorfile"
	SectionExtentDescription        = "extent description"
	SectionDiskDataBase             = "the disk data base"
	SectionDDB                      = "ddb"
	Sector                    int64 = 0x200
)

type sectionReaderInterface interface {
	io.ReaderAt
	Size() int64
}

// grainReader abstracts grain-level read operations shared by
// StreamOptimizedImage and MonolithicSparseImage.
type grainReader interface {
	TranslateOffset(off int64) (grainOffset int64, dataOffset int64, err error)
	read(grainOffset int64) ([]byte, error)
	grainDataSize() int64
	Size() int64
}

// readAt implements io.ReaderAt for any grainReader by looping over grains.
func readAt(gr grainReader, p []byte, off int64) (int, error) {
	totalSize := gr.Size()
	if off >= totalSize {
		return 0, io.EOF
	}

	grain := gr.grainDataSize()
	totalRead := 0
	for totalRead < len(p) {
		currentOff := off + int64(totalRead)
		if currentOff >= totalSize {
			return totalRead, io.EOF
		}

		grainOff, dataOff, err := gr.TranslateOffset(currentOff)
		if err == ErrDataNotPresent {
			// Zero-fill up to next grain boundary or remaining buffer
			zeroLen := grain - (currentOff % grain)
			remaining := int64(len(p) - totalRead)
			if zeroLen > remaining {
				zeroLen = remaining
			}
			if currentOff+zeroLen > totalSize {
				zeroLen = totalSize - currentOff
			}
			zeroSlice := p[totalRead : totalRead+int(zeroLen)]
			for i := range zeroSlice {
				zeroSlice[i] = 0
			}
			totalRead += int(zeroLen)
			continue
		} else if err != nil {
			return totalRead, xerrors.Errorf("failed to translate offset: %w", err)
		}

		data, err := gr.read(grainOff)
		if err != nil {
			return totalRead, xerrors.Errorf("failed to read data: %w", err)
		}

		available := int64(len(data)) - dataOff
		if available <= 0 {
			return totalRead, xerrors.Errorf("invalid data offset %d for grain size %d", dataOff, len(data))
		}
		need := int64(len(p) - totalRead)
		if need > available {
			need = available
		}
		if currentOff+need > totalSize {
			need = totalSize - currentOff
		}
		copy(p[totalRead:], data[dataOff:dataOff+need])
		totalRead += int(need)
	}
	return totalRead, nil
}

var (
	_                          sectionReaderInterface = &StreamOptimizedImage{}
	ErrUnSupportedDividedImage                        = xerrors.New("divided images are not supported")
	ErrUnSupportedType                                = xerrors.New("type is not supported")
	ErrIsNotVMDK                                      = xerrors.New("this file is not vmdk")
)

type VMDK struct {
	Header         Header
	DiskDescriptor DiskDescriptor
	cache          Cache[string, []byte]

	rs io.ReadSeeker
}

func (v *VMDK) Size() int64 {
	var size int64
	for _, extent := range v.DiskDescriptor.Extents {
		size += extent.Size
	}
	return size * Sector
}

type DiskDescriptor struct {
	Version    int
	CID        string
	ParentCID  string
	CreateType string
	Extents    []ExtentDescription
}

type ExtentDescription struct {
	Mode string
	Size int64
	Type string
	Name string
}

func Check(r io.Reader) (bool, error) {
	var signature uint32
	if err := binary.Read(r, binary.LittleEndian, &signature); err != nil {
		return false, xerrors.Errorf("failed to read signature: %w", err)
	}
	return signature == KDMV, nil
}

func ParseHeader(r io.Reader) (Header, error) {
	var header Header
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return Header{}, xerrors.Errorf("failed to read binary error: %w", err)
	}
	if header.Signature != KDMV {
		return Header{}, xerrors.Errorf("invalid signature: actual(0x%08x), expected(0x%08x)", header.Signature, KDMV)
	}
	if err := validateIncompatFlags(uint32(header.Flag)); err != nil {
		return Header{}, err
	}
	return header, nil
}

// isGTEAbsent returns true if the grain table entry indicates no data.
// Per VMDK spec, GTE=1 is a valid sector offset when FlagUseZeroedGrainTableEntry is not set.
func isGTEAbsent(entry Entry, flags uint32) bool {
	return entry == GTEEmpty ||
		(entry == GTEZeroed && flags&FlagUseZeroedGrainTableEntry != 0)
}

// validateIncompatFlags rejects unknown incompatible flags and
// validates flag combinations per the VMDK spec.
func validateIncompatFlags(flags uint32) error {
	unknownIncompat := flags & incompatFlagsMask & ^knownIncompatFlags
	if unknownIncompat != 0 {
		return xerrors.Errorf("unknown incompatible flags: 0x%08x", unknownIncompat)
	}
	if flags&FlagEmbeddedLBA != 0 && flags&FlagCompressed == 0 {
		return xerrors.Errorf("EMBEDDED_LBA flag requires COMPRESSED flag")
	}
	return nil
}

func Open(rs io.ReadSeeker, cache Cache[string, []byte]) (*io.SectionReader, error) {
	var err error

	// If cache is not provided, use mock.
	if cache == nil {
		cache = &mockCache[string, []byte]{}
	}
	v := VMDK{rs: rs, cache: cache}
	v.Header, err = ParseHeader(v.rs)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	v.DiskDescriptor, err = ParseDiskDescriptor(v.rs, v.Header)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse disk descriptor: %w", err)
	}
	if len(v.DiskDescriptor.Extents) != 1 {
		// TODO: Support divided image (e.g. image1.vmdk, image2.vmdk, ... )
		return nil, ErrUnSupportedDividedImage
	}

	var r sectionReaderInterface
	switch v.DiskDescriptor.CreateType {
	case StreamOptimized:
		r, err = NewStreamOptimizedImage(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to new stream-optimized image: %w", err)
		}
	case MonolithicSparse:
		r, err = NewMonolithicSparseImage(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to new monolithic-sparse image: %w", err)
		}
	default:
		return nil, xerrors.Errorf("%s: %w", v.DiskDescriptor.CreateType, ErrUnSupportedType)
	}

	return io.NewSectionReader(r, io.SeekStart, r.Size()), nil
}

func ParseDiskDescriptor(rs io.ReadSeeker, header Header) (DiskDescriptor, error) {
	i, err := rs.Seek(header.DescriptorOffset*Sector, io.SeekStart)
	if err != nil {
		return DiskDescriptor{}, xerrors.Errorf("failed to seek descriptor: %w", err)
	}
	if i != header.DescriptorOffset*Sector {
		return DiskDescriptor{}, xerrors.Errorf(ErrSeekOffsetFormat, i, header.DescriptorOffset*Sector)
	}

	return parseDescriptorLines(bufio.NewScanner(io.LimitReader(rs, Sector*header.DescriptorSize)))
}

func parseDiskDataBase(line string, dd *DiskDescriptor) error {
	// TODO: parse not yet ...
	return nil
}

func parseExtentDescription(line string, dd *DiskDescriptor) error {
	if strings.HasPrefix(line, "#") {
		return nil
	}
	ss := strings.Fields(line)
	if len(ss) < 4 {
		return xerrors.Errorf("failed to parse disk extents: %s", line)
	}

	extent := ExtentDescription{
		Mode: ss[0],
		Type: ss[2],
		Name: strings.Trim(ss[3], "\""),
	}

	var err error
	extent.Size, err = strconv.ParseInt(ss[1], 0, 64)
	if err != nil {
		return xerrors.Errorf("failed to parse disk size: %s", ss[1])
	}

	dd.Extents = append(dd.Extents, extent)

	return nil
}

func parseDiskDescriptorFile(line string, dd *DiskDescriptor) error {
	switch {
	case strings.HasPrefix(line, "version="):
		vstr := strings.TrimPrefix(line, "version=")
		v, err := strconv.Atoi(vstr)
		if err != nil {
			return xerrors.Errorf("failed to parse version: %s", vstr)
		}
		dd.Version = v
	case strings.HasPrefix(line, "CID"):
		dd.CID = strings.TrimPrefix(line, "CID=")
	case strings.HasPrefix(line, "createType="):
		dd.CreateType = strings.Trim(strings.TrimPrefix(line, "createType="), "\"")
	case strings.HasPrefix(line, "parentCID="):
		dd.ParentCID = strings.TrimPrefix(line, "parentCID=")
	}
	return nil
}

func parseDescriptorLines(scanner *bufio.Scanner) (DiskDescriptor, error) {
	var descriptor DiskDescriptor
	var currentSectionFunc func(string, *DiskDescriptor) error
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "#"))) {
		case SectionDiskDescriptorFile:
			currentSectionFunc = parseDiskDescriptorFile
		case SectionExtentDescription:
			currentSectionFunc = parseExtentDescription
		case SectionDiskDataBase, SectionDDB:
			currentSectionFunc = parseDiskDataBase
		default:
			if currentSectionFunc == nil {
				return DiskDescriptor{}, xerrors.Errorf("invalid descriptor")
			}
			err := currentSectionFunc(line, &descriptor)
			if err != nil {
				return DiskDescriptor{}, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return DiskDescriptor{}, xerrors.Errorf("failed to scan descriptor: %w", err)
	}
	return descriptor, nil
}
