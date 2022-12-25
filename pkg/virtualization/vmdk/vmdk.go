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
	return header, nil
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

	var descriptor DiskDescriptor
	scanner := bufio.NewScanner(io.LimitReader(rs, Sector*header.DescriptorSize))
	var currentSectionFunc func(string, *DiskDescriptor) error
	for {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
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
	return descriptor, nil
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
