package vmdk

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/xerrors"
)

var Sector int64 = 0x200

type Header struct {
	Signature          int32
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
	SectionDiskDescriptorFile = "disk descriptorfile"
	SectionExtentDescription  = "extent description"
	SectionDiskDataBase       = "the disk data base"
)

type sectionReaderInterface interface {
	io.ReaderAt
	Size() int64
}

var _ sectionReaderInterface = &StreamOptimizedImage{}

type VMDK struct {
	Header         Header
	DiskDescriptor DiskDescriptor

	f *os.File
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

func Open(name string) (*io.SectionReader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, xerrors.Errorf("failed to open %s: %w", name, err)
	}

	v := VMDK{f: f}
	var header Header
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return nil, xerrors.Errorf("failed to read binary error: %w", err)
	}
	v.Header = header

	_, err = f.Seek(header.DescriptorOffset*Sector, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek descriptor: %w", err)
	}
	v.DiskDescriptor, err = parseDiskDescriptor(io.LimitReader(f, Sector*header.DescriptorSize))
	if err != nil {
		return nil, xerrors.Errorf("failed to parse disk descriptor: %w", err)
	}
	if len(v.DiskDescriptor.Extents) != 1 {
		return nil, xerrors.New("divided images are not supported")
	}

	var r sectionReaderInterface
	switch v.DiskDescriptor.CreateType {
	case StreamOptimized:
		r, err = NewStreamOptimizedImage(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to new stream optimized image: %w", err)
		}
	default:
		return nil, xerrors.Errorf("%s type is not supported", v.DiskDescriptor.CreateType)
	}

	return io.NewSectionReader(r, 0, r.Size()), nil
}

func parseDiskDescriptor(r io.Reader) (DiskDescriptor, error) {
	descriptor := DiskDescriptor{}
	scanner := bufio.NewScanner(r)

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
		case SectionDiskDataBase:
			currentSectionFunc = parseDiskDataBase
		default:
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
	ss := strings.Fields(line)
	if len(ss) != 4 {
		return xerrors.Errorf("failed to parse disk extents: %s", line)
	}

	extent := ExtentDescription{
		Mode: ss[0],
		Type: ss[2],
		Name: strings.Trim(ss[3], "\""),
	}

	size, err := strconv.ParseInt(ss[1], 0, 64)
	if err != nil {
		return xerrors.Errorf("failed to parse disk size: %s", ss[1])
	}
	extent.Size = size

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
