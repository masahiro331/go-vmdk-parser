package types

type Partition interface {
	Bootable() bool
	GetStartSector() uint64
	GetSize() uint64
	Name() string
	GetType() []byte

	IsSupported() bool
}
