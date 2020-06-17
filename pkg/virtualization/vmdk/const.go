package vmdk

const (
	MARKER_EOS    = uint32(0x00000000)
	MARKER_GT     = uint32(0x00000001)
	MARKER_GD     = uint32(0x00000002)
	MARKER_FOOTER = uint32(0x00000003)
	MARKER_GRAIN  = uint32(0xffffffff)

	COWD = uint32(0x434f5744)
	KDMV = uint32(0x564d444b)
)

type ExtentType int

const (
	FLAT = iota
	SPARSE
	ZERO
	VMFS
	VMFSSPARSE
	VMFSRDM
	VMFSRAW
)

type CreateType int

const (
	StreamOptimized = iota

/* Not Support */
// 	Custom
// 	MonolithicSparse
// 	MonolithicFlat
// 	TwoGbMaxExtentSparse
// 	TwoGbMaxExtentFlat
// 	FullDevice
// 	PartitionedDevice
// 	VmfsPreallocated
// 	VmfsEagerZeroedThick
// 	VmfsThin
// 	VmfsSparse
// 	VmfsRDM
// 	VmfsRDMP
// 	VmfsRaw
)
