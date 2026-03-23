package vmdk

const (
	// MARKER_EOS    = uint32(0x00000000)
	MARKER_GT = uint32(0x00000001)
	MARKER_GD = uint32(0x00000002)
	// MARKER_FOOTER = uint32(0x00000003)
	MARKER_GRAIN = uint32(0xffffffff)

	// COWD = uint32(0x434f5744)
	KDMV = uint32(0x564d444b)

	// Sparse extent header flags
	FlagUseZeroedGrainTableEntry = int32(0x00000004)

	// Incompatible flags (upper 16 bits)
	FlagCompressed  = uint32(0x00010000)
	FlagEmbeddedLBA = uint32(0x00020000)
	// Mask for all known incompatible flags
	knownIncompatFlags = FlagCompressed | FlagEmbeddedLBA
	// Mask for incompatible flag bits
	incompatFlagsMask = uint32(0xFFFF0000)
)

const (
	// GTE special values
	GTEEmpty  = Entry(0) // Sparse: no data allocated
	GTEZeroed = Entry(1) // Zeroed grain (only when FlagUseZeroedGrainTableEntry is set)
)

const (
	SPARSE = "SPARSE"
	// FLAT
	// ZERO
	// VMFS
	// VMFSSPARSE
	// VMFSRDM
	// VMFSRAW
)

const (
	StreamOptimized  = "streamOptimized"
	MonolithicSparse = "monolithicSparse"
	// Custom
	// MonolithicFlat
	// TwoGbMaxExtentSparse
	// TwoGbMaxExtentFlat
	// FullDevice
	// PartitionedDevice
	// VmfsPreallocated
	// VmfsEagerZeroedThick
	// VmfsThin
	// VmfsSparse
	// VmfsRDM
	// VmfsRDMP
	// VmfsRaw
)
