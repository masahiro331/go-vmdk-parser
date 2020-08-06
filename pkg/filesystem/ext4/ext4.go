package ext4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/xerrors"
)

var (
	NotSupport64BitError = xerrors.New("Not support 64bit filesystem")
)

type Reader interface {
	io.ReadCloser
	Next() error
}

const BLOCK_SIZE = 0x400

func NewReader(r io.Reader) (Reader, error) {
	block := make([]byte, BLOCK_SIZE)

	// first block is boot sector
	_, err := r.Read(block)
	if err != nil {
		return nil, err
	}

	// only ext4 support
	return NewExt4Reader(r)
}

type Ext4Reader struct {
	r io.Reader

	buffer *bytes.Buffer
	sb     Superblock
	gds    []GroupDescriptor
}

/*
Ext4 Block Layout
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
| Group 0 Padding | ext4 Super Block | Group Descriptors | Reserved GDT Blocks | Data Block Bitmap | inode Bitmap | inode Table	| Data Blocks      |
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
| 1024 bytes      | 1 block	     | many blocks       | many blocks         | 1 block           | 1 block      | many blocks | many more blocks |
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
*/

func NewExt4Reader(r io.Reader) (Reader, error) {
	// ext4 Super Block
	var sb Superblock
	if err := binary.Read(r, binary.LittleEndian, &sb); err != nil {
		return nil, err
	}
	if sb.Magic != 0xEF53 {
		return nil, xerrors.New("unsupported block")
	}

	// Read padding block
	if sb.GetBlockSize() != BLOCK_SIZE {
		_, err := r.Read(make([]byte, sb.GetBlockSize()-BLOCK_SIZE))
		if err != nil {
			return nil, err
		}
	}

	numBlockGroups := (sb.GetBlockCount() + int64(sb.BlockPer_group) - 1) / int64(sb.BlockPer_group)
	numBlockGroups2 := (sb.InodeCount + sb.InodePer_group - 1) / sb.InodePer_group
	if numBlockGroups != int64(numBlockGroups2) {
		return nil, fmt.Errorf("Block/inode mismatch: %d %d %d", sb.GetBlockCount(), numBlockGroups, numBlockGroups2)
	}

	rawbuffer := bytes.NewBuffer([]byte{})
	buf := make([]byte, BLOCK_SIZE)
	for i := uint32(0); i < sb.GetGroupDescriptorCount(); i++ {
		_, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		rawbuffer.Write(buf)
	}

	// Group Descriptors
	var gds []GroupDescriptor
	for i := uint32(0); i < sb.GetGroupDescriptorTableCount(); i++ {
		var size uint32
		if sb.FeatureIncompat64bit() {
			size = 64
		} else {
			size = 32
		}
		tmpbuf := make([]byte, size)
		_, err := rawbuffer.Read(tmpbuf)
		if err != nil {
			return nil, err
		}
		if len(tmpbuf) == 32 {
			tmpbuf = append(tmpbuf, make([]byte, 32)...)
		}

		var gd GroupDescriptor
		binary.Read(bytes.NewReader(tmpbuf), binary.LittleEndian, &gd)
		fmt.Println(gd)
		gds = append(gds, gd)
	}

	// TODO: 64bit filesystem is not support
	if sb.Desc_size != 0 {
		return nil, NotSupport64BitError
	}

	return &Ext4Reader{
		r:      r,
		buffer: bytes.NewBuffer([]byte{}),
		sb:     sb,
		gds:    gds,
	}, nil
}

// DOC Group Descriptors
/*
オフセット	サイズ	名前			説明
0x0		__le32	bg_block_bitmap_lo	ブロックビットマップの位置の下位32ビット。
0x4		__le32	bg_inode_bitmap_lo	iノードビットマップの位置の下位32ビット。
0x8		__le32	bg_inode_table_lo	iノードテーブルの位置の下位32ビット。
0xC		__le16	bg_free_blocks_count_lo	下位16ビットのフリーブロック数。
0xE		__le16	bg_free_inodes_count_lo	空きiノード数の下位16ビット。
0x10		__le16	bg_used_dirs_count_lo	ディレクトリカウントの下位16ビット。
0x12		__le16	bg_flags		ブロックグループフラグ。のいずれか：
						0x1	iノードテーブルとビットマップは初期化されていません（EXT4_BG_INODE_UNINIT）。
						0x2	ブロックビットマップは初期化されていません（EXT4_BG_BLOCK_UNINIT）。
						0x4	inodeテーブルはゼロ化されます（EXT4_BG_INODE_ZEROED）。
0x14		__le32	bg_exclude_bitmap_lo	スナップショット除外ビットマップの位置の下位32ビット。
0x18		__le16	bg_block_bitmap_csum_lo	ブロックビットマップチェックサムの下位16ビット。
0x1A		__le16	bg_inode_bitmap_csum_lo	iノードビットマップチェックサムの下位16ビット。
0x1C		__le16	bg_itable_unused_lo	未使用のiノード数の下位16ビット。設定されている場合(sb.s_inodes_per_group - gdt.bg_itable_unused)、このグループのiノードテーブルのth番目のエントリをスキャンする必要はありません。
0x1E		__le16	bg_checksum		グループ記述子のチェックサム。RO_COMPAT_GDT_CSUM機能が設定されている場合はcrc16（sb_uuid + group + desc）、RO_COMPAT_METADATA_CSUM機能が設定されている場合はcrc32c（sb_uuid + group_desc）＆0xFFFF。
これらの	フィールドは、64ビット機能が有効でs_desc_size> 32の場合にのみ存在します。
0x20		__le32	bg_block_bitmap_hi	ブロックビットマップの位置の上位32ビット。
0x24		__le32	bg_inode_bitmap_hi	iノードのビットマップの位置の上位32ビット。
0x28		__le32	bg_inode_table_hi	iノードテーブルの位置の上位32ビット。
0x2C		__le16	bg_free_blocks_count_hi	空きブロックカウントの上位16ビット。
0x2E		__le16	bg_free_inodes_count_hi	空きiノード数の上位16ビット。
0x30		__le16	bg_used_dirs_count_hi	ディレクトリカウントの上位16ビット。
0x32		__le16	bg_itable_unused_hi	未使用のiノード数の上位16ビット。
0x34		__le32	bg_exclude_bitmap_hi	スナップショット除外ビットマップの場所の上位32ビット。
0x38		__le16	bg_block_bitmap_csum_hi	ブロックビットマップチェックサムの上位16ビット。
0x3A		__le16	bg_inode_bitmap_csum_hi	iノードビットマップチェックサムの上位16ビット。
0x3C		__u32	bg_reserved		64バイトにパディングします。
*/
// sample
/*
{
0x0  259    ブロックビットマップの位置の下位32ビット。
0x4  272    iノードビットマップの位置の下位32ビット。
0x8  285    iノードテーブルの位置の下位32ビット。
0xC  4683   下位16ビットのフリーブロック数。
0xE  1952   空きiノード数の下位16ビット。
0x10 2      ディレクトリカウントの下位16ビット。
0x12 4      ブロックグループフラグ。のいずれか：
            0x1	iノードテーブルとビットマップは初期化されていません（EXT4_BG_INODE_UNINIT）。
            0x2	ブロックビットマップは初期化されていません（EXT4_BG_BLOCK_UNINIT）。
            0x4	inodeテーブルはゼロ化されます（EXT4_BG_INODE_ZEROED）。
0x14 0      スナップショット除外ビットマップの位置の下位32ビット。
0x18 64232  ブロックビットマップチェックサムの下位16ビット。
0x1A 20514  iノードビットマップチェックサムの下位16ビット。
0x1C 1951   未使用のiノード数の下位16ビット。設定されている場合(sb.s_inodes_per_group - gdt.bg_itable_unused)、このグループのiノードテーブルのth番目のエントリをスキャンする必要はありません。
0x1E 61735  グループ記述子のチェックサム。RO_COMPAT_GDT_CSUM機能が設定されている場合はcrc16（sb_uuid + group + desc）、RO_COMPAT_METADATA_CSUM機能が設定されている場合はcrc32c（sb_uuid + group_desc）＆0xFFFF。
0x20 0
0x24 0
0x28 0
0x2C 0
0x2E 0
0x30 0
0x32 0
0x34 0
0x38 0
0x3A 0
0x3C 0
}
{260 273 532 3521 1976 0 5 0 59990 0 1976 4786 0 0 0 0 0 0 0 0 0 0 0}
{261 274 779 1435 1976 0 5 0 16114 0 1976 49814 0 0 0 0 0 0 0 0 0 0 0}
{262 275 1026 7751 1976 0 5 0 59623 0 1976 53664 0 0 0 0 0 0 0 0 0 0 0}
{263 276 1273 8192 1976 0 5 0 23897 0 1976 32384 0 0 0 0 0 0 0 0 0 0 0}
{264 277 1520 7934 1976 0 5 0 38899 0 1976 48803 0 0 0 0 0 0 0 0 0 0 0}
{265 278 1767 4096 1976 0 5 0 12694 0 1976 33546 0 0 0 0 0 0 0 0 0 0 0}
{266 279 2014 7934 1976 0 5 0 38899 0 1976 15313 0 0 0 0 0 0 0 0 0 0 0}
{267 280 2261 8192 1976 0 5 0 23897 0 1976 12569 0 0 0 0 0 0 0 0 0 0 0}
{268 281 2508 7934 1976 0 5 0 38899 0 1976 1512 0 0 0 0 0 0 0 0 0 0 0}
{269 282 2755 8192 1976 0 5 0 23897 0 1976 13438 0 0 0 0 0 0 0 0 0 0 0}
{270 283 3002 8192 1976 0 5 0 23897 0 1976 30762 0 0 0 0 0 0 0 0 0 0 0}
{271 284 3249 4095 1976 0 5 0 58098 0 1976 33883 0 0 0 0 0 0 0 0 0 0 0}
*/

func (ext4 *Ext4Reader) Read(p []byte) (int, error) {
	return 0, nil
}

func (ext4 *Ext4Reader) Next() error {

	return nil
}

func (ext4 *Ext4Reader) Close() error {
	return nil
}
