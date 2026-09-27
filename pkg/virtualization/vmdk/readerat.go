package vmdk

import (
	"io"
	"sync"

	"golang.org/x/xerrors"
)

// seekReaderAt adapts an io.ReadSeeker that does not implement io.ReaderAt.
// Seek+Read pairs are serialized by a mutex, so it is safe for concurrent use
// but does not read in parallel.
type seekReaderAt struct {
	mu sync.Mutex
	rs io.ReadSeeker
}

func (r *seekReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.rs.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := io.ReadFull(r.rs, p)
	if err == io.ErrUnexpectedEOF {
		// Match the io.ReaderAt convention of *os.File for short reads.
		err = io.EOF
	}
	return n, err
}

// readFullAt reads exactly size bytes at off.
func readFullAt(ra io.ReaderAt, off, size int64) ([]byte, error) {
	buf := make([]byte, size)
	n, err := ra.ReadAt(buf, off)
	if int64(n) == size {
		// io.ReaderAt may return io.EOF along with a full read at the end of input.
		return buf, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, xerrors.Errorf(ErrReadSizeFormat, n, size)
}
