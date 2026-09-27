package vmdk

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
)

// oneByteReadSeeker returns at most one byte per Read to exercise short reads.
type oneByteReadSeeker struct {
	r *bytes.Reader
}

func (o *oneByteReadSeeker) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}

func (o *oneByteReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return o.r.Seek(offset, whence)
}

func TestSeekReaderAt(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251)
	}

	tests := []struct {
		name    string
		off     int64
		size    int
		wantN   int
		wantErr error
	}{
		{name: "full read", off: 100, size: 1000, wantN: 1000},
		{name: "read to the end", off: 4000, size: 96, wantN: 96},
		{name: "partial read at the end", off: 4000, size: 200, wantN: 96, wantErr: io.EOF},
		{name: "read beyond the end", off: 5000, size: 10, wantN: 0, wantErr: io.EOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ra := &seekReaderAt{rs: &oneByteReadSeeker{r: bytes.NewReader(data)}}
			buf := make([]byte, tt.size)
			n, err := ra.ReadAt(buf, tt.off)
			if err != tt.wantErr {
				t.Fatalf("ReadAt() err = %v, want %v", err, tt.wantErr)
			}
			if n != tt.wantN {
				t.Fatalf("ReadAt() n = %d, want %d", n, tt.wantN)
			}
			if n > 0 && !bytes.Equal(buf[:n], data[tt.off:tt.off+int64(n)]) {
				t.Fatal("ReadAt() content mismatch")
			}
		})
	}
}

func TestSeekReaderAtConcurrent(t *testing.T) {
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	ra := &seekReaderAt{rs: &oneByteReadSeeker{r: bytes.NewReader(data)}}

	const workers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 16; i++ {
				off := int64((w*16 + i) * 512)
				buf := make([]byte, 512)
				if n, err := ra.ReadAt(buf, off); err != nil || n != len(buf) {
					errs <- fmt.Sprintf("off=%d: n=%d, err=%v", off, n, err)
					return
				}
				if !bytes.Equal(buf, data[off:off+512]) {
					errs <- "content mismatch"
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("ReadAt(): %s", e)
	}
}
