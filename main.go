package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/go-vmdk-parser/pkg/extractor/vmdk"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("invalid arguments")
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	extractor, err := vmdk.NewExtractor(file)
	if err != nil {
		log.Fatal(err)
	}
	filemap, err := extractor.ExtractFromFile(file, []string{})
	if err != nil {
		log.Fatal(err)
	}
	for name, bytesa := range filemap {
		fmt.Printf("%s: %d bytes\n", name, len(bytesa))
		f, _ := os.Create(name)

		for _, b := range bytesa {
			zr, _ := zlib.NewReader(bytes.NewReader(b))
			io.Copy(f, zr)
		}
	}
}

// func (h *Header) CheckSignature() bool {
// 	return h.Signature == 0x564d444b
// }
