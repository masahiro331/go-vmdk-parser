package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/gexto"
	"github.com/masahiro331/go-vmdk-parser/pkg/analyzer"
	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

const BUFFER_SIZE = 512 * 128

func main() {
	if len(os.Args) != 2 {
		log.Fatal("invalid arguments")
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	reader, err := vmdk.NewReader(f, []byte{})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		_, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		fileName := fmt.Sprintf("%d.img", i)
		if !Exists(fileName) {
			f, _ := os.Create(fileName)
			defer f.Close()

			for {
				b := make([]byte, BUFFER_SIZE)
				_, err := reader.Read(b)
				if err != nil {
					break
				}
				io.Copy(f, bytes.NewReader(b))
			}
		}
	}

	fs, err := gexto.NewFileSystem("./1.img")
	if err != nil {
		log.Fatal(err)
	}

	_, err = analyzer.AnalyzeFileSystem(fs)
	if err != nil {
		log.Fatal(err)
	}
}

func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}
