package main

import (
	"bytes"
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

	reader, err := vmdk.NewReader(file, []byte{})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		p, err := reader.Next()
		if err != nil {
			log.Fatal(err)
		}

		f, _ := os.Create(fmt.Sprintf("%d.img", i))
		defer f.Close()
		fmt.Println(p)

		for {
			b := make([]byte, 65536)
			_, err := reader.Read(b)
			if err != nil {
				break
			}

			io.Copy(f, bytes.NewReader(b))
		}
	}
}
