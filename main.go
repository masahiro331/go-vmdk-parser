package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

const BUFFER_SIZE = 512 * 128

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
		fmt.Println(p)

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

}

func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}
