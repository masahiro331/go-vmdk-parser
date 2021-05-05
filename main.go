package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("invalid arguments")
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	reader, err := vmdk.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	for {
		partition, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		if partition.Bootable() != true {
			f, err := os.Create(partition.Name() + ".img")
			if err != nil {
				log.Fatal(err)
			}

			i, err := io.Copy(f, reader)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("file size:", i)
		}
	}
}
