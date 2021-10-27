package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

func main() {
	f, err := os.Open("path to your vmdk image")
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

		if !partition.Bootable() {
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
