package main

import (
	"log"
	"os"

	disk "github.com/masahiro331/go-disk"
	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

func main() {
	v, err := vmdk.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	driver, err := disk.NewDriver(v)
	if err != nil {
		log.Fatal(err)
	}

	for {
		p, err := driver.Next()
		if err != nil {
			log.Fatal(err)
		}
		if p.Bootable() {
			continue
		}

		reader := p.GetSectionReader()
		f, err := os.Create(p.Name())
		if err != nil {
			log.Fatal(err)
		}

		for {
			buf := make([]byte, 512)
			_, err := reader.Read(buf)
			if err != nil {
				log.Fatal(err)
			}
			f.Write(buf)
		}
	}
}