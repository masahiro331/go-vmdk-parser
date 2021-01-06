package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	ext4 "github.com/masahiro331/go-ext4-filesystem/pkg"
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

	for i := 0; i < 4; i++ {
		partition, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		if partition.Boot != true {
			ext4Reader, err := ext4.NewReader(reader)
			if err != nil {
				log.Fatal(err)
			}
			for {
				filename, err := ext4Reader.Next()
				if err != nil {
					log.Fatalf("%+v", err)
				}
				if filename == "installed" {
					buf, _ := ioutil.ReadAll(ext4Reader)
					fmt.Println(string(buf))
					return
				}
			}
		}
	}
}
