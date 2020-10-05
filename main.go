package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aquasecurity/trivy-db/pkg/db"
	l "github.com/aquasecurity/trivy/pkg/log"
	"github.com/aquasecurity/trivy/pkg/types"
	"github.com/aquasecurity/trivy/pkg/utils"
	"github.com/masahiro331/gexto"
	"github.com/masahiro331/go-vmdk-parser/pkg/analyzer"
	"github.com/masahiro331/go-vmdk-parser/pkg/detector"
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

	ar, err := analyzer.AnalyzeFileSystem(fs)
	if err != nil {
		log.Fatal(err)
	}

	dir := utils.DefaultCacheDir()
	if err := db.Init(dir); err != nil {
		log.Fatalf("%+v\n", err)
	}

	if err = l.InitLogger(true, false); err != nil {
		log.Fatal(err)
	}

	var dvs []types.DetectedVulnerability
	for _, pkgInfo := range ar.PackageInfos {
		vulns, _, _ := detector.DetectOSVulnerability("generic/alpine", ar.OS.Family, ar.OS.Name, nil, pkgInfo.Packages)
		dvs = append(dvs, vulns...)
	}

	fmt.Printf("%+v\n", dvs)
}

func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}
