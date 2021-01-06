# go-vmdk-parser

VMDK parser for Virtual Machine Image.

## Quick Start 
```
func main() {
	f, err := os.Open("./generic-alpine38-virtualbox-disk001.vmdk")
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
            buf := make([]byte, 1024)
            for {
                _, err := reader.Read(buf)
                if err == io.EOF {
                    break
                }
            }
        }
	}
}
```