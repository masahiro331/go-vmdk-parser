# go-vmdk-parser

VMDK parser for Virtual Machine Image.

## Quick Start 
```
func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}
	v, err := vmdk.Open(f)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	fmt.Println(v.Size())
}
```