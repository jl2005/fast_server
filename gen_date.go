package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
)

func main() {
	name := flag.String("name", "data", "data file name")
	size := flag.Int("size", 1024*1024, "data file size. 1M=1048576 1G=1073741824")
	maxNum := flag.Int("max", 200, "max length of line")
	flag.Parse()

	file, err := os.OpenFile(*name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("open file '%s' error. %s\n", *name, err)
		return
	}
	defer file.Close()

	data := make([]byte, *maxNum)
	for *size > 0 {
		n := rand.Intn(*maxNum)
		if n == 0 {
			continue
		}
		if n > *size {
			n = *size
		}
		genData(data, n)
		file.Write(data[:n])
		*size -= n
	}
	fmt.Printf("generate file finshed.\n")
}

func genData(data []byte, n int) {
	for i := 0; i < n-1; i++ {
		item := byte(rand.Intn(127-32) + 32)
		data[i] = item
	}
	data[n-1] = byte('\n')
}
