package main 

import (
    "log"
	"flag"
	"net"
	"encoding/binary"

	"golang.org/x/exp/mmap"
)

func main() {
	name := flag.String("name", "data", "data file name")
	addr := flag.String("addr", ":8888", "listen address")
	flag.Parse()

	data, err := readFile(*name)
	if err != nil {
		log.Printf("read file '%s' error. %s", err)
		return 
	}
	list := convert(data)
	lis, err := net.Listen("tcp", *addr)
	if err !=nil {
		log.Printf("listen failed. %s", err)
		return
	}
	log.Printf("start listen %s", *addr)
	for {
		conn, err := lis.Accept() 
		if err != nil {
			log.Printf("accept failed. %s", err)
			return
		}
		go handle(conn, list)
	}
}

func readFile(name string) ([]byte, error) {
	reader, err := mmap.Open(name)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data := make([]byte, reader.Len())
	if _, err = reader.ReadAt(data, 0); err != nil {
		return nil, err
	}
	return data, nil
}

func convert(data []byte) [][]byte {
	start := 0
	var list [][]byte
	for i := range data {
		if data[i] == '\n' {
			//TODO delete 1/3
			//TODO reverse
			list = append(list, data[start:i])
			start = i+1
		}
	}
	return list
}

func handle(conn net.Conn, list [][]byte) {
	defer conn.Close()
	var err error
	var index uint32
	for {
		if err = binary.Read(conn, binary.LittleEndian, &index); err != nil {
			log.Printf("read failed. %s", err)
			return
		}
		if int(index) >= len(list) {
			return
		}
		if _, err = conn.Write(list[index]); err != nil {
			log.Printf("write failed. %s", err)
			return
		}
	}
}
