package main

import (
	"flag"
	"encoding/binary"
    "net"
    "log"
	"fmt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Printf("connect %s error. %s", *addr, err)
		return
	}
	defer conn.Close()
	const MAXSIZE = 300
	data := make([]byte, MAXSIZE)
	var n int
	var i uint32
	for i=0; ; i++{
		if err = binary.Write(conn, binary.LittleEndian, i); err != nil {
			log.Printf("write error. %s", err)
			break
		}
		if n, err = conn.Read(data); err != nil {
			log.Printf("read error. %s", err)
			break
		}
		fmt.Printf("%s\n", data[:n])
	}
}
