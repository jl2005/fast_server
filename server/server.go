package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/exp/mmap"
)

var num *int

var total uint64

func main() {
	name := flag.String("name", "data", "data file name")
	addr := flag.String("addr", ":8888", "listen address")
	num = flag.Int("num", 8, "pasre thread num")
	flag.Parse()

	start := time.Now()
	data, err := readFile(*name)
	if err != nil {
		log.Printf("read file '%s' error. %s", err)
		return
	}
	end := time.Now()
	log.Printf("load file used: %v", end.Sub(start))

	list := convert(data)
	log.Printf("convert data used: %v", time.Now().Sub(end))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Printf("listen failed. %s", err)
		return
	}
	var ch chan struct{}
	defer close(ch)
	go stat(ch)
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

func stat(ch chan struct{}) {
	var speed uint64
	for {
		select {
		case <-time.Tick(time.Second):
			speed = 0
			speed = atomic.SwapUint64(&total, speed)
			log.Printf("speed %d", speed)
		case <-ch:
			log.Printf("total lines %d", total)
			return
		}
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
	NUM := *num
	chs := make([]chan [][]byte, NUM)
	for i := 0; i < NUM-1; i++ {
		chs[i] = make(chan [][]byte, 1)
		go parse(data, i*(len(data)/NUM), (i+1)*(len(data)/NUM), chs[i])
	}
	chs[NUM-1] = make(chan [][]byte, 1)
	go parse(data, (NUM-1)*(len(data)/NUM), len(data), chs[NUM-1])
	var list [][]byte
	for i := 0; i < NUM; i++ {
		select {
		case l := <-chs[i]:
			list = append(list, l...)
		}
	}
	return list
}

func parse(data []byte, start int, end int, ch chan [][]byte) {
	for start != 0 && data[start] != byte('\n') {
		start++
	}
	if data[start] == '\n' {
		start++
	}
	i := start
	var list [][]byte
	for start < end {
		for start < len(data) && data[start] != byte('\n') {
			start++
		}
		list = append(list, deleteAndReverse(data[i:start]))
		start++
		i = start
	}
	ch <- list
}

func deleteAndReverse(data []byte) []byte {
	l := len(data) / 3
	if l > 0 {
		copy(data[l:], data[l+l:])
		data = data[:len(data)-l]
	}
	i := 0
	j := len(data) - 1
	for i < j {
		data[i], data[j] = data[j], data[i]
		i++
		j--
	}
	return data
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
		atomic.AddUint64(&total, 1)
	}
}
