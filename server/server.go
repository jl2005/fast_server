package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/exp/mmap"
)

var num *int

var total uint64

type Request struct {
	Index uint32
	Addr  *net.UDPAddr
}

func main() {
	name := flag.String("f", "data", "data file name")
	addr := flag.String("addr", ":8888", "listen address")
	wn := flag.Int("wn", 4, "worker num")
	num = flag.Int("num", 8, "pasre thread num")
	profAddr := flag.String("paddr", "", "prof listen address")
	flag.Parse()

	if len(*profAddr) > 0 {
		log.Printf("start pprof listen in %s", *profAddr)
		go func() {
			log.Println(http.ListenAndServe(*profAddr, nil))
		}()
	}

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

	address, err := net.ResolveUDPAddr("udp", *addr)
	if err != nil {
		fmt.Println("Can't resolve address: ", err)
		os.Exit(1)

	}
	conn, err := net.ListenUDP("udp", address)
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer conn.Close()

	ch := make(chan *Request)
	done := make(chan struct{})
	go startUdpServer(conn, len(list), ch)
	go worker(conn, list, ch, done)
	for i := 1; i < *wn; i++ {
		go worker(conn, list, ch, nil)
	}
	select {
	case <-done:
	}
	fmt.Printf("send finish")
}

func startUdpServer(conn *net.UDPConn, l int, ch chan *Request) {
	defer close(ch)

	var index uint32
	data := make([]byte, 4)
	for {
		_, remoteAddr, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("read failed. %s", err)
			return
		}
		index = binary.BigEndian.Uint32(data)
		if int(index) >= l {
			return
		}
		ch <- &Request{index, remoteAddr}
	}
}

func worker(conn *net.UDPConn, list [][]byte, ch chan *Request, done chan struct{}) {
	if done != nil {
		defer close(done)
	}
	for {
		select {
		case req, ok := <-ch:
			if !ok {
				return
			}
			conn.WriteToUDP(list[req.Index], req.Addr)
		}
	}
}

func statStat(ch chan struct{}) {
	var speed, t uint64
	for {
		select {
		case <-time.Tick(time.Second):
			speed = 0
			speed = atomic.SwapUint64(&total, speed)
			t += speed
			log.Printf("speed %d, total %d", speed, t)
		case <-ch:
			log.Printf("total lines %d", t)
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
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		if err = binary.Read(reader, binary.LittleEndian, &index); err != nil {
			log.Printf("read failed. %s", err)
			return
		}
		if int(index) >= len(list) {
			writer.Flush()
			return
		}
		if err = binary.Write(writer, binary.LittleEndian, uint32(len(list[index]))); err != nil {
			log.Printf("write data length error. %s", err)
			return
		}
		if _, err = writer.Write(list[index]); err != nil {
			log.Printf("write failed. %s", err)
			return
		}
		atomic.AddUint64(&total, 1)
	}
}
