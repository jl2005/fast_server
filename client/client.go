package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "server address")
	n := flag.Int("n", 1, "connect thread")
	lines := flag.Int("l", 1000, "every thread get lines")
	profAddr := flag.String("paddr", "", "prof listen address")
	flag.Parse()

	if len(*profAddr) > 0 {
		log.Printf("pprof listen addr %s", *profAddr)
		go func() {
			log.Println(http.ListenAndServe(*profAddr, nil))
		}()
	}

	start := time.Now()
	done := make(chan struct{})
	chs := make([]chan []byte, *n)
	go receiver(chs, done)
	const NUM = 10
	for i := 0; i < *n; i++ {
		chs[i] = make(chan []byte, NUM)
		go worker(*addr, uint32(i), uint32(*n), uint32(*lines), chs[i])
	}
	<-done
	log.Printf("use time %v", time.Now().Sub(start))
}

func receiver(chs []chan []byte, done chan struct{}) {
	defer close(done)
	var buf []byte
	var ok bool
	n := len(chs)
	for {
		for i := 0; i < n; i++ {
			select {
			case buf, ok = <-chs[i]:
				if !ok {
					return
				}
				write(buf)
			}
		}
	}
}

func worker(addr string, id, nn, lines uint32, ch chan []byte) {
	defer close(ch)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("%d connect %s error. %s", id, addr, err)
		return
	}
	defer conn.Close()
	tokens := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		tokens <- struct{}{}
	}
	go send(conn, tokens, id, nn, lines)
	receive(conn, tokens, id, nn, lines, ch)
}

func send(conn net.Conn, tokens chan struct{}, id, nn, lines uint32) {
	var err error
	var i, index uint32
	writer := bufio.NewWriter(conn)
	for i = 0; err == nil; i++ {
		start := id*lines + i*nn*lines
		end := start + lines
		select {
		case <-tokens:
		}
		for index = start; index < end; index++ {
			if err = binary.Write(writer, binary.LittleEndian, index); err != nil {
				log.Printf("%d write error. %s", id, err)
				//TODO 如果失败则需要重试
				return
			}
		}
	}
}

func receive(conn net.Conn, tokens chan struct{}, id, nn, lines uint32, ch chan []byte) {
	reader := bufio.NewReader(conn)
	var err error
	var i, index, l uint32
	for i = 0; err == nil; i++ {
		n, m := 0, 0
		buf := make([]byte, 256*lines)
		start := id*lines + i*nn*lines
		end := start + lines
		for index = start; index < end; index++ {
			if err = binary.Read(reader, binary.LittleEndian, &l); err != nil {
				log.Printf("%d read length error. %s", id, err)
				return
			}
			data := strconv.AppendUint(buf[n:n], uint64(index+1), 10)
			n += len(data)
			if m, err = io.ReadFull(reader, buf[n:n+int(l)]); err != nil {
				if err != io.EOF {
					n += m
					buf[n] = byte('\n')
					n++
				}
				log.Printf("%d read data error. %s", id, err)
				break
			}
			n += m
			buf[n] = byte('\n')
			n++
		}
		ch <- buf[:n]
		tokens <- struct{}{}
	}
}

/*
func worker(addr string, id, nn, lines uint32, ch chan []byte) {
	defer close(ch)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("%d connect %s error. %s", id, addr, err)
		return
	}
	defer conn.Close()
	buf := make([]byte, 1024*1024*1024)
	n, m := 0, 0
	for err == nil {
		if m, err = conn.Read(buf[n:]); err != nil {
			if err != io.EOF {
				n += m
				buf[n] = byte('\n')
				n++
			}
			log.Printf("%d read error. %s", id, err)
			break
		}
		n += m
		buf[n] = byte('\n')
		n++
	}
	ch <- buf[:n]
}
*/

func write(buf []byte) {
	//FIXME write to pagecache
	//fmt.Printf("%s", string(buf))
}
