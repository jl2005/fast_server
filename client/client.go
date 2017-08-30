package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
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
	flag.Parse()

	start := time.Now()
	defer func() {
		log.Printf("use time %v", time.Now().Sub(start))
	}()
	chs := make([]chan []byte, *n)
	const NUM = 10
	for i := 0; i < *n; i++ {
		chs[i] = make(chan []byte, NUM)
		go worker(*addr, uint32(i), uint32(*n), uint32(*lines), chs[i])
	}

	var buf []byte
	var ok bool
	for {
		for i := 0; i < *n; i++ {
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
	var i, index uint32
	for i = 0; err == nil; i++ {
		n, m := 0, 0
		buf := make([]byte, 256*lines)
		start := id*lines + i*nn*lines
		end := start + lines
		for index = start; index < end; index++ {
			if err = binary.Write(conn, binary.LittleEndian, index); err != nil {
				log.Printf("%d write error. %s", id, err)
				//TODO 如果失败则需要重试
				return
			}
			data := strconv.AppendUint(buf[n:n], uint64(index+1), 10)
			n += len(data)
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
}

func write(buf []byte) {
	//FIXME write to pagecache
	//fmt.Printf("%s", string(buf))
}
