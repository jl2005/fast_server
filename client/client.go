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
	"os"
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
	outFile := flag.String("o", "output.data", "output file name")
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
	go receiver(chs, done, *outFile)
	const NUM = 10
	for i := 0; i < *n; i++ {
		chs[i] = make(chan []byte, NUM)
		go worker(*addr, uint32(i), uint32(*n), uint32(*lines), chs[i])
	}
	<-done
	log.Printf("use time %v", time.Now().Sub(start))
}

func receiver(chs []chan []byte, done chan struct{}, outFileName string) {
	defer close(done)

	file, err := os.OpenFile(outFileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("open output file errror. %s\n", err)
		return

	}
	defer file.Close()

	bufFile := bufio.NewWriter(file)
	//	defer bufFile.Flush()

	var buf []byte
	var ok bool
	n := len(chs)
	for {
		for i := 0; i < n; i++ {
			select {
			case buf, ok = <-chs[i]:
				if !ok {
					bufFile.Flush()
					time.Sleep(5 * time.Second)
					return
				}
				write(bufFile, buf)
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
				log.Printf("%d read length error. %s start=%d, end=%d, index=%d", id, err, start, end, index)
				if n > 0 {
					ch <- buf[:n]
				}
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
				log.Printf("%d read data error. %s start=%d, end=%d, index=%d", id, err, start, end, index)
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

func write(file io.Writer, buf []byte) {
	file.Write(buf)
}
