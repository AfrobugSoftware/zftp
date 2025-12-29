package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"zftp/protocol"
)

func DoGet(remotefile, localfile, hostname, port string) error {
	lfile, err := os.Create(localfile)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%s", hostname, port))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}
	//state is in charge of closing the connection and the file
	s := &protocol.State{}
	s.Conn = conn
	s.CurFile = lfile
	s.Rtt = protocol.NewRtt()
	s.SType = protocol.CLIENT
	start := time.Now()
	err = s.SendRQ(protocol.OP_RRQ, remotefile)
	if err != nil {
		return err
	}
	s.Loop(context.Background())
	elapsed := time.Since(start)
	log.Printf("Received %d bytes from %s:%s in %.1f seconds\n", s.TotalBytes, hostname, port, elapsed.Seconds())
	return nil
}

func DoPut(remotefile, localfile, hostname, port string) error {
	lfile, err := os.Open(localfile)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%s", hostname, port))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}
	//state is in charge of closing the connection and the file
	s := &protocol.State{}
	s.Conn = conn
	s.CurFile = lfile
	s.Rtt = protocol.NewRtt()
	s.SType = protocol.CLIENT
	start := time.Now()
	err = s.SendRQ(protocol.OP_WRQ, remotefile)
	if err != nil {
		return err
	}
	s.Loop(context.Background())
	elapsed := time.Since(start)
	log.Printf("Sent %d bytes to %s:%s in %.1f seconds\n", s.TotalBytes, hostname, port, elapsed.Seconds())
	return nil
}
