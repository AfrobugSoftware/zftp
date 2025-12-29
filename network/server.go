package network

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"zftp/protocol"
)

const (
	MAXBUFFERSIZE = 2048
)

func StartServer(host, port string) error {
	// Resolve the string address to a UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return err
	}
	// Start listening for UDP packages on the given address, this address should only be opened to RRQ and WRQ packages
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	log.Printf("Listening on: %s:%s", host, port)
	ctx, cancel := context.WithCancel(context.Background())
	defer conn.Close()
	defer cancel()
	for {
		var buffer [MAXBUFFERSIZE]byte
		_, address, err := conn.ReadFromUDP(buffer[:])
		if err != nil {
			return nil
		}
		go HandleResponderConnection(ctx, address, buffer)
	}
}

func HandleResponderConnection(ctx context.Context, address *net.UDPAddr, buff [MAXBUFFERSIZE]byte) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Recieved packet from: %s", address.String())
	s := &protocol.State{}
	s.Conn = conn
	s.RecvBuf = bytes.NewBuffer(buff[:])
	s.Rtt = protocol.NewRtt()
	s.SType = protocol.SERVER
	s.SendAddr = address
	s.Loop(ctx)
}
