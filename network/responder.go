package network

import (
	"fmt"
	"net"
	"zftp/protocol"
)

const (
	MAXBUFFERSIZE = 2048
)

func StartResponder(host, port string) error {
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
	defer conn.Close()
	for {
		var buffer [MAXBUFFERSIZE]byte
		_, address, err := conn.ReadFromUDP(buffer[:])
		if err != nil {
			return nil
		}
		go HandleResponderConnection(address, buffer)
	}
}

func HandleResponderConnection(address *net.UDPAddr, buff [MAXBUFFERSIZE]byte) {
	conn, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		return
	}
	s := &protocol.State{}

	defer conn.Close()

}
