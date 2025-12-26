package client

import (
	"fmt"
	"net"
	"os"
)

func DoGet(remotefile, localfile string) error {
	lfile, err := os.Open(localfile)
	if err != nil {
		return err
	}
	conn, err := net.Dial("udp4", fmt.Sprintf("%s:%s", HostName, Port))
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}
