package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"zftp/client"
	"zftp/network"
)

const (
	prompt = "zftp> "
)

func main() {
	fmt.Println("Starting zftp")
	if len(os.Args) < 2 {
		log.Println("sp")
	}
	ftpType := strings.TrimSpace(strings.ToLower(os.Args[1]))
	sv := flag.NewFlagSet("server", flag.ExitOnError)
	host := sv.String("host", "localhost", "specifiy the server host")
	port := sv.String("port", "8000", "specify the server host")

	switch ftpType {
	case "client":
		for {
			log.Print(prompt)
			input, err := client.GetLine(os.Stdin)
			if err != nil {
				log.Println(err)
				return
			}
			err = client.DoCommand(input)
			if err != nil {
				log.Println(err)
				return
			}
		}
	case "server":
		log.Println("Staring server")
		subArg := os.Args[2:]
		err := sv.Parse(subArg)
		if err != nil {
			fmt.Println(err)
			return
		}
		network.StartServer(*host, *port)
	}
}
