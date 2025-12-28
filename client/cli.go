package client

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"zftp/network"
)

type CommandFunc func(args []string) error

var (
	Commands map[string]CommandFunc = map[string]CommandFunc{
		"help":    help,
		"get":     get,
		"put":     put,
		"connect": connect,
		"exit":    exit,
	}
)

var (
	ErrNoToken          = errors.New("no token")
	ErrNoCommand        = errors.New("no such command")
	ErrConnectArgument  = errors.New("needs 2 arguments to connect a hostname and a port")
	ErrGetArgument      = errors.New("needs to arguments to get file, a remote name and a localname")
	ErrInvalidLocalFile = errors.New("Invalid local file")
	ErrExit             = errors.New("exit")
)

var (
	HostName  string
	Port      string
	Connected bool
)

func exit(_ []string) error {
	return ErrExit
}

func GetLine(in *os.File) (string, error) {
	reader := bufio.NewReader(in)
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	input = strings.TrimSpace(input)
	return input, err
}

func getToken(stream string) ([]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(stream))
	scanner.Split(bufio.ScanWords)
	tokens := make([]string, 0)
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return tokens, nil
}

func DoCommand(c string) error {
	strs, err := getToken(c)
	if err != nil {
		return err
	}
	cmd, ok := Commands[strs[0]]
	if !ok {
		return ErrNoCommand
	}
	return cmd(strs[1:])
}

func connect(str []string) error {
	if len(str) != 2 {
		return ErrConnectArgument
	}
	HostName = str[0]
	Port = str[1]
	return nil
}

func get(str []string) error {
	if len(str) == 2 {
		return ErrGetArgument
	}
	if strings.Contains(str[0], ":") {
		s := strings.Split(str[0], ":")
		HostName = s[0]
		str[0] = s[1]
	}
	if strings.Contains(str[1], ":") {
		return ErrInvalidLocalFile
	}
	err := network.DoGet(str[0], str[1], HostName, Port)
	if err != nil {
		return err
	}
	return nil
}

func help(_ []string) error {
	var helpString strings.Builder
	fmt.Fprint(&helpString, "Usage:\n")
	fmt.Fprint(&helpString, "get <remote filename> <local file name> ; reads remote file into the local file path\n")
	fmt.Fprint(&helpString, "put <remote filename> <local file name> ; reads remote file into the local file path\n")
	fmt.Fprint(&helpString, "connect <hostname> <port>; connects to the host \n")
	fmt.Fprint(&helpString, "mode <modetype> ; it should be either text or binary \n")
	fmt.Fprint(&helpString, "exit ; closes the applicaiton\n")

	fmt.Println(helpString)
	return nil
}

func put(str []string) error {
	if len(str) == 2 {
		return ErrGetArgument
	}
	if strings.Contains(str[0], ":") {
		s := strings.Split(str[0], ":")
		HostName = s[0]
		str[0] = s[1]
	}
	if strings.Contains(str[1], ":") {
		return ErrInvalidLocalFile
	}
	err := network.DoPut(str[0], str[1], HostName, Port)
	if err != nil {
		return err
	}
	return nil
}
