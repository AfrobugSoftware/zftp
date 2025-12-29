package protocol

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
)

const (
	BlockDataSize = 1024
	MaxFileSize   = 2 ^ 32
)

// for gobing
type OpCode struct {
	Code int16
}

// for WRQ and RRQ
type Request struct {
	Filename string
}

type Block struct {
	BlockNumber int16
	CheckSum    [32]byte
	Data        []byte
}

type Ack struct {
	BlockNumber int16
}

type Error struct {
	ErrString string
}

var (
	ErrNotDefined         = errors.New("not defined")
	ErrFileNotFound       = errors.New("file not found")
	ErrAccessViolation    = errors.New("Access violation")
	ErrDiskFull           = errors.New("disk full")
	ErrIllegalOperation   = errors.New("illegal operation")
	ErrUnknownPortNumber  = errors.New("unknown port number")
	ErrFileAlreadyExists  = errors.New("file already exists")
	ErrInvalidFileName    = errors.New("Invalid file name")
	ErrNoSuchUser         = errors.New("no such user")
	ErrNoFile             = errors.New("no such file")
	ErrBlockSize          = errors.New("block size excceded")
	ErrInvalidBlockNumner = errors.New("Invalid block number received")
	ErrInvalidCheckSum    = errors.New("Invalid checksum")
	ErrInvalidOpCode      = errors.New("invalid opcode")
	Complete              = errors.New("transfer complete")
)

var (
	BufferPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
)

func GetOpcode(buff []byte) (int16, error) {
	dec := gob.NewDecoder(bytes.NewReader(buff))
	var code OpCode
	if err := dec.Decode(&code); err != nil {
		return -1, err
	}
	return code.Code, nil
}

func GobEncode(data any, code int16) ([]byte, error) {
	buff := BufferPool.Get().(*bytes.Buffer)
	defer func() {
		buff.Reset()
		BufferPool.Put(buff)
	}()
	opcode := OpCode{code}
	enc := gob.NewEncoder(buff)
	err := enc.Encode(opcode)
	if err != nil {
		return nil, err
	}
	err = enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func DecodeGob[T any](data *bytes.Buffer, v *T) (int16, error) {
	dec := gob.NewDecoder(data)
	var code OpCode
	if err := dec.Decode(&code); err != nil {
		return -1, err
	}
	if err := dec.Decode(v); err != nil {
		return -1, err
	}
	return code.Code, nil
}
