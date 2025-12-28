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

// for WRQ and RRQ
type Request struct {
	Opcode   uint16
	Filename string
}

type Block struct {
	Opcode      uint16
	BlockNumber int16
	CheckSum    [32]byte
	Data        []byte
}

type Ack struct {
	Opcode      uint16
	BlockNumber int16
}

type Error struct {
	Opcode    uint16
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
	Complete              = errors.New("transfer complete")
)

var (
	BufferPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
)

func GobEncode(data any) ([]byte, error) {
	buff := BufferPool.Get().(*bytes.Buffer)
	defer func() {
		buff.Reset()
		BufferPool.Put(buff)
	}()
	enc := gob.NewEncoder(buff)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}
