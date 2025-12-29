package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// operations
const (
	OP_INIT = iota
	OP_RRQ
	OP_WRQ
	OP_DATA
	OP_ACK
	OP_ERROR
)

const (
	OP_MIN = 1
	OP_MAX = 5
)

// state type
const (
	CLIENT = iota
	SERVER
)
const MAXBUFFERSIZE = 2048

type State struct {
	TotalBytes   int64
	CurFile      *os.File
	Rtt          *Rtt
	Conn         *net.UDPConn
	SendBuf      *bytes.Buffer
	RecvBuf      *bytes.Buffer
	NextBlockNum int
	OpSent       int
	OpRecv       int
	SType        int
}

type StateFunc func(*State) error

var (
	pattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+\.[a-zA-Z0-9]+$`)
)

// [SENT][RECV]
var FSMClientTable = [OP_MAX + 1][OP_MAX + 1]StateFunc{
	//[sent = INIT]
	{invalid,
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
	},
	//[sent = RRQ]
	{
		invalid,
		invalid,
		invalid,
		recvData,
		invalid,
		recvError,
	},
	//[sent = WRQ]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		recvAck,
		recvError,
	},
	//[sent = DATA]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		recvAck,
		recvError,
	},
	//[sent = ACK]
	{
		invalid,
		invalid,
		invalid,
		recvData,
		invalid,
		recvError,
	},
	//[sent = ERROR]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
		recvError,
	},
}
var FSMServerTable = [OP_MAX + 1][OP_MAX + 1]StateFunc{
	//[sent = INIT]
	{invalid,
		recvRQ,
		recvRQ,
		invalid,
		invalid,
		invalid,
	},
	//[sent = RRQ]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
	},
	//[sent = WRQ]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
	},
	//[sent = DATA]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		recvAck,
		recvError,
	},
	//[sent = ACK]
	{
		invalid,
		invalid,
		invalid,
		recvData,
		invalid,
		recvError,
	},
	//[sent = ERROR]
	{
		invalid,
		invalid,
		invalid,
		invalid,
		invalid,
		recvError,
	},
}

func invalid(s *State) error {
	log.Printf("protocol error: op_sent: %d, op_recv: %d\n", s.OpSent, s.OpRecv)
	return nil
}

func recvAck(s *State) error {
	var ack Ack
	_, err := DecodeGob(s.RecvBuf, &ack)
	if err != nil {
		return err
	}
	log.Println("RECEVING [ACK <-]")
	defer s.RecvBuf.Reset()
	if ack.BlockNumber == int16(s.NextBlockNum) {
		//send the next block
		var blkData [BlockDataSize]byte
		n, err := io.ReadFull(s.CurFile, blkData[:])
		//we want to still send and empty block if we get to end of file
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		err = s.SendData(s.NextBlockNum+1, blkData[:n])
		if err != nil {
			return err
		}
	} else if ack.BlockNumber < (int16(s.NextBlockNum)-1) || ack.BlockNumber > int16(s.NextBlockNum) {
		return ErrInvalidBlockNumner
	}
	return nil
}

func recvError(s *State) error {
	var payload Error
	_, err := DecodeGob(s.RecvBuf, &payload)
	if err != nil {
		return err
	}
	log.Println("RECEVING [ERROR <-]")
	defer s.RecvBuf.Reset()
	return errors.New(payload.ErrString)
}

func recvData(s *State) error {
	var payload Block
	_, err := DecodeGob(s.RecvBuf, &payload)
	if err != nil {
		return err
	}
	log.Println("RECEVING [DATA <-]")

	defer s.RecvBuf.Reset()
	dlen := len(payload.Data)
	if dlen > BlockDataSize {
		return ErrBlockSize
	}
	if s.NextBlockNum == int(payload.BlockNumber) {
		//correct block
		if dlen > 0 {
			nc := sha256.Sum256(payload.Data)
			if !bytes.Equal(nc[:], payload.CheckSum[:]) {
				return ErrInvalidCheckSum
			}
			n, err := s.CurFile.Write(payload.Data)
			if err != nil {
				return err
			}
			s.TotalBytes += int64(n)
		}
		if dlen < BlockDataSize {
			//transfer complete, close file
			s.CurFile.Close()
		}
		s.NextBlockNum++
	} else if payload.BlockNumber < (int16(s.NextBlockNum)-1) || payload.BlockNumber > int16(s.NextBlockNum) {
		return ErrInvalidBlockNumner
	}
	err = s.SendAck(int(payload.BlockNumber))
	if err != nil {
		return err
	}
	if dlen < BlockDataSize {
		return Complete
	}
	return nil
}

func recvRQ(s *State) error {
	var rrq Request
	opcode, err := DecodeGob(s.RecvBuf, &rrq)
	if err != nil {
		return err
	}
	defer s.RecvBuf.Reset()
	//verify file name
	log.Println("RECEVING [RQ <-]")
	filename := strings.TrimSpace(strings.ToLower(rrq.Filename))
	log.Printf("READING: [%s]\n", filename)
	if !pattern.MatchString(filename) {
		return ErrInvalidFileName
	}
	//use the current working directory
	//would add directory service maybe another time
	dir, err := os.Getwd()
	if err != nil {
		return ErrAccessViolation
	}
	filename = fmt.Sprintf("%s/%s", dir, filename)
	switch opcode {
	case OP_RRQ:
		err = recvRRQ(s, filename)
	case OP_WRQ:
		err = recvWQR(s, filename)
	}
	return err
}

func recvRRQ(s *State, filename string) error {
	stat, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = s.SendError(ErrNoFile)
			if err != nil {
				return err
			}
			return ErrNoFile
		}
	}
	//no read permission
	if stat.Mode()&0444 == 0 {
		err = s.SendError(ErrAccessViolation)
		if err != nil {
			return err
		}
		return (ErrAccessViolation)
	}
	file, err := os.Open(filename)
	if err != nil {
		s.SendError(err)
		return err
	}
	s.CurFile = file
	//set up an ack so we can pretend that we received it to start the data transfer
	ack := Ack{
		BlockNumber: 0,
	}
	b, err := GobEncode(ack, OP_ACK)
	s.RecvBuf = bytes.NewBuffer(b)
	err = recvAck(s)
	if err != nil {
		return nil
	}
	return nil
}
func recvWQR(s *State, filename string) error {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsExist(err) {
			err = s.SendError(err)
			if err != nil {
				return err
			}
			return ErrFileAlreadyExists
		}
	}
	file, err := os.Create(filename)
	if err != nil {
		s.SendError(err)
		return err
	}
	s.CurFile = file
	err = s.SendAck(0)
	s.NextBlockNum = 1
	return nil
}

func (state *State) Loop(ctx context.Context) {
	state.Rtt.NewPack()
	//close the file and the connection ??
	defer func() {
		state.CurFile.Close()
		state.Conn.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if state.RecvBuf == nil || state.RecvBuf.Len() == 0 {
				if err := state.Conn.SetReadDeadline(time.Now().Add(state.Rtt.Start())); err != nil {
					log.Println(err)
					runtime.Goexit()
				}
				buffer := make([]byte, MAXBUFFERSIZE)
				n, addr, err := state.Conn.ReadFrom(buffer)
				if err != nil {
					if e, ok := err.(net.Error); ok && e.Timeout() {
						err := state.Rtt.Timeout()
						if err != nil {
							log.Println(err)
							return
						}
						_, err = state.Conn.WriteTo(state.SendBuf.Bytes(), addr)
						if err != nil {
							log.Println(err)
							return
						}
					}
				}
				state.Rtt.Stop()
				if n < 4 {
					log.Printf("received: %d bytes\n", n)
					return
				}
				state.RecvBuf = bytes.NewBuffer(buffer)
			}
			Opcode, err := GetOpcode(state.RecvBuf.Bytes())
			if err != nil {
				log.Println(err)
				return
			}
			if Opcode < OP_MIN || Opcode > OP_MAX {
				log.Printf("Invalid opcode: %d\n", Opcode)
				return
			}
			var fsmStep StateFunc
			switch state.SType {
			case CLIENT:
				fsmStep = FSMClientTable[state.OpSent][Opcode]
			case SERVER:
				fsmStep = FSMServerTable[state.OpSent][Opcode]
			default:
				log.Println("Invalid state type")
				return
			}
			state.OpRecv = int(Opcode)
			err = fsmStep(state)
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (s *State) SendRQ(r int, filename string) error {
	var req Request
	req.Filename = filename
	b, err := GobEncode(req, OP_RRQ)
	if err != nil {
		return err
	}
	log.Println("SENDING [RQ ->]")
	_, err = s.Conn.Write(b)
	if err != nil {
		return err
	}
	s.OpSent = r
	s.SendBuf = bytes.NewBuffer(b)
	return nil
}

func (s *State) SendAck(blknum int) error {
	var ack Ack
	ack.BlockNumber = int16(blknum)
	b, err := GobEncode(ack, OP_ACK)
	if err != nil {
		return err
	}
	log.Println("SENDING [ACK ->]")
	_, err = s.Conn.Write(b)
	if err != nil {
		return err
	}
	s.OpSent = OP_ACK
	s.SendBuf = bytes.NewBuffer(b)
	return nil
}

func (s *State) SendData(blknum int, data []byte) error {
	var block Block
	block.BlockNumber = int16(blknum)
	block.CheckSum = sha256.Sum256(data)
	copy(block.Data, data)
	b, err := GobEncode(block, OP_DATA)
	if err != nil {
		return err
	}
	log.Println("SENDING [DATA ->]")
	n, err := s.Conn.Write(b)
	if err != nil {
		return err
	}
	s.OpSent = OP_DATA
	s.SendBuf = bytes.NewBuffer(b)
	s.TotalBytes += int64(n)
	s.NextBlockNum = blknum //for the ack
	return nil
}

func (s *State) SendError(err error) error {
	var es Error
	es.ErrString = err.Error()
	b, err := GobEncode(es, OP_ERROR)
	if err != nil {
		return err
	}
	log.Println("SENDING [ERROR ->]")
	_, err = s.Conn.Write(b)
	if err != nil {
		return err
	}
	s.OpSent = OP_ERROR
	s.SendBuf = bytes.NewBuffer(b)
	return nil
}
