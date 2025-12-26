package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"runtime"
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
		invalid,
		recvRRQ,
		recvWQR,
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
	dec := gob.NewDecoder(s.RecvBuf)
	err := dec.Decode(&ack)
	if err != nil {
		return err
	}
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
	dec := gob.NewDecoder(s.RecvBuf)
	var payload Error
	err := dec.Decode(&payload)
	if err != nil {
		return err
	}
	switch payload.ErrCode {
	case ErrCodeNotDefined:
		err = ErrNotDefined
	case ErrCodeFileNotFound:
		err = ErrFileNotFound
	case ErrCodeAccessViolation:
		err = ErrAccessViolation
	case ErrCodeDiskFull:
		err = ErrDiskFull
	case ErrCodeIllegalOperation:
		err = ErrIllegalOperation
	case ErrCodeUnknownPortNumber:
		err = ErrUnknownPortNumber
	case ErrCodeFileAlreadyExists:
		err = ErrFileAlreadyExists
	case ErrCodeNoSuchUser:
		err = ErrNoSuchUser
	}
	return err
}

func recvData(s *State) error {
	var payload Block
	dec := gob.NewDecoder(s.RecvBuf)
	err := dec.Decode(&payload)
	if err != nil {
		return err
	}
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

func recvRRQ(s *State) error {
	var payload Request
	dec := gob.NewDecoder(s.RecvBuf)
	err := dec.Decode(&payload)
	if err != nil {
		return err
	}
	return nil
}
func recvWQR(s *State) error {
	var payload Request
	dec := gob.NewDecoder(s.RecvBuf)
	err := dec.Decode(&payload)
	if err != nil {
		return err
	}
	return nil
}

func (state *State) Loop(ctx context.Context) {
	state.Rtt.NewPack()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := state.Conn.SetReadDeadline(time.Now().Add(state.Rtt.Start())); err != nil {
				log.Println(err)
				runtime.Goexit()
			}
			state.RecvBuf.Reset()
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
			Opcode := binary.BigEndian.Uint16(buffer[0:2])
			if Opcode < OP_MIN || Opcode > OP_MAX {
				log.Printf("Invalid opcode: %d\n", Opcode)
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
			state.RecvBuf = bytes.NewBuffer(buffer)
			err = fsmStep(state)
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (s *State) SendRQ(r int) error {
	var req Request
	req.Opcode = uint16(r)
	req.Filename = s.CurFile.Name()
	b, err := GobEncode(req)
	if err != nil {
		return err
	}
	n, err := s.Conn.WriteTo(b, s.Conn.RemoteAddr())
	if err != nil {
		return err
	}
	s.OpSent = r
	s.SendBuf = bytes.NewBuffer(b)
	s.TotalBytes += int64(n)
	return nil
}

func (s *State) SendAck(blknum int) error {
	var ack Ack
	ack.Opcode = OP_ACK
	ack.BlockNumber = int16(blknum)
	b, err := GobEncode(ack)
	if err != nil {
		return err
	}
	n, err := s.Conn.WriteTo(b, s.Conn.RemoteAddr())
	if err != nil {
		return err
	}
	s.OpSent = OP_ACK
	s.SendBuf = bytes.NewBuffer(b)
	s.TotalBytes += int64(n)
	return nil
}

func (s *State) SendData(blknum int, data []byte) error {
	var block Block
	block.Opcode = OP_DATA
	block.BlockNumber = int16(blknum)
	block.CheckSum = sha256.Sum256(data)
	copy(block.Data, data)
	b, err := GobEncode(block)
	if err != nil {
		return err
	}
	n, err := s.Conn.WriteTo(b, s.Conn.RemoteAddr())
	if err != nil {
		return err
	}
	s.OpSent = OP_DATA
	s.SendBuf = bytes.NewBuffer(b)
	s.TotalBytes += int64(n)
	s.NextBlockNum = blknum //for the ack
	return nil
}

func (s *State) SendError(errCode int) error {
	var es Error
	es.Opcode = OP_ERROR
	es.ErrCode = int16(errCode)
	var err error
	switch errCode {
	case ErrCodeNotDefined:
		err = ErrNotDefined
	case ErrCodeFileNotFound:
		err = ErrFileNotFound
	case ErrCodeAccessViolation:
		err = ErrAccessViolation
	case ErrCodeDiskFull:
		err = ErrDiskFull
	case ErrCodeIllegalOperation:
		err = ErrIllegalOperation
	case ErrCodeUnknownPortNumber:
		err = ErrUnknownPortNumber
	case ErrCodeFileAlreadyExists:
		err = ErrFileAlreadyExists
	case ErrCodeNoSuchUser:
		err = ErrNoSuchUser
	}
	es.ErrString = err.Error()
	b, err := GobEncode(es)
	if err != nil {
		return err
	}
	n, err := s.Conn.WriteTo(b, s.Conn.RemoteAddr())
	if err != nil {
		return err
	}
	s.OpSent = OP_ERROR
	s.SendBuf = bytes.NewBuffer(b)
	s.TotalBytes += int64(n)
	return nil
}
