package protocol

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// A timeout and retransmission technique using adaptive timeout with exponential backoff
// all calculations are in nanoseconds
type Rtt struct {
	Rtt       float32
	SRtt      float32
	DevRtt    float32
	NumRexmt  int16
	CurRto    time.Duration
	NextRto   time.Duration
	TimeStart time.Time
	TimeStop  time.Time
}

const (
	RTT_RXTMIN    = 2   //min retransmit timeout value in seconds
	RTT_RXTMAX    = 120 //max retransmit timeout value in seconds
	RTT_MAXNREXMT = 4   //max #times to retransmit
)

var (
	ExpBackoff = [RTT_MAXNREXMT + 1]int{1, 2, 4, 8, 16}
	ErrTimeout = errors.New("timeout")
)

func NewRtt() *Rtt {
	return &Rtt{
		Rtt:     0,
		SRtt:    0,
		DevRtt:  1.5,
		NextRto: 0,
	}
}

func (rtt *Rtt) NewPack() {
	rtt.NumRexmt = 0
}

func (rtt *Rtt) Start() time.Duration {
	var rexmt int
	if rtt.NumRexmt > 0 {
		//this is a retransmission
		rtt.CurRto *= (time.Duration(ExpBackoff[rtt.NumRexmt]))
		return rtt.CurRto * time.Second
	}
	rtt.TimeStart = time.Now()
	if rtt.NextRto > 0 {
		rtt.CurRto = rtt.NextRto
		rtt.NextRto = 0
		return rtt.CurRto
	}
	rexmt = int(rtt.SRtt + (2.0 * rtt.DevRtt) + 0.5)
	if rexmt < RTT_RXTMIN {
		rexmt = RTT_RXTMIN
	} else if rexmt > RTT_RXTMAX {
		rexmt = RTT_RXTMAX
	}
	rtt.CurRto = time.Duration(rexmt)
	return rtt.CurRto * time.Second
}

func (rtt *Rtt) Stop() {
	var err float64
	if rtt.NumRexmt > 0 {
		rtt.NextRto = rtt.CurRto
		return
	}
	//reset next retransmit timeout
	rtt.NextRto = 0

	rtt.TimeStop = time.Now()
	elapsedTime := rtt.TimeStop.Sub(rtt.TimeStart)
	rtt.Rtt = float32(elapsedTime.Seconds())

	//using jacobson's algorithm, update the smooth and mean deviation
	err = float64(rtt.Rtt) - float64(rtt.SRtt)
	rtt.SRtt += float32(err / 8)
	rtt.DevRtt += float32((math.Abs(err) - float64(rtt.DevRtt)) / 4)
}

func (rtt *Rtt) Timeout() error {
	rtt.Stop()
	rtt.NumRexmt = rtt.NumRexmt + 1
	if rtt.NumRexmt > RTT_MAXNREXMT {
		return ErrTimeout
	}
	return nil
}

func (rtt *Rtt) String() string {
	return fmt.Sprintf("rtt = %.5f, srtt = %.3f rtt-dev = %.3f currto = %d\n", rtt.Rtt, rtt.SRtt, rtt.DevRtt, rtt.CurRto)
}
