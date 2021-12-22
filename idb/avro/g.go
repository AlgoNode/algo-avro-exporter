package avro

import (
	"context"
	"errors"
	"strconv"
	"time"
)

//GetInt Parses string and returns int32 or zero
func GetInt(i string) int32 {
	if iInt, err := strconv.ParseInt(i, 10, 64); err != nil {
		return 0
	} else {
		return int32(iInt)
	}
}

//ParseUepoch parses string with 64bit microsecond epoch and returns string with 32bit seconds unix epoch
func ParseUepoch(us string) string {
	if us == "" || us == "0" {
		return "NULL"
	}
	if usInt, err := strconv.ParseInt(us, 10, 64); err != nil {
		return ""
	} else {
		tm := time.Unix(0, usInt*int64(time.Microsecond))
		return "'" + tm.String()[:23] + "'"
	}
}

//SafeDIVINT divides by zero return ZERO :D
func SafeDIVINT(n, sum int64) int64 {
	if sum > 0 {
		return n / sum
	}
	return 0
}

//SafePCTINT returns 0-100% n/sum
func SafePCTINT(n, sum int64) int64 {
	if sum > 0 {
		return 100 * n / sum
	}
	return 0
}

//MkBool returns "true" for true and "false" otherwise
func MkBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

//BackoffFun - a template for backoff function
type BackoffFun func(try int) (fast bool, e error)

//Backoff - universal backoff
func Backoff(ctx context.Context, tries int, msLimit int, fun BackoffFun) error {
	sleepms := 16
	var f bool
	var e error
	for i := 0; i < tries || tries < 0; i++ {
		f, e = fun(i + 1)
		if f {
			return e
		}
		if e == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("Context cancelled")
		case <-time.After(time.Duration(sleepms) * time.Millisecond):
		}
		sleepms *= 2
		if msLimit > 0 && (sleepms > msLimit) {
			sleepms = msLimit
		}
	}
	return e
}

//Timer - a timer util structure
type Timer struct {
	time.Time
}

//PStart starts a timer
func PStart() Timer {
	return Timer{time.Now()}
}

//ElapsedUs returns microseconds since start
func (t Timer) ElapsedUs() int64 {
	return int64(time.Since(t.Time) / time.Microsecond)
}

//ElapsedMs returns milliseconds since start
func (t Timer) ElapsedMs() int64 {
	return int64(time.Since(t.Time) / time.Millisecond)
}

//IsChanClosed - returns true when channel is closed
func IsChanClosed(ch chan struct{}) bool {
	if len(ch) == 0 {
		select {
		case _, ok := <-ch:
			return !ok
		}
	}
	return false
}
