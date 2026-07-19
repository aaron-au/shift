//go:build unix

package main

import (
	"runtime"
	"syscall"
	"time"
)

// procUsage returns peak RSS in bytes plus user/system CPU time.
func procUsage() (peakRSS int64, user, sys time.Duration) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0, 0
	}
	peakRSS = ru.Maxrss
	if runtime.GOOS != "darwin" {
		peakRSS *= 1024 // linux reports KiB; darwin reports bytes
	}
	user = time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys = time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return peakRSS, user, sys
}
