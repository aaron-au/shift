//go:build !unix

package main

import "time"

func procUsage() (peakRSS int64, user, sys time.Duration) { return 0, 0, 0 }
