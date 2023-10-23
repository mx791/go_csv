package main

import (
	"fmt"
	"time"
)

func benchmark(fn func()) {
	for threadCount := 1; threadCount < 8; threadCount += 2 {
		start := time.Now()
		fn()
		elapsed := time.Since(start)
		fmt.Println("threads: ", threadCount, ", time: ", elapsed)
	}
}
