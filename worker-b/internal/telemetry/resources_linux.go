//go:build linux

package telemetry

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func resources() (cpu, rss, current, peak *int64) {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		c := r.Utime.Sec*1000000 + r.Utime.Usec + r.Stime.Sec*1000000 + r.Stime.Usec
		m := r.Maxrss * 1024 // Linux ru_maxrss is KiB, process lifetime high-water mark.
		cpu, rss = &c, &m
	}
	read := func(path string) *int64 {
		b, err := os.ReadFile(path)
		if err != nil || len(b) > 128 {
			return nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil || n < 0 {
			return nil
		}
		return &n
	}
	return cpu, rss, read("/sys/fs/cgroup/memory.current"), read("/sys/fs/cgroup/memory.peak")
}
