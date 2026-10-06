//go:build !linux

package telemetry

func resources() (cpu, rss, current, peak *int64) { return nil, nil, nil, nil }
