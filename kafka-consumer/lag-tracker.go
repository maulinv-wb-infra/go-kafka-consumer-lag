package kafkaconsumer

import (
	"sync"
	"time"
)

// MinuteLagMetric is emitted on minute rollover with the average consumption
// lag (ms) for the minute that just ended.
type MinuteLagMetric struct {
	Minute   time.Time // minute bucket (truncated to minute)
	AvgLagMs float64   // average consumption lag in milliseconds
	Count    int       // number of records in that minute
}

// minuteLagBucket holds sum of consumption lags (ms) and count for one minute.
type minuteLagBucket struct {
	sumLagMs int64
	count    int
}

// ConsumptionLagTracker measures record consumption lag (now - rec.Timestamp) in
// milliseconds and reports per-minute average lag. Safe for concurrent use.
// If MinuteLagChan is set, the tracker sends a MinuteLagMetric on each minute
// rollover (non-blocking).
type ConsumptionLagTracker struct {
	mu            sync.RWMutex
	minutes       map[time.Time]*minuteLagBucket // minute (consumption time) -> bucket
	maxMins       int                            // if > 0, keep only last maxMins minutes
	MinuteLagChan chan<- MinuteLagMetric         // optional; receives metric on minute rollover
	lastMinute    time.Time                      // last minute we recorded into; used to detect rollover
}

// NewConsumptionLagTracker creates a lag tracker. If maxMins > 0, only the
// last maxMins minute buckets are retained. If minuteLagChan is non-nil, a
// MinuteLagMetric is sent on it on each minute rollover (non-blocking send).
func NewConsumptionLagTracker(maxMins int, minuteLagChan chan<- MinuteLagMetric) *ConsumptionLagTracker {
	return &ConsumptionLagTracker{
		minutes:       make(map[time.Time]*minuteLagBucket),
		maxMins:       maxMins,
		MinuteLagChan: minuteLagChan,
	}
}

// RecordAndMinuteAvgLag records one consumed record: computes consumption lag
// (now - recTimestamp) in ms, buckets by current minute, and returns the lag
// for this record and the average lag in ms for the current minute.
// On minute rollover, if MinuteLagChan is set, sends the previous minute's
// average lag (non-blocking).
func (c *ConsumptionLagTracker) RecordAndMinuteAvgLag(recTimestamp time.Time) (lagMs int64, avgLagMsThisMinute float64) {
	now := time.Now()
	lagMs = now.Sub(recTimestamp).Milliseconds()
	minute := now.Truncate(time.Minute)

	c.mu.Lock()
	// On minute rollover, emit metric for the minute we just left
	if c.MinuteLagChan != nil && !c.lastMinute.IsZero() && minute != c.lastMinute {
		if b := c.minutes[c.lastMinute]; b != nil && b.count > 0 {
			metric := MinuteLagMetric{
				Minute:   c.lastMinute,
				AvgLagMs: float64(b.sumLagMs) / float64(b.count),
				Count:    b.count,
			}
			select {
			case c.MinuteLagChan <- metric:
			default:
				// non-blocking: skip if channel full
			}
		}
	}
	c.lastMinute = minute

	if c.minutes[minute] == nil {
		c.minutes[minute] = &minuteLagBucket{}
	}
	b := c.minutes[minute]
	b.sumLagMs += lagMs
	b.count++
	avgLagMsThisMinute = float64(b.sumLagMs) / float64(b.count)

	if c.maxMins > 0 {
		cutoff := minute.Add(-time.Duration(c.maxMins) * time.Minute)
		for t := range c.minutes {
			if t.Before(cutoff) {
				delete(c.minutes, t)
			}
		}
	}
	c.mu.Unlock()

	return lagMs, avgLagMsThisMinute
}

// AvgLagMsPerMinute returns the overall average consumption lag in milliseconds
// across all retained minute buckets (each minute contributes one average).
func (c *ConsumptionLagTracker) AvgLagMsPerMinute() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.minutes) == 0 {
		return 0
	}
	var totalAvg float64
	for _, b := range c.minutes {
		if b.count > 0 {
			totalAvg += float64(b.sumLagMs) / float64(b.count)
		}
	}
	return totalAvg / float64(len(c.minutes))
}
