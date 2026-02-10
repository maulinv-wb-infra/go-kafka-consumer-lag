package kafkaconsumer

import (
	"sync"
	"time"
)

// nowFunc is used for time in the tracker; tests can override for deterministic behavior.
var nowFunc = time.Now

// MinuteLagMetric is emitted on minute rollover with the average consumption
// lag (ms) for the minute that just ended, per topic and partition.
type MinuteLagMetric struct {
	Minute    time.Time // minute bucket (truncated to minute)
	Topic     string    // record topic
	Partition int32     // record partition
	AvgLagMs  float64   // average consumption lag in milliseconds
	Count     int       // number of records in that minute for this topic/partition
}

// minuteLagBucket holds sum of consumption lags (ms) and count for one (minute, topic, partition).
type minuteLagBucket struct {
	sumLagMs int64
	count    int
}

// bucketKey identifies a per-minute, per-topic, per-partition bucket.
type bucketKey struct {
	minute    time.Time
	topic     string
	partition int32
}

// ConsumptionLagTracker measures record consumption lag (now - rec.Timestamp) in
// milliseconds and reports per-minute average lag per topic/partition. Safe for concurrent use.
// If MinuteLagChan is set, the tracker sends a MinuteLagMetric on each minute
// rollover for each (topic, partition) that had records (non-blocking).
type ConsumptionLagTracker struct {
	mu            sync.RWMutex
	buckets       map[bucketKey]*minuteLagBucket // (minute, topic, partition) -> bucket
	maxMins       int                            // if > 0, keep only last maxMins minutes
	MinuteLagChan chan<- MinuteLagMetric         // optional; receives metric on minute rollover
	lastMinute    time.Time                      // last minute we recorded into; used to detect rollover
}

// NewConsumptionLagTracker creates a lag tracker. If maxMins > 0, only the
// last maxMins minute buckets are retained. If minuteLagChan is non-nil, a
// MinuteLagMetric is sent on it on each minute rollover (non-blocking send).
func NewConsumptionLagTracker(maxMins int, minuteLagChan chan<- MinuteLagMetric) *ConsumptionLagTracker {
	return &ConsumptionLagTracker{
		buckets:       make(map[bucketKey]*minuteLagBucket),
		maxMins:       maxMins,
		MinuteLagChan: minuteLagChan,
	}
}

// RecordLag records one consumed record: computes consumption lag (now - recTimestamp)
// in ms, buckets by current minute and recTopic/recPartition, and returns the lag for this record.
// On minute rollover, if MinuteLagChan is set, sends the previous minute's
// average lag per (topic, partition) (non-blocking).
func (c *ConsumptionLagTracker) RecordLag(recTimestamp time.Time, recTopic string, recPartition int32) int64 {
	now := nowFunc()
	lagMs := now.Sub(recTimestamp).Milliseconds()
	minute := now.Truncate(time.Minute)
	key := bucketKey{minute: minute, topic: recTopic, partition: recPartition}

	c.mu.Lock()
	// On minute rollover, emit one metric per (topic, partition) for the minute we just left
	if c.MinuteLagChan != nil && !c.lastMinute.IsZero() && minute != c.lastMinute {
		for k, b := range c.buckets {
			if k.minute.Equal(c.lastMinute) && b != nil && b.count > 0 {
				metric := MinuteLagMetric{
					Minute:    c.lastMinute,
					Topic:     k.topic,
					Partition: k.partition,
					AvgLagMs:  float64(b.sumLagMs) / float64(b.count),
					Count:     b.count,
				}
				select {
				case c.MinuteLagChan <- metric:
				default:
					// non-blocking: skip if channel full
				}
			}
		}
	}
	c.lastMinute = minute

	if c.buckets[key] == nil {
		c.buckets[key] = &minuteLagBucket{}
	}
	b := c.buckets[key]
	b.sumLagMs += lagMs
	b.count++

	if c.maxMins > 0 {
		cutoff := minute.Add(-time.Duration(c.maxMins) * time.Minute)
		for k := range c.buckets {
			if k.minute.Before(cutoff) {
				delete(c.buckets, k)
			}
		}
	}
	c.mu.Unlock()

	return lagMs
}
