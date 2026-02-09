package kafkaconsumer

import (
	"sync"
	"testing"
	"time"
)

func TestNewConsumptionLagTracker(t *testing.T) {
	tracker := NewConsumptionLagTracker(0, nil)
	if tracker == nil {
		t.Fatal("NewConsumptionLagTracker returned nil")
	}
	if tracker.maxMins != 0 {
		t.Errorf("maxMins = %d, want 0", tracker.maxMins)
	}

	trackerWithChan := NewConsumptionLagTracker(5, make(chan MinuteLagMetric, 1))
	if trackerWithChan == nil || trackerWithChan.maxMins != 5 {
		t.Errorf("expected maxMins 5, got %d", trackerWithChan.maxMins)
	}
}

func TestRecordAndMinuteAvgLag_SingleRecord(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)
	recTs := base.Add(-100 * time.Millisecond) // 100 ms ago

	lagMs, avgLagMs := tracker.RecordAndMinuteAvgLag(recTs)

	if lagMs != 100 {
		t.Errorf("lagMs = %d, want 100", lagMs)
	}
	if avgLagMs != 100 {
		t.Errorf("avgLagMsThisMinute = %v, want 100", avgLagMs)
	}
}

func TestRecordAndMinuteAvgLag_MultipleRecordsSameMinute(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)

	// First record: 100 ms lag
	lag1, avg1 := tracker.RecordAndMinuteAvgLag(base.Add(-100 * time.Millisecond))
	if lag1 != 100 || avg1 != 100 {
		t.Errorf("first record: lag1=%d avg1=%v, want 100, 100", lag1, avg1)
	}

	// Second record: 200 ms lag -> bucket sum 300, count 2, avg 150
	lag2, avg2 := tracker.RecordAndMinuteAvgLag(base.Add(-200 * time.Millisecond))
	if lag2 != 200 || avg2 != 150 {
		t.Errorf("second record: lag2=%d avg2=%v, want 200, 150", lag2, avg2)
	}

	// Third record: 300 ms lag -> sum 600, count 3, avg 200
	lag3, avg3 := tracker.RecordAndMinuteAvgLag(base.Add(-300 * time.Millisecond))
	if lag3 != 300 || avg3 != 200 {
		t.Errorf("third record: lag3=%d avg3=%v, want 300, 200", lag3, avg3)
	}
}

func TestRecordAndMinuteAvgLag_MinuteRolloverSendsMetric(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 2)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	// Minute 1: one record with 50 ms lag
	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordAndMinuteAvgLag(now.Add(-50 * time.Millisecond))

	// Minute 2: rollover -> should emit metric for minute 14:30
	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordAndMinuteAvgLag(now.Add(-10 * time.Millisecond))

	select {
	case m := <-metricChan:
		if !m.Minute.Equal(time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)) {
			t.Errorf("metric Minute = %v, want 14:30", m.Minute)
		}
		if m.AvgLagMs != 50 {
			t.Errorf("metric AvgLagMs = %v, want 50", m.AvgLagMs)
		}
		if m.Count != 1 {
			t.Errorf("metric Count = %d, want 1", m.Count)
		}
	default:
		t.Fatal("no metric received on rollover")
	}
}

func TestRecordAndMinuteAvgLag_NoMetricWhenChannelNil(t *testing.T) {
	tracker := NewConsumptionLagTracker(10, nil) // nil channel

	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker.RecordAndMinuteAvgLag(base.Add(-50 * time.Millisecond))

	// Roll to next minute
	nowFunc = func() time.Time { return base.Add(time.Minute) }
	tracker.RecordAndMinuteAvgLag(base.Add(time.Minute - 10*time.Millisecond))

	// No channel to receive from; just ensure no panic
}

func TestRecordAndMinuteAvgLag_NonBlockingWhenChannelFull(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 1) // buffer 1
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	// Fill the channel
	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordAndMinuteAvgLag(now.Add(-1 * time.Millisecond))
	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordAndMinuteAvgLag(now.Add(-1 * time.Millisecond)) // sends first metric, channel full

	// Trigger another rollover without consuming from channel -> non-blocking send should skip
	now = time.Date(2025, 2, 9, 14, 32, 0, 0, time.UTC)
	tracker.RecordAndMinuteAvgLag(now.Add(-1 * time.Millisecond))

	// Only one metric should be in the channel (second rollover send was dropped)
	_, ok := <-metricChan
	if !ok {
		t.Fatal("channel closed")
	}
	select {
	case <-metricChan:
		t.Error("expected only one metric in channel; second rollover should have been dropped")
	default:
		// good: channel empty
	}
}

func TestAvgLagMsPerMinute_Empty(t *testing.T) {
	tracker := NewConsumptionLagTracker(0, nil)
	avg := tracker.AvgLagMsPerMinute()
	if avg != 0 {
		t.Errorf("AvgLagMsPerMinute() = %v, want 0", avg)
	}
}

func TestAvgLagMsPerMinute_OneMinute(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)
	tracker.RecordAndMinuteAvgLag(base.Add(-100 * time.Millisecond))
	tracker.RecordAndMinuteAvgLag(base.Add(-200 * time.Millisecond))

	avg := tracker.AvgLagMsPerMinute()
	if avg != 150 {
		t.Errorf("AvgLagMsPerMinute() = %v, want 150", avg)
	}
}

func TestAvgLagMsPerMinute_TwoMinutes(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)
	tracker.RecordAndMinuteAvgLag(base.Add(-100 * time.Millisecond))           // min1 avg 100
	tracker.RecordAndMinuteAvgLag(base.Add(-200 * time.Millisecond))           // min1 avg 150
	nowFunc = func() time.Time { return base.Add(time.Minute) }
	tracker.RecordAndMinuteAvgLag(base.Add(time.Minute - 50*time.Millisecond)) // min2 avg 50

	avg := tracker.AvgLagMsPerMinute()
	// (150 + 50) / 2 = 100
	if avg != 100 {
		t.Errorf("AvgLagMsPerMinute() = %v, want 100", avg)
	}
}

func TestMaxMins_EvictsOldBuckets(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(2, nil) // keep only last 2 minutes

	tracker.RecordAndMinuteAvgLag(base.Add(-10 * time.Millisecond))
	nowFunc = func() time.Time { return base.Add(1 * time.Minute) }
	tracker.RecordAndMinuteAvgLag(base.Add(1*time.Minute - 10*time.Millisecond))
	nowFunc = func() time.Time { return base.Add(2 * time.Minute) }
	tracker.RecordAndMinuteAvgLag(base.Add(2*time.Minute - 10*time.Millisecond))

	// Bucket for 14:30 should be evicted; only 14:31 and 14:32 remain
	avg := tracker.AvgLagMsPerMinute()
	if avg != 10 {
		t.Errorf("AvgLagMsPerMinute() = %v, want 10 (only two minutes retained)", avg)
	}
}

func TestConcurrentRecordAndAvgLag(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			recTs := base.Add(-time.Duration(j*10) * time.Millisecond)
			tracker.RecordAndMinuteAvgLag(recTs)
			tracker.AvgLagMsPerMinute()
		}(i)
	}
	wg.Wait()

	avg := tracker.AvgLagMsPerMinute()
	if avg <= 0 {
		t.Errorf("AvgLagMsPerMinute() = %v after concurrent records", avg)
	}
}
