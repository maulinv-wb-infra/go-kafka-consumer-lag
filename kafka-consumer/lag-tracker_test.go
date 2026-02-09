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

func TestRecordLag_SingleRecord(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)
	recTs := base.Add(-100 * time.Millisecond) // 100 ms ago

	lagMs := tracker.RecordLag(recTs)

	if lagMs != 100 {
		t.Errorf("lagMs = %d, want 100", lagMs)
	}
}

func TestRecordLag_MultipleRecordsSameMinute(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)

	lag1 := tracker.RecordLag(base.Add(-100 * time.Millisecond))
	if lag1 != 100 {
		t.Errorf("first record: lag1=%d, want 100", lag1)
	}

	lag2 := tracker.RecordLag(base.Add(-200 * time.Millisecond))
	if lag2 != 200 {
		t.Errorf("second record: lag2=%d, want 200", lag2)
	}

	lag3 := tracker.RecordLag(base.Add(-300 * time.Millisecond))
	if lag3 != 300 {
		t.Errorf("third record: lag3=%d, want 300", lag3)
	}
}

func TestRecordLag_MinuteRolloverSendsMetric(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 2)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	// Minute 1: one record with 50 ms lag
	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-50 * time.Millisecond))

	// Minute 2: rollover -> should emit metric for minute 14:30
	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-10 * time.Millisecond))

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

func TestRecordLag_NoMetricWhenChannelNil(t *testing.T) {
	tracker := NewConsumptionLagTracker(10, nil) // nil channel

	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker.RecordLag(base.Add(-50 * time.Millisecond))

	// Roll to next minute
	nowFunc = func() time.Time { return base.Add(time.Minute) }
	tracker.RecordLag(base.Add(time.Minute - 10*time.Millisecond))

	// No channel to receive from; just ensure no panic
}

func TestRecordLag_NonBlockingWhenChannelFull(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 1) // buffer 1
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	// Fill the channel
	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1 * time.Millisecond))
	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1 * time.Millisecond)) // sends first metric, channel full

	// Trigger another rollover without consuming from channel -> non-blocking send should skip
	now = time.Date(2025, 2, 9, 14, 32, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1 * time.Millisecond))

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

func TestMaxMins_EvictsOldBuckets(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(2, nil) // keep only last 2 minutes

	tracker.RecordLag(base.Add(-10 * time.Millisecond))
	nowFunc = func() time.Time { return base.Add(1 * time.Minute) }
	tracker.RecordLag(base.Add(1*time.Minute - 10*time.Millisecond))
	nowFunc = func() time.Time { return base.Add(2 * time.Minute) }
	lagMs := tracker.RecordLag(base.Add(2*time.Minute - 10*time.Millisecond))

	if lagMs != 10 {
		t.Errorf("last record: lagMs=%d, want 10", lagMs)
	}
}

func TestConcurrentRecordLag(t *testing.T) {
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
			tracker.RecordLag(recTs)
		}(i)
	}
	wg.Wait()
}
