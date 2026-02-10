package kafkaconsumer

import (
	"fmt"
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
	recTs := base.Add(-100 * time.Millisecond)

	lagMs := tracker.RecordLag(recTs, "foo", 0)

	if lagMs != 100 {
		t.Errorf("lagMs = %d, want 100", lagMs)
	}
}

func TestRecordLag_MultipleRecordsSameMinuteSameTopicPartition(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(0, nil)

	lag1 := tracker.RecordLag(base.Add(-100*time.Millisecond), "foo", 0)
	if lag1 != 100 {
		t.Errorf("first record: lag1=%d, want 100", lag1)
	}

	lag2 := tracker.RecordLag(base.Add(-200*time.Millisecond), "foo", 0)
	if lag2 != 200 {
		t.Errorf("second record: lag2=%d, want 200", lag2)
	}

	lag3 := tracker.RecordLag(base.Add(-300*time.Millisecond), "foo", 0)
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

	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-50*time.Millisecond), "foo", 0)

	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-10*time.Millisecond), "foo", 0)

	select {
	case m := <-metricChan:
		if !m.Minute.Equal(time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)) {
			t.Errorf("metric Minute = %v, want 14:30", m.Minute)
		}
		if m.Topic != "foo" || m.Partition != 0 {
			t.Errorf("metric Topic=%q Partition=%d, want foo 0", m.Topic, m.Partition)
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
	tracker := NewConsumptionLagTracker(10, nil)

	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker.RecordLag(base.Add(-50*time.Millisecond), "foo", 0)

	nowFunc = func() time.Time { return base.Add(time.Minute) }
	tracker.RecordLag(base.Add(time.Minute-10*time.Millisecond), "foo", 0)
}

func TestRecordLag_NonBlockingWhenChannelFull(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 1)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1*time.Millisecond), "foo", 0)
	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1*time.Millisecond), "foo", 0)

	now = time.Date(2025, 2, 9, 14, 32, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-1*time.Millisecond), "foo", 0)

	_, ok := <-metricChan
	if !ok {
		t.Fatal("channel closed")
	}
	select {
	case <-metricChan:
		t.Error("expected only one metric; second rollover send should be dropped when channel full")
	default:
	}
}

func TestRecordLag_MultipleTopicPartitionsEmitSeparateMetrics(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 4)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-50*time.Millisecond), "foo", 0)
	tracker.RecordLag(now.Add(-100*time.Millisecond), "bar", 1)

	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-10*time.Millisecond), "foo", 0)

	metrics := make(map[string]MinuteLagMetric)
	for i := 0; i < 2; i++ {
		select {
		case m := <-metricChan:
			metrics[fmt.Sprintf("%s-%d", m.Topic, m.Partition)] = m
		default:
			t.Fatalf("expected 2 metrics on rollover, got %d", i)
		}
	}
	if m := metrics["foo-0"]; m.Count != 1 || m.AvgLagMs != 50 {
		t.Errorf("foo/0: Count=%d AvgLagMs=%v, want 1, 50", m.Count, m.AvgLagMs)
	}
	if m := metrics["bar-1"]; m.Count != 1 || m.AvgLagMs != 100 {
		t.Errorf("bar/1: Count=%d AvgLagMs=%v, want 1, 100", m.Count, m.AvgLagMs)
	}
}

func TestRecordLag_MultipleTopicPartitionsEmitSeparateMetrics2(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 4)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-50*time.Millisecond), "foo", 0)
	tracker.RecordLag(now.Add(-150*time.Millisecond), "foo", 0)
	tracker.RecordLag(now.Add(-100*time.Millisecond), "bar", 1)

	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-10*time.Millisecond), "foo", 0)

	metrics := make(map[string]MinuteLagMetric)
	for i := 0; i < 2; i++ {
		select {
		case m := <-metricChan:
			metrics[fmt.Sprintf("%s-%d", m.Topic, m.Partition)] = m
		default:
			t.Fatalf("expected 2 metrics on rollover, got %d", i)
		}
	}
	if m := metrics["foo-0"]; m.Count != 2 || m.AvgLagMs != 100 {
		t.Errorf("foo/0: Count=%d AvgLagMs=%v, want 2, 100", m.Count, m.AvgLagMs)
	}
	if m := metrics["bar-1"]; m.Count != 1 || m.AvgLagMs != 100 {
		t.Errorf("bar/1: Count=%d AvgLagMs=%v, want 1, 100", m.Count, m.AvgLagMs)
	}
}

func TestRecordLag_MaxMinsEvictsOldBuckets(t *testing.T) {
	base := time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(2, nil)

	tracker.RecordLag(base.Add(-10*time.Millisecond), "foo", 0)
	nowFunc = func() time.Time { return base.Add(1 * time.Minute) }
	tracker.RecordLag(base.Add(1*time.Minute-10*time.Millisecond), "foo", 0)
	nowFunc = func() time.Time { return base.Add(2 * time.Minute) }
	lagMs := tracker.RecordLag(base.Add(2*time.Minute-10*time.Millisecond), "foo", 0)

	if lagMs != 10 {
		t.Errorf("lagMs = %d, want 10", lagMs)
	}
}

func TestRecordLag_AverageLagInMetric(t *testing.T) {
	metricChan := make(chan MinuteLagMetric, 1)
	var now time.Time
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	tracker := NewConsumptionLagTracker(10, metricChan)

	now = time.Date(2025, 2, 9, 14, 30, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-100*time.Millisecond), "foo", 0)
	tracker.RecordLag(now.Add(-200*time.Millisecond), "foo", 0)

	now = time.Date(2025, 2, 9, 14, 31, 0, 0, time.UTC)
	tracker.RecordLag(now.Add(-10*time.Millisecond), "foo", 0)

	m := <-metricChan
	if m.Count != 2 {
		t.Errorf("Count = %d, want 2", m.Count)
	}
	if m.AvgLagMs != 150 {
		t.Errorf("AvgLagMs = %v, want 150 (average of 100 and 200)", m.AvgLagMs)
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
			tracker.RecordLag(recTs, "foo", 0)
		}(i)
	}
	wg.Wait()
}
