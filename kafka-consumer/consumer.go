package kafkaconsumer

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run starts the Kafka consumer loop on topic "foo", measuring consumption lag.
// It blocks until the context is cancelled.
// If minuteLagChan is non-nil, a MinuteLagMetric is sent on each minute rollover.
func Run(ctx context.Context, minuteLagChan chan<- MinuteLagMetric) {
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("my-kgo-1"),
		kgo.ConsumeTopics("foo"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	lagTracker := NewConsumptionLagTracker(10, minuteLagChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			lagMs := lagTracker.RecordLag(rec.Timestamp, rec.Topic, rec.Partition)
			fmt.Printf("msg %q | partition %d | lag %d ms\n", rec.Value, rec.Partition, lagMs)
		}
	}
}
