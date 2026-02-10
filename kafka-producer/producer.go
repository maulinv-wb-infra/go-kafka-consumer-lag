package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()
	topic := "foo"
	var n int

	for {
		n++
		payload := fmt.Sprintf("msg-%d", n)
		record := &kgo.Record{
			Topic:     topic,
			Value:     []byte(payload),
			Timestamp: time.Now(),
		}
		cl.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				fmt.Printf("produce error: %v\n", err)
				return
			}
			fmt.Printf("produced %q | partition %d\n", r.Value, r.Partition)
		})
		time.Sleep(2000 * time.Millisecond) // e.g. 1 msg/2 sec; adjust as needed
	}
}
