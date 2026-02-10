package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"go-kafka-consumer-lag/kafka-consumer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Channel for minute rollover lag metrics; caller can consume or pass nil to disable.
	minuteLagChan := make(chan kafkaconsumer.MinuteLagMetric, 10)
	go func() {
		for m := range minuteLagChan {
			log.Printf("minute rollover metric: minute=%s consumer_group=%s topic=%s partition=%d avg_lag_ms=%.2f count=%d",
				m.Minute.Format("2006-01-02T15:04"), m.ConsumerGroup, m.Topic, m.Partition, m.AvgLagMs, m.Count)
		}
	}()

	kafkaconsumer.Run(ctx, minuteLagChan)
	close(minuteLagChan)
	fmt.Println("consumer stopped")
}
