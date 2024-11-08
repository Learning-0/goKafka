package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	topic  = "user"
	reader *kafka.Reader
)

func Customer(ctx context.Context) {
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		GroupID:        "rec_term",
		StartOffset:    kafka.FirstOffset,
	})
	for {
		if msg, err := reader.ReadMessage(ctx); err != nil {
			fmt.Printf("信息读取失败%v", err)
			break
		} else {
			fmt.Printf("topic=%s,partition=%d,offset=%d,key=%s,value=%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		}
	}
}

func listenSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c
	fmt.Printf("接收到信号%s", sig.String())
	if reader != nil {
		reader.Close()
	}
	os.Exit(0)
}

func main() {
	ctx := context.Background()
	go listenSignal()
	Customer(ctx)
}
