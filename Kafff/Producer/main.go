package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

var topic = "user"

func Producer(ctx context.Context) {
	writer := kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true, //实际生产环境应当为false,topic的创建需要专门的运维成员负责
	}
	defer writer.Close()
	for i := 0; i < 3; i++ {
		if err := writer.WriteMessages( //写入是原子操作
			ctx,
			kafka.Message{Key: []byte("1"), Value: []byte("你好")},
			kafka.Message{Key: []byte("2"), Value: []byte("hello")},
			kafka.Message{Key: []byte("3"), Value: []byte("hah")},
			kafka.Message{Key: []byte("2"), Value: []byte("哈")},
			kafka.Message{Key: []byte("1"), Value: []byte("3月7")},
		); err != nil {
			if err == kafka.LeaderNotAvailable {
				time.Sleep(5 * time.Second)
				continue
			} else {
				fmt.Printf("写入失败%v", err)
			}
		} else {
			break
		}
	}
}

func main() {
	ctx := context.Background()
	Producer(ctx)
}
