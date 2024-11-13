package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type likeEvent struct {
	UserId string `json:"user_id"`
	PostId int    `json:"post_id"`
}

func main() {
	writer := kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "likes",
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true,
	}

	defer writer.Close()

	//模拟用户点赞
	event := likeEvent{
		UserId: "user123",
		PostId: 1, //postid对应帖子喜欢数加一
	}

	//将事件转为json
	message, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("failed to marshal event: %v", err)
	}

	//发送消息到kafka
	for i := 0; i < 3; i++ {
		if err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(event.UserId),
				Value: message,
			},
		); err != nil {
			if err == kafka.LeaderNotAvailable {
				time.Sleep(5 * time.Second)
				continue
			} else {
				log.Fatalf("failed to write messasge %v", err)
			}
		} else {
			log.Println("点赞事件已发送")
			break
		}
	}

}
