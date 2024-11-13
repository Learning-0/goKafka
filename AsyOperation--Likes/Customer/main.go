package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type likeEvent struct {
	UserId string `json:"user_id"`
	PostId int    `json:"post_id"`
}

type Post struct {
	ID    int `gorm:"primaryKey"`
	Title string
	Likes int `gorm:"default:0"`
}

var reader *kafka.Reader

func main() {
	dsn := "root:password@tcp(localhost:3306)/test"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	err = db.AutoMigrate(&Post{})
	if err != nil {
		log.Fatalf("failed to migrate database: %v", err)
	}
	// 创建一个示例帖子
	//createPost(db, "示例帖子标题")
	go listenSignal()
	ctx := context.Background()
	Customer(ctx, db)

}
func Customer(ctx context.Context, db *gorm.DB) {
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "likes",
		GroupID:        "like-consumer-group",
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("failed to read message: %v", err)
			continue
		} else {
			fmt.Printf("topic=%s,partition=%d,offset=%d,key=%s,value=%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		}

		var event likeEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("failed to Unmarshal event:%V", err)
			continue
		}
		var post Post

		// 尝试查找帖子
		err = db.Where("id = ?", event.PostId).First(&post).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			log.Printf("failed to query db: %v", err)
			continue
		}

		// 如果找不到记录，则创建一条新的记录。其实不符合逻辑，按理说帖子不存在就应该报错。这里不想操作数据库，不存在就直接创建了
		if err == gorm.ErrRecordNotFound {
			post = Post{
				ID:    event.PostId,
				Title: "新帖子",
				Likes: 1,
			}
			err = db.Create(&post).Error
			if err != nil {
				log.Printf("failed to create post: %v", err)
				continue
			}
		} else {
			// 如果记录存在，则更新点赞数
			err = db.Model(&post).Update("likes", gorm.Expr("likes + ?", 1)).Error
			if err != nil {
				log.Printf("failed to update db: %v", err)
				continue
			}
		}
		log.Printf("用户 %s 点赞了帖子 %s", event.UserId, event.PostId)
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

func createPost(db *gorm.DB, title string) {
	post := Post{

		Title: title,
	}
	if err := db.Create(&post).Error; err != nil {
		log.Printf("failed to create post:%v", err)
	} else {
		log.Printf("创建成功ID：%s", post.ID)
	}

}
