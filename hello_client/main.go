package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "grpc/hello_server/proto"
	"log"
)

func main() {
	//连接到server端，此处没有使用加密和验证
	conn, err := grpc.Dial("127.0.0.1:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatal("error", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	//建立连接
	client := pb.NewSayHelloClient(conn)

	//执行rpc调用（这个方法在服务器端实现并返回结果）
	resp, err := client.SayHello(context.Background(), &pb.HelloRequest{RequestName: "world"})
	if err != nil {
		fmt.Println("rpc调用失败", err)
	}
	fmt.Println(resp.GetResponseMsg())
}
