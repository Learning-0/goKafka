package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	pb "grpc/hello_server/proto"

	"net"
)

type server struct {
	//UnimplementedSayHelloServer取自hello_grpc.pb.go文件

	pb.UnimplementedSayHelloServer
}

// SayHello 对hello_grpc.pb.go文件中的SayHello方法进行改写，修改成自己定义的结构体
func (s *server) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	return &pb.HelloResponse{ResponseMsg: "hello " + req.RequestName}, nil
}

// 这是一段注释
func main() {
	//开启端口
	listen, _ := net.Listen("tcp", "127.0.0.1:9090")
	//创建grpc服务
	grpcServer := grpc.NewServer()
	//服务注册
	pb.RegisterSayHelloServer(grpcServer, &server{})

	//启动服务
	err := grpcServer.Serve(listen)
	if err != nil {
		fmt.Println("服务启动失败！")
		return
	}
}
