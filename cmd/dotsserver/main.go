package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/dtrust-project/dtrust-server/internal/dotsservergrpc"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

func main() {
	var port int

	// Create and parse arguments.
	flag.IntVar(&port, "port", 50051, "Port for the gRPC server to listen on")
	flag.Parse()

	// Spawn gRPC server.
	conn, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	dotspb.RegisterDecExecServer(grpcServer, &dotsservergrpc.DotsServerGrpc{})

	// Listen.
	if err := grpcServer.Serve(conn); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
