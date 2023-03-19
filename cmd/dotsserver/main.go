package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/dtrust-project/dtrust-server/internal/config"
	"github.com/dtrust-project/dtrust-server/internal/dotsservergrpc"
	"github.com/dtrust-project/dtrust-server/protos/dotspb"
)

func main() {
	// Create and parse arguments.
	var configPath string
	var nodeId string
	flag.StringVar(&configPath, "config", "", "Path to the config file")
	flag.StringVar(&nodeId, "node_id", "", "ID of the DoTS node")
	flag.Parse()
	if configPath == "" || nodeId == "" {
		flag.Usage()
		return
	}

	// Get config.
	config, err := config.ReadConfig(configPath)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	// Get our node's config.
	nodeConfig, ok := config.Nodes[nodeId]
	if !ok {
		log.Fatalf("Node ID not present in config nodes: %s", nodeId)
	}

	// Spawn gRPC server.
	conn, err := net.Listen("tcp", nodeConfig.Addr)
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
