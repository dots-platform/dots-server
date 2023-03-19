package main

import (
	"flag"
	"net"

	log "github.com/sirupsen/logrus"
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
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Error reading config")
	}

	// Get our node's config.
	nodeConfig, ok := config.Nodes[nodeId]
	if !ok {
		log.WithFields(log.Fields{
			"nodeId": nodeId,
		}).Fatal("Node ID not present in config nodes")
	}

	// Spawn gRPC server.
	conn, err := net.Listen("tcp", nodeConfig.Addr)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to listen")
	}
	grpcServer := grpc.NewServer()
	dotspb.RegisterDecExecServer(grpcServer, &dotsservergrpc.DotsServerGrpc{})

	// Listen.
	if err := grpcServer.Serve(conn); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("Failed to server")
	}
}
