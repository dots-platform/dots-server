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
	var logLevel string
	flag.StringVar(&configPath, "config", "", "Path to the config file")
	flag.StringVar(&nodeId, "node_id", "", "ID of the DoTS node")
	flag.StringVar(&logLevel, "log_level", "info", "Log level. One of: fatal, error, warn, info, debug")
	flag.Parse()
	if configPath == "" || nodeId == "" {
		flag.Usage()
		return
	}
	switch logLevel {
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	}

	// Get config.
	conf, err := config.ReadConfig(configPath, nodeId)
	if err != nil {
		log.WithError(err).Fatal("Error reading config")
	}

	// Spawn gRPC server.
	conn, err := net.Listen("tcp", conf.OurNodeConfig.Addr)
	if err != nil {
		log.WithError(err).Fatal("Failed to listen")
	}
	grpcServer := grpc.NewServer()
	dotsServer, err := dotsservergrpc.NewDotsServerGrpc(nodeId, conf)
	if err != nil {
		log.WithError(err).Fatal("Failed to instantiate DoTS server")
	}
	defer dotsServer.Shutdown()
	dotspb.RegisterDecExecServer(grpcServer, dotsServer)

	// Listen.
	if err := grpcServer.Serve(conn); err != nil {
		log.WithError(err).Fatal("Failed to server")
	}
}
