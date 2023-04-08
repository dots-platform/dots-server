package main

import (
	"crypto/tls"
	"flag"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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

	// Instantiate server instance.
	dotsServer, err := dotsservergrpc.NewDotsServerGrpc(nodeId, conf)
	if err != nil {
		log.WithError(err).Fatal("Failed to instantiate DoTS server")
	}
	defer dotsServer.Shutdown()

	// Spawn gRPC service.
	conn, err := net.Listen("tcp", conf.OurNodeConfig.Addr)
	if err != nil {
		log.WithError(err).Fatal("Failed to listen")
	}
	grpcOpts := []grpc.ServerOption{}
	if conf.GRPCSecurity == config.GRPCSecurityTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{conf.GRPCTLSCert},
		}
		tlsCreds := credentials.NewTLS(tlsConfig)
		grpcOpts = append(grpcOpts, grpc.Creds(tlsCreds))
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	dotspb.RegisterDecExecServer(grpcServer, dotsServer)

	// Listen.
	if err := grpcServer.Serve(conn); err != nil {
		log.WithError(err).Fatal("Failed to server")
	}
}
