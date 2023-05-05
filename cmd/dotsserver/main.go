package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/dtrust-project/dotspb/go/dotspb"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dtrust-project/dots-server/internal/config"
	"github.com/dtrust-project/dots-server/internal/dotsservergrpc"
)

var programLogLevel = new(slog.LevelVar)

func main() {
	// Create and parse arguments.
	var configPath string
	var nodeId string
	var logLevel string
	var listenOffset int
	flag.StringVar(&configPath, "config", "", "Path to the config file")
	flag.StringVar(&nodeId, "node_id", "", "ID of the DoTS node")
	flag.StringVar(&logLevel, "log_level", "info", "Log level. One of: fatal, error, warn, info, debug")
	flag.IntVar(&listenOffset, "listen_offset", 0, "Additive offset of the peer and gRPC ports to listen on relative to the the config. Useful for scripting multiple nodes using the same config.")
	flag.Parse()
	if configPath == "" || nodeId == "" {
		flag.Usage()
		return
	}
	switch logLevel {
	case "error":
		programLogLevel.Set(slog.LevelError)
	case "warn":
		programLogLevel.Set(slog.LevelWarn)
	case "info":
		programLogLevel.Set(slog.LevelInfo)
	case "debug":
		programLogLevel.Set(slog.LevelDebug)
	}

	// Configure logger
	handlerOptions := &slog.HandlerOptions{
		Level: programLogLevel,
	}
	slog.SetDefault(slog.New(handlerOptions.NewTextHandler(os.Stderr)))

	// Get config.
	conf, err := config.ReadConfig(configPath, nodeId, listenOffset)
	if err != nil {
		slog.Error("Error reading config", "err", err)
		os.Exit(1)
	}

	// Instantiate server instance.
	dotsServer, err := dotsservergrpc.NewDotsServerGrpc(nodeId, conf)
	if err != nil {
		slog.Error("Failed to instantiate DoTS server", "err", err)
		os.Exit(1)
	}
	defer dotsServer.Shutdown()

	// Spawn gRPC service.
	conn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.GRPCBindAddr, conf.GRPCPort))
	if err != nil {
		slog.Error("Failed to listen", "err", err)
		os.Exit(1)
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
		slog.Error("Failed to serve", "err", err)
		os.Exit(1)
	}
}
