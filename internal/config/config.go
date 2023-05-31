// Copyright 2023 The Dots Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"

	"gopkg.in/yaml.v3"
)

type PeerSecurity string

const (
	PeerSecurityNone PeerSecurity = "none"
	PeerSecurityTLS               = "tls"
)

type GRPCSecurity string

const (
	GRPCSecurityNone GRPCSecurity = "none"
	GRPCSecurityTLS               = "tls"
)

type NodeConfig struct {
	Addr string `yaml:"addr"`

	PeerTLSCertFile string `yaml:"peer_tls_cert_file"`
	PeerTLSCert     *x509.Certificate
}

type AppConfig struct {
	Path string `yaml:"path"`
}

type Config struct {
	PeerBindAddr       string       `yaml:"peer_bind_addr"`
	PeerPort           int          `yaml:"peer_port"`
	PeerSecurity       PeerSecurity `yaml:"peer_security"`
	PeerTLSCertFile    string       `yaml:"peer_tls_cert_file"`
	PeerTLSCertKeyFile string       `yaml:"peer_tls_cert_key_file"`
	PeerTLSCert        tls.Certificate

	GRPCBindAddr       string       `yaml:"grpc_bind_addr"`
	GRPCPort           int          `yaml:"grpc_port"`
	GRPCSecurity       GRPCSecurity `yaml:"grpc_security"`
	GRPCTLSCertFile    string       `yaml:"grpc_tls_cert_file"`
	GRPCTLSCertKeyFile string       `yaml:"grpc_tls_cert_key_file"`
	GRPCTLSCert        tls.Certificate

	FileStorageDir string `yaml:"file_storage_dir"`

	Nodes     map[string]*NodeConfig `yaml:"nodes"`
	NodeIds   []string
	NodeRanks map[string]int

	Apps map[string]*AppConfig `yaml:"apps"`

	OurNodeId     string
	OurNodeRank   int
	OurNodeConfig *NodeConfig
}

func verifyConfig(conf *Config, ourNodeId string) error {
	// Verify config.

	if conf.FileStorageDir == "" {
		return errors.New("Missing file_storage_dir")
	}

	switch conf.PeerSecurity {
	case "":
		return errors.New("Missing peer_security")
	case PeerSecurityNone:
	case PeerSecurityTLS:
		if conf.PeerTLSCertFile == "" {
			return errors.New("Missing peer_tls_cert_file when peer_security = tls")
		}
		if conf.PeerTLSCertKeyFile == "" {
			return errors.New("Missing peer_tls_cert_key_file when peer_security = tls")
		}
	default:
		return fmt.Errorf("Invalid value for peer_security: %s", conf.PeerSecurity)
	}

	switch conf.GRPCSecurity {
	case "":
		return errors.New("Missing grpc_security")
	case GRPCSecurityNone:
	case GRPCSecurityTLS:
		if conf.GRPCTLSCertFile == "" {
			return errors.New("Missing grpc_tls_cert_file when grpc_security = tls")
		}
		if conf.GRPCTLSCertKeyFile == "" {
			return errors.New("Missing grpc_tls_cert_key_file when grpc_security = tls")
		}
	default:
		return fmt.Errorf("Invalid value for grpc_security: %s", conf.GRPCSecurity)
	}

	// Verify node configs.
	for _, nodeConfig := range conf.Nodes {
		if nodeConfig.Addr == "" {
			return errors.New("Missing addr from node config")
		}
	}

	// Verify app configs.
	for _, appConfig := range conf.Apps {
		if appConfig.Path == "" {
			return errors.New("Missing path from app config")
		}
	}

	return nil
}

func ReadConfig(configPath string, ourNodeId string, listenOffset int) (*Config, error) {
	// Read config bytes.
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	// Unmarshal config.
	var config Config
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return nil, err
	}

	// Verify config.
	if err := verifyConfig(&config, ourNodeId); err != nil {
		return nil, err
	}

	// Generate ranks for node IDs.
	config.NodeIds = make([]string, 0, len(config.Nodes))
	for nodeId := range config.Nodes {
		config.NodeIds = append(config.NodeIds, nodeId)
	}
	sort.Strings(config.NodeIds)
	config.NodeRanks = make(map[string]int)
	for i, nodeId := range config.NodeIds {
		config.NodeRanks[nodeId] = i
	}

	// Get our node rank and config.
	var ok bool
	config.OurNodeId = ourNodeId
	config.OurNodeRank, ok = config.NodeRanks[ourNodeId]
	if !ok {
		return nil, errors.New("Node ID not present in config nodes")
	}
	config.OurNodeConfig = config.Nodes[ourNodeId]

	// Load peer cert if necessary.
	if config.PeerSecurity == PeerSecurityTLS {
		certPath := path.Join(path.Dir(configPath), config.PeerTLSCertFile)
		keyPath := path.Join(path.Dir(configPath), config.PeerTLSCertKeyFile)
		tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		config.PeerTLSCert = tlsCert
	}

	// Load GRPC cert if necessary.
	if config.GRPCSecurity == GRPCSecurityTLS {
		certPath := path.Join(path.Dir(configPath), config.GRPCTLSCertFile)
		keyPath := path.Join(path.Dir(configPath), config.GRPCTLSCertKeyFile)
		tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		config.GRPCTLSCert = tlsCert
	}

	// Load node-specific info.
	for _, nodeConfig := range config.Nodes {
		// Load TLS cert (and key if present) if TLS is enabled and certificate
		// path is present.
		if config.PeerSecurity == PeerSecurityTLS && nodeConfig.PeerTLSCertFile != "" {
			certPath := path.Join(path.Dir(configPath), nodeConfig.PeerTLSCertFile)
			pemBytes, err := os.ReadFile(certPath)
			if err != nil {
				return nil, err
			}
			block, _ := pem.Decode(pemBytes)
			if block == nil {
				return nil, errors.New("No PEM block in certificate: " + nodeConfig.PeerTLSCertFile)
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}
			nodeConfig.PeerTLSCert = cert
		}
	}

	// Apply listen offset.
	config.PeerPort += listenOffset
	config.GRPCPort += listenOffset

	return &config, nil
}
