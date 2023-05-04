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
	Addr  string `yaml:"addr"`
	Ports []int  `yaml:"ports"`

	PeerTLSCertFile string `yaml:"peer_tls_cert_file"`
	PeerTLSCert     *x509.Certificate
}

type AppConfig struct {
	Path string `yaml:"path"`
}

type Config struct {
	PeerSecurity       PeerSecurity `yaml:"peer_security"`
	PeerTLSCertFile    string       `yaml:"peer_tls_cert_file"`
	PeerTLSCertKeyFile string       `yaml:"peer_tls_cert_key_file"`
	PeerTLSCert        tls.Certificate

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
		if nodeConfig.Ports == nil {
			return errors.New("Missing ports from node config")
		}
		if len(nodeConfig.Ports) != len(conf.Nodes) {
			return errors.New("Number of ports in node config not equal to number of nodes")
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

func ReadConfig(configPath string, ourNodeId string) (*Config, error) {
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

	// If there is only one node with more than one port and all others have
	// one, infer the other ports.
	maxPorts := 0
	maxPortsNodeId := ""
	for nodeId, nodeConfig := range config.Nodes {
		if len(nodeConfig.Ports) == 0 {
			return nil, errors.New("All node configs must have at least one port")
		}
		if len(nodeConfig.Ports) > 1 {
			if maxPorts > 1 && len(nodeConfig.Ports) != maxPorts {
				return nil, errors.New("All node configs must have the same number of ports or exactly one port")
			} else {
				maxPorts = len(nodeConfig.Ports)
				maxPortsNodeId = nodeId
			}
		}
	}
	if maxPorts > 1 {
		for _, nodeConfig := range config.Nodes {
			if len(nodeConfig.Ports) == 1 {
				portBase := nodeConfig.Ports[0]
				nodeConfig.Ports = make([]int, maxPorts)
				nodeConfig.Ports[0] = portBase
				for i := 1; i < maxPorts; i++ {
					nodeConfig.Ports[i] = portBase + config.Nodes[maxPortsNodeId].Ports[i] - config.Nodes[maxPortsNodeId].Ports[0]
				}
			}
		}
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
		// Load TLS cert (and key if present) if TLS is enabled.
		if config.PeerSecurity == PeerSecurityTLS {
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

	return &config, nil
}
