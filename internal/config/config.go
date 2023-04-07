package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sort"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	Addr  string `yaml:"addr"`
	Ports []int  `yaml:"ports"`

	TLSServerCertFile string `yaml:"tls_server_cert_file"`
	TLSServerKeyFile  string `yaml:"tls_server_key_file"`

	// The parsed x509 cert.
	TLSServerX509Cert *x509.Certificate

	// The parsed TLS cert, including the private key, only if TLSServerKeyFile
	// is set.
	TLSServerTLSCert tls.Certificate
}

type AppConfig struct {
	Path string `yaml:"path"`
}

type Config struct {
	NodeIds   []string
	NodeRanks map[string]int
	Nodes     map[string]*NodeConfig `yaml:"nodes"`

	Apps map[string]*AppConfig `yaml:"apps"`

	FileStorageDir string `yaml:"file_storage_dir"`
	UseTLS         bool   `yaml:"use_tls"`

	OurNodeId     string
	OurNodeRank   int
	OurNodeConfig *NodeConfig
}

func verifyConfig(conf *Config, ourNodeId string) error {
	// Verify config.
	if conf.FileStorageDir == "" {
		return errors.New("Missing file_storage_dir")
	}

	// Verify node configs.
	for nodeId, nodeConfig := range conf.Nodes {
		if nodeConfig.Addr == "" {
			return errors.New("Missing addr from node config")
		}
		if nodeConfig.Ports == nil {
			return errors.New("Missing ports from node config")
		}
		if len(nodeConfig.Ports) != len(conf.Nodes) {
			return errors.New("Number of ports in node config not equal to number of nodes")
		}
		if conf.UseTLS {
			if nodeConfig.TLSServerCertFile == "" {
				return errors.New("Missing tls_server_cert_file from node config when use_tls = true")
			}
			if nodeId == ourNodeId && nodeConfig.TLSServerKeyFile == "" {
				return errors.New("Missing tls_server_key_file from our node config when use_tls = true")
			}
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

	// Load node-specific info.
	for _, nodeConfig := range config.Nodes {
		// Load TLS cert (and key if present) if TLS is enabled.
		if config.UseTLS {
			certPath := path.Join(path.Dir(configPath), nodeConfig.TLSServerCertFile)
			if nodeConfig.TLSServerKeyFile == "" {
				// Load only cert.
				pemBytes, err := os.ReadFile(certPath)
				if err != nil {
					return nil, err
				}
				block, _ := pem.Decode(pemBytes)
				if block == nil {
					return nil, errors.New("No PEM block in certificate: " + nodeConfig.TLSServerCertFile)
				}
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, err
				}
				nodeConfig.TLSServerX509Cert = cert
			} else {
				// Load cert and key.
				keyPath := path.Join(path.Dir(configPath), nodeConfig.TLSServerKeyFile)
				tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
				if err != nil {
					return nil, err
				}
				cert, err := x509.ParseCertificate(tlsCert.Certificate[0])
				if err != nil {
					return nil, err
				}
				nodeConfig.TLSServerTLSCert = tlsCert
				nodeConfig.TLSServerX509Cert = cert
			}
		}
	}

	return &config, nil
}
