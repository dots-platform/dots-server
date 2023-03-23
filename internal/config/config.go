package config

import (
	"errors"
	"io/ioutil"
	"sort"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	Addr  string `yaml:"addr"`
	Ports []int  `yaml:"ports"`
}

type AppConfig struct {
	Path string `yaml:"path"`
}

type Config struct {
	NodeIds   []string
	NodeRanks map[string]int
	Nodes     map[string]*NodeConfig `yaml:"nodes"`

	Apps           map[string]*AppConfig `yaml:"apps"`
	FileStorageDir string                `yaml:"file_storage_dir"`

	OurNodeId     string
	OurNodeRank   int
	OurNodeConfig *NodeConfig
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

	return &config, nil
}
