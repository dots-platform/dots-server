# Berkeley DoTS: The Berkeley Decentralized Trust Stack

Distributing trust has become a fundamental principle in building many of today's applications. However, developing and deploying distributed applications remained challenging. Berkeley Dots is a platform for users to easily develop, test, and deploy applications with distributed trust. Here are some example real-world applications with distributed trust that can be built from Berkeley Dots:

### Cryptocurrency wallet custody
People claim ownership to crypto assets and authorize transactions using private keys. If these keys are lost, then the user permanently lose access to their assets. To protect these private keys, cryptocurrency custody service (e.g. Fireblocks) distribute the keys to multiple servers. Each server holds a secret share of the key, and the servers jointly run secure multi-party computation to authorize transactions with the key.


### Distributed PKI
A Distributed Public key infrastructure (PKI) logs clients' public keys on multiple servers. These servers jointly maintain a consistent global view of users' public key. A user can check if his public key is not tampered with by comparing if the public keys stored on these servers are equal to each other.


### Collaborative learning/analytics.
Multiple organizations (e.g. banks, hospitals) wants to jointly train a ML model or performing analytics using their sensitive data. They can use MPC to run collaborative learning/analytics without revealing their sensitive data to each other.


## Getting Started
Our platform can run on MacOS and Linux. Windows is currently not supported. Before you begin, ensure that both Golang and 'make' are installed on your machine.

### 1. Create a workspace
Start by creating a master folder to serve as your workspace, then clone `dots-server` into this workspace using the following steps:

```
mkdir dots
cd dots
git clone https://github.com/dots-platform/dots-server.git
cd dots-server
```

### 2. Configure nodes
To configure all the nodes, specify your settings in the `server_conf.yml` file. By default, the configuration includes two nodes named `node1` and `node2`.


### 2. Start nodes
The `dots-server` repository provides a command for building the `dotsserver` executable and starting the server. Use the following command in your terminal:

```bash
./start-n.sh 0 <N-1>
```

In the command, `<N-1>` represents the total number of servers subtracted by one, or the index of the last server node. For instance, if there are two nodes, you should execute:
```
./start-n.sh 0 1
```

Alternatively, you may start each node individually. To do so, build the `dotsserver` executable using the command:
```
go build -o cmd/dotsserver/dotsserver ./cmd/dotsserver
```

Afterward, start a node using:
```
./cmd/dotsserver/dotsserver -config server_conf.yml -node_id node0
```
In the command, node_id should match one of the nodes specified in `server_conf.yml`.


### 3. Running an example application
You can run the [Secret Key Recovery](https://github.com/dots-platform/skrecovery-app) and [Distributed Signature Signing](https://github.com/dots-platform/signing-app) applications by following the documentations there.



