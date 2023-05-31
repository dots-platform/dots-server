# Berkeley DoTS: The Berkeley Decentralized Trust Stack

Distributing trust has become a fundamental principle in building many of today's applications. However, developing and deploying distributed applications remained challenging. Berkeley Dots is a platform for users to easily develop, test, and deploy applications with distributed trust. Here are some example real-world applications with distributed trust that can be built from Berkeley Dots:

### Cryptocurrency wallet custody
People claim ownership to crypto assets and authorize transactions using private keys. If these keys are lost, then the user permanently lose access to their assets. To protect these private keys, cryptocurrency custody service (e.g. Fireblocks) distribute the keys to multiple servers. Each server holds a secret share of the key, and the servers jointly run secure multi-party computation to authorize transactions with the key.


### Distributed PKI
A Distributed Public key infrastructure (PKI) logs clients' public keys on multiple servers. These servers jointly maintain a consistent global view of users' public key. A user can check if his public key is not tampered with by comparing if the public keys stored on these servers are equal to each other.


### Collaborative learning/analytics.
Multiple organizations (e.g. banks, hospitals) wants to jointly train a ML model or performing analytics using their sensitive data. They can use MPC to run collaborative learning/analytics without revealing their sensitive data to each other.


## Getting Started
Our platform can run on MacOS and Linux. Windows is currently not supported.


### 1. Configure applications
In `server_conf.yml`, you can specify configs for all the nodes. The default config specifies two nodes called `node1` and `node2`.  


### 2. Start servers
Run the following command in one terminal in the `dots-server` repo.

```bash
./start-n.sh 0 <N-1>
```

Where `<N-1>` is the number of servers minus one, or the index of the last server node.


### 3. Running an example application
You can run the [Secret Key Recovery](https://github.com/dots-platform/skrecovery-app) and [Distributed Signature Signing](https://github.com/dots-platform/signing-app) applications by following the documentations there.
