Repository for Arboreal, a generic solution for data management in the edge, that provides
causal+ consistency while scaling for a large number of edge nodes.

The Main class initializes 5 protocols, where all the important logic is. The protocols are:
- **manager.Manager.kt**: The manager protocol contains the logic to bootstrap the initial tree, whose layout can be
provided as a file, can be generated randomly, or can use a heuristic based on the location of edge nodes. It uses
HyParView to collect information about existing nodes.
- **hyparfllod.HyParFlood.kt**: The HyParFlood protocol is a gossip-based protocol that uses HyParView maintain a fully connected
network overlay across all edge nodes. It uses a flooding mechanism that allows node to disseminate information to
all other nodes in the network.
- **tree.Tree.kt**: Is the main protocol that implements most of the features of Arboreal, including maintaing the tree structure, handling faults,
propagating operations and object replica requests, etc.
- **storage.Storage.kt**: The storage protocol stores the data objects themselves (the storage engine can be changed,
we use a simple in-memory key-value store). It also serves as the interface between the Tree protocol and the ClientProxy,
resorting to the Tree to request data objects from other nodes when needed.
- **proxy.ClientProxy.kt**: The client proxy is simply the interface for client to interact with the system. It
maintains a list of clients, and forwards requests to the Storage protocol.

The client logic for Arboreal can be found in https://gihub.com/pfouto/edge-client, while the scripts to run the
experiments in the paper, along with simple instructions to run Arboreal, can be found in https://github.com/pfouto/edge-exps.