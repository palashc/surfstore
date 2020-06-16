
# surfstore
SurfStore is a collaborative, fault-tolerant file storage service. 
 - Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. 
 - Clients accessing SurfStore “see” a consistent set of updates to files. 
 - SurfStore uses leader election and log replication from the [Raft](https://raft.github.io/) consensus protocol to keep a consistent state across servers.
 - SurfStore uses _versions_ to detect conflicts. It uses _first-write-wins_ semantics. Clients whose updates conflict, must sync again and retry.

## Components
1. **BlockStore** - The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. BlockStore stores these blocks, and when given an identifier, retrieves and returns the appropriate block.
2. **MetadataStore** - The MetadataStore service holds the mapping of filenames to blocks. It is implemented as a replicated state machine to ensure consistency and fault tolerance. 

## Usage
1. Create a configuration file with the number of metadata servers and their host:port information.
```bash
> cat config.txt
M: 5
metadata0: localhost:9000
metadata1: localhost:9001
metadata2: localhost:9002
metadata3: localhost:9003
metadata4: localhost:9006
```
2. Start atleast _majority_ of the servers.
```bash
> ./run-server config.txt {i}
Attempting to start XML-RPC Server...
Started successfully.
Accepting requests. (Halt program to stop.)
```
3. User-1 creates some data and syncs with a server using a block size.
```bash
> ls /user1
> 
> cp ~/dog.jpg /user1
> ./run-client {server_host:port} /user1 1024
```
4. User-2 makes her own changes and syncs to see `dog.jpg`
```bash
> ls /user2
> 
> cp ~/cat.jpg /user2
> ./run-client {server_host:port} /user2 1024
> ls /user2
dog.jpg cat.jpg
```
