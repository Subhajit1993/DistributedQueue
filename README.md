## TODO ##
1. WAL rotation using walOffset
2. Use checksums to verify the data




Core Features to Implement:

Basic queue operations (enqueue/dequeue)
Distributed consensus for queue operations
Leader election
Node membership management
Fault tolerance
Replication for data consistency


System Components:

Distributed Queue High Level DesignClick to open diagram

Component Details:

A. Queue Manager:

Handles basic queue operations (enqueue/dequeue)
Maintains in-memory queue state
Implements queue persistence
Manages queue metadata

B. Consensus Module:

Implements basic Paxos/Raft-like consensus
Handles proposal and acceptance phases
Manages leader election
Ensures consistency across nodes

C. Replication Manager:

Manages data replication across nodes
Handles sync/async replication strategies
Maintains replication logs
Handles recovery and catch-up

D. Membership Manager:

Tracks active nodes in the cluster
Handles node join/leave events
Maintains heartbeat mechanism
Updates cluster state


Implementation Phases:

Phase 1: Basic Structure

Set up basic node structure
Implement queue operations
Basic network communication

Phase 2: Consensus Implementation

Leader election
Basic consensus protocol
Queue operation ordering

Phase 3: Replication

Data replication
Consistency management
Failure handling

Phase 4: Membership & Scaling

Node membership
Dynamic scaling
Load balancing


Technical Considerations:

A. Communication Protocol:

gRPC for inter-node communication
HTTP/REST for client interactions
Protocol buffers for serialization

B. Storage:

In-memory queue with disk persistence
WAL (Write-Ahead Log) for durability
Snapshot mechanism for state recovery

C. Consistency:

Strong consistency model
Sequential consistency for queue operations
Quorum-based consensus

D. Monitoring & Debugging:

Metrics collection
Distributed tracing
Log aggregation


Failure Scenarios to Handle:


Node failures
Network partitions
Split-brain scenarios
Message losses
Leader failures

## Leader Responsibilities ##
<b>Leader's Responsibilities:</b>

1. Handles all write operations (Enqueue, Dequeue, Ack, Nack)
2. Ensures consistency by ordering all operations
3. Replicates operations to followers
4. Maintains the source of truth for queue state
5. Sends regular heartbeats to followers to prove it's alive


## Follower Responsibilities ##
<b>Follower's Responsibilities:</b>
1. Accept and store replicated data from leader
2. Forward client requests to the leader
3. Monitor leader's heartbeat
4. Handle read operations (if configured for read scalability)
5. Participate in leader election if leader fails
6. Forward write requests to the leader



# Start first node
go run main.go -node node-1 -port 50051

# Start second node
go run main.go -node node-2 -port 50052 -peers localhost:50051

# Start third node
go run main.go -node node-3 -port 50053 -peers localhost:50051,localhost:50052
