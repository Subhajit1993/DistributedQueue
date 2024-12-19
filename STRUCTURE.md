distributed-queue/
├── cmd/                          # Application entry points
│   ├── node/                     # Node service
│   │   └── main.go              # Node binary entry point
│   └── client/                  # Client CLI tool
│       └── main.go              # Client binary entry point
│
├── pkg/                         # Public packages
│   ├── queue/                   # Queue implementation
│   │   ├── queue.go            # Queue interface and base implementation
│   │   ├── memory.go           # In-memory queue implementation
│   │   └── persistent.go       # Persistent queue implementation
│   │
│   ├── consensus/              # Consensus mechanism
│   │   ├── paxos.go           # Paxos implementation
│   │   ├── leader.go          # Leader election
│   │   ├── proposal.go        # Proposal types and handling
│   │   └── state.go           # Consensus state management
│   │
│   ├── membership/             # Cluster membership
│   │   ├── discovery.go       # Node discovery
│   │   ├── heartbeat.go       # Health checking
│   │   └── registry.go        # Node registry
│   │
│   ├── replication/            # Data replication
│   │   ├── log.go             # Replication log
│   │   ├── sync.go           # Sync mechanisms
│   │   └── recovery.go        # Recovery procedures
│   │
│   └── transport/             # Network communication
│       ├── grpc/             # gRPC implementation
│       │   ├── proto/        # Protocol buffer definitions
│       │   ├── server.go     # gRPC server
│       │   └── client.go     # gRPC client
│       └── http/             # HTTP API
│           ├── handlers/     # HTTP handlers
│           └── middleware/   # HTTP middleware
│
├── internal/                  # Private application code
│   ├── config/               # Configuration management
│   │   └── config.go
│   ├── metrics/              # Metrics collection
│   │   └── metrics.go
│   ├── storage/              # Storage implementations
│   │   ├── wal.go           # Write-ahead log
│   │   └── snapshot.go      # State snapshots
│   └── util/                 # Utility functions
│       └── util.go
│
├── api/                      # API definitions
│   ├── proto/               # Protocol buffer definitions
│   │   └── queue.proto
│   └── swagger/             # REST API specifications
│
├── test/                    # Integration and e2e tests
│   ├── integration/
│   └── e2e/
│
├── deployments/             # Deployment configurations
│   ├── docker/
│   │   └── Dockerfile
│   └── kubernetes/
│       ├── node.yaml
│       └── client.yaml
│
├── docs/                    # Documentation
│   ├── architecture.md
│   ├── consensus.md
│   └── api.md
│
├── scripts/                 # Build and utility scripts
│   ├── build.sh
│   └── test.sh
│
├── go.mod
├── go.sum
├── Makefile
└── README.md