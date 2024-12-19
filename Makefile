# Project configuration
PROJECT_NAME := distributed-queue
GO := go
PROTOC := protoc

# Path configurations for nested directory structure
# Base directories
API_DIR := api
PROTO_DIR := $(API_DIR)/proto
PB_DIR := $(PROTO_DIR)/pb

# Output directories for generated files
PROTO_OUT_QUEUE := $(PB_DIR)/queue
PROTO_OUT_RAFT := $(PB_DIR)/raft

# Source proto files
PROTO_FILE_QUEUE := $(PROTO_DIR)/queue.proto
PROTO_FILE_RAFT := $(PROTO_DIR)/raft.proto

# Output files that will be generated
PROTO_GEN_QUEUE := $(PROTO_OUT_QUEUE)/queue.pb.go
PROTO_GEN_RAFT := $(PROTO_OUT_RAFT)/raft.pb.go

# Create necessary directories
$(PROTO_OUT_QUEUE):
	mkdir -p $(PROTO_OUT_QUEUE)

$(PROTO_OUT_RAFT):
	mkdir -p $(PROTO_OUT_RAFT)

# Generate Go files from queue.proto
# The paths are set to maintain the desired nested structure
$(PROTO_GEN_QUEUE): $(PROTO_FILE_QUEUE) | $(PROTO_OUT_QUEUE)
	$(PROTOC) \
		--go_out=$(PROTO_DIR) \
		--go-grpc_out=$(PROTO_DIR) \
		$<

# Generate Go files from raft.proto
# Similar structure as queue.proto generation
$(PROTO_GEN_RAFT): $(PROTO_FILE_RAFT) | $(PROTO_OUT_RAFT)
	$(PROTOC) \
		--go_out=$(PROTO_DIR) \
		--go-grpc_out=$(PROTO_DIR) \
		$<

# Main target to generate all proto files
proto: $(PROTO_GEN_QUEUE) $(PROTO_GEN_RAFT)
	@echo "Protobuf files have been generated in the nested pb directory structure!"

# Clean generated files
clean:
	rm -rf $(PB_DIR)
	@echo "Cleaned up generated protobuf files."

# Default target
all: proto