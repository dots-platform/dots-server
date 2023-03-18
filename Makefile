TARGET_DIR = cmd/dotsserver
TARGET = $(TARGET_DIR)/dotsserver
SRC_DIRS = \
	cmd \
	internal \
	protos
SRCS = $(wildcard \
		$(patsubst %,%/*.go,$(SRC_DIRS)) \
		$(patsubst %,%/*/*.go,$(SRC_DIRS)) \
		$(patsubst %,%/*/*/*.go,$(SRC_DIRS)) \
	)
PROTOS_DIR = ../dtrust/platform/protos

$(TARGET): $(SRCS)
	go build -o $@ ./$(TARGET_DIR)

.PHONY: protos
protos:
	protoc \
		--proto_path=$(PROTOS_DIR) \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=Mdec_exec.proto=protos/dotspb \
		--go-grpc_opt=Mdec_exec.proto=protos/dotspb \
		dec_exec.proto
