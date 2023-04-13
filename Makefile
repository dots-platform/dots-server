TARGET_DIR = cmd/dotsserver
TARGET = $(TARGET_DIR)/dotsserver
SRC_DIRS = \
	cmd \
	internal
SRCS = $(wildcard \
		$(patsubst %,%/*.go,$(SRC_DIRS)) \
		$(patsubst %,%/*/*.go,$(SRC_DIRS)) \
		$(patsubst %,%/*/*/*.go,$(SRC_DIRS)) \
	)

$(TARGET): $(SRCS)
	go build -o $@ ./$(TARGET_DIR)
