CC ?= cc
WARN := -Wall -Wextra -Wpedantic -Wshadow -Werror
BASE := -std=c11 -Isrc $(WARN)

SERVER_SRC := src/server.c src/buffer.c src/topics.c
LOADGEN_SRC := bench/loadgen.c src/buffer.c
HEADERS := $(wildcard src/*.h)

.PHONY: all release debug asan test bench netem clean

all: release

release: build/release/pubsub-server build/release/loadgen
debug: build/debug/pubsub-server
asan: build/asan/pubsub-server

build/release/pubsub-server: $(SERVER_SRC) $(HEADERS) | build/release
	$(CC) $(BASE) -O2 -g -o $@ $(SERVER_SRC)

build/release/loadgen: $(LOADGEN_SRC) $(HEADERS) | build/release
	$(CC) $(BASE) -O2 -g -o $@ $(LOADGEN_SRC)

build/debug/pubsub-server: $(SERVER_SRC) $(HEADERS) | build/debug
	$(CC) $(BASE) -O0 -g3 -o $@ $(SERVER_SRC)

build/asan/pubsub-server: $(SERVER_SRC) $(HEADERS) | build/asan
	$(CC) $(BASE) -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -o $@ $(SERVER_SRC)

build/release build/debug build/asan:
	mkdir -p $@

test: release
	python3 -m pytest -q tests

bench: release
	scripts/bench.sh

netem: release
	scripts/netem.sh

clean:
	rm -rf build results
