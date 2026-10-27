CC ?= cc
CFLAGS ?= -O2 -g
WARNINGS = -std=c11 -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion
SOURCES = src/main.c src/event.c
BIN ?= build/streamgate

.PHONY: all clean test demo benchmark sanitize
all: $(BIN)

$(BIN): $(SOURCES) src/event.h
	mkdir -p $(dir $(BIN))
	$(CC) $(CFLAGS) $(WARNINGS) $(SOURCES) -o $(BIN) $(LDFLAGS)

test: all
	python3 -m unittest discover -s tests -v

demo: all
	python3 tools/lab.py demo

benchmark: all
	python3 tools/lab.py benchmark --clients 100 --duration 5

sanitize:
	$(MAKE) BIN=build/streamgate-asan CFLAGS='-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer' LDFLAGS='-fsanitize=address,undefined' all
	STREAMGATE_BIN=$(CURDIR)/build/streamgate-asan python3 -m unittest discover -s tests -v

clean:
	rm -rf build
