CC ?= cc
CFLAGS ?= -O2 -g
WARNINGS = -std=c11 -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion
SOURCES = src/main.c src/event.c
BIN ?= build/streamgate
ifeq ($(shell uname -s),Darwin)
SANITIZERS ?= undefined
else
SANITIZERS ?= address,undefined
endif

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
	$(MAKE) -B BIN=build/streamgate-sanitize CFLAGS='-O1 -g -fsanitize=$(SANITIZERS) -fno-omit-frame-pointer' LDFLAGS='-fsanitize=$(SANITIZERS)' all
	STREAMGATE_BIN="$(CURDIR)/build/streamgate-sanitize" UBSAN_OPTIONS=halt_on_error=1 python3 -m unittest discover -s tests -v

clean:
	rm -rf build
