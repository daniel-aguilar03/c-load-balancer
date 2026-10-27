# Build/test stages use the same Debian Linux toolchain as the shipped binary.
FROM python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc make libc6-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Makefile ./
COPY src ./src
RUN make

FROM build AS test
COPY tools ./tools
COPY tests ./tests
RUN make test && make sanitize
CMD ["python3", "tools/lab.py", "demo"]

FROM debian:bookworm-slim AS runtime
COPY --from=build /app/build/streamgate /usr/local/bin/streamgate
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/streamgate"]
CMD ["--listen", "0.0.0.0:8080", "--admin", "0.0.0.0:9090", "--backend", "backend-a:9001", "--backend", "backend-b:9001", "--backend", "backend-c:9001"]
