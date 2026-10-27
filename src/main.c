#define _DARWIN_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#include "event.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAX_BACKENDS 32
#define BUFFER_SIZE 16384
#define ADMIN_COUNT 16
#define ADMIN_SIZE 32768
#define EVENT_BATCH 256
#define HEALTH_BASE 2
#define ADMIN_BASE (HEALTH_BASE + MAX_BACKENDS)
#define CONN_BASE (ADMIN_BASE + ADMIN_COUNT)

typedef struct { unsigned char *data; size_t start, end; } Buffer;
typedef struct {
    char name[256];
    struct sockaddr_storage addr;
    socklen_t addrlen;
    int fd, active, successes, failures;
    bool healthy;
    uint64_t selected, up_transitions, down_transitions;
    int64_t probe_started, next_probe;
} Backend;
typedef struct {
    int fd[2], backend;
    bool connecting, eof[2], write_closed[2];
    Buffer buffer[2];
    int64_t created, activity;
} Connection;
typedef struct {
    int fd;
    char request[2048];
    size_t request_len, length, sent;
    char *response;
    int64_t started;
} Admin;
typedef struct {
    uint64_t accepted, completed, rejected, connect_errors, io_errors;
    uint64_t idle_timeouts, connect_timeouts, forced_shutdowns, bytes[2];
    uint64_t peak_active, backpressure_events;
    int active;
} Stats;

static EventLoop loop;
static Backend backends[MAX_BACKENDS];
static Connection *connections;
static Admin admins[ADMIN_COUNT];
static Stats stats;
static int backend_count, max_connections = 2048, next_backend, next_connection;
static int listen_fd = -1, admin_fd = -1;
static int connect_timeout = 1000, idle_timeout = 30000, health_interval = 500;
static int health_timeout = 300, health_threshold = 2, drain_timeout = 5000;
static bool least_connections, draining;
static int64_t started, drain_started;
static volatile sig_atomic_t stop_requested;

static int64_t now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}
static void log_event(const char *event, const char *detail) {
    fprintf(stderr, "{\"time_ms\":%" PRId64 ",\"event\":\"%s\",\"detail\":\"%s\"}\n",
            now_ms() - started, event, detail);
}
static void on_signal(int sig) { (void)sig; stop_requested = 1; }
static void die(const char *message) { perror(message); exit(EXIT_FAILURE); }
static int conn_slot(int i, int side) { return CONN_BASE + i * 2 + side; }
static size_t buffered(const Buffer *b) { return b->end - b->start; }
static void close_watched(int slot, int *fd) {
    if (*fd < 0) return;
    event_watch(&loop, slot, -1, 0);
    close(*fd);
    *fd = -1;
}
static int setup_socket(int fd) {
    if (fd < 0) return -1;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
        fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) { close(fd); return -1; }
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    return fd;
}
static int resolve(const char *endpoint, struct sockaddr_storage *out, socklen_t *len) {
    char host[256], service[16];
    const char *port;
    size_t hostlen;
    if (endpoint[0] == '[') {
        const char *end = strchr(endpoint, ']');
        if (!end || end[1] != ':') return -1;
        hostlen = (size_t)(end - endpoint - 1);
        if (hostlen >= sizeof(host)) return -1;
        memcpy(host, endpoint + 1, hostlen);
        port = end + 2;
    } else {
        const char *colon = strrchr(endpoint, ':');
        if (!colon || strchr(endpoint, ':') != colon) return -1;
        hostlen = (size_t)(colon - endpoint);
        if (hostlen >= sizeof(host)) return -1;
        memcpy(host, endpoint, hostlen);
        port = colon + 1;
    }
    host[hostlen] = 0;
    if (!hostlen || !*port || strlen(port) >= sizeof(service)) return -1;
    for (size_t i = 0; i < strlen(endpoint); i++) {
        char c = endpoint[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || strchr(".:-_[]", c))) return -1;
    }
    char *end;
    long number = strtol(port, &end, 10);
    if (*end || number < 1 || number > 65535) return -1;
    snprintf(service, sizeof(service), "%ld", number);
    struct addrinfo hints = {0}, *result = NULL;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    int error = getaddrinfo(host, service, &hints, &result);
    if (error) { fprintf(stderr, "%s: %s\n", host, gai_strerror(error)); return -1; }
    memcpy(out, result->ai_addr, result->ai_addrlen);
    *len = (socklen_t)result->ai_addrlen;
    freeaddrinfo(result);
    return 0;
}
static int listener(const char *endpoint) {
    struct sockaddr_storage addr;
    socklen_t len;
    if (resolve(endpoint, &addr, &len)) { errno = EINVAL; return -1; }
    int fd = setup_socket(socket(addr.ss_family, SOCK_STREAM, 0));
    if (fd < 0) return -1;
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (bind(fd, (struct sockaddr *)&addr, len) < 0 || listen(fd, 512) < 0) {
        int saved = errno; close(fd); errno = saved; return -1;
    }
    return fd;
}
static void set_health(Backend *b, bool up) {
    if (b->healthy == up) return;
    b->healthy = up;
    if (up) b->up_transitions++; else b->down_transitions++;
    log_event(up ? "backend_up" : "backend_down", b->name);
}
static void probe_result(int i, bool success, int64_t now) {
    Backend *b = &backends[i];
    close_watched(HEALTH_BASE + i, &b->fd);
    if (success) {
        b->failures = 0;
        if (b->successes < health_threshold) b->successes++;
        if (b->successes >= health_threshold) set_health(b, true);
    } else {
        b->successes = 0;
        if (b->failures < health_threshold) b->failures++;
        if (b->failures >= health_threshold) set_health(b, false);
    }
    b->next_probe = now + health_interval;
}
static bool socket_connected(int fd) {
    int error = 0;
    socklen_t length = sizeof(error);
    return getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && !error;
}
static void probe_start(int i, int64_t now) {
    Backend *b = &backends[i];
    b->fd = setup_socket(socket(b->addr.ss_family, SOCK_STREAM, 0));
    b->probe_started = now;
    if (b->fd < 0) { probe_result(i, false, now); return; }
    if (connect(b->fd, (struct sockaddr *)&b->addr, b->addrlen) == 0) {
        probe_result(i, true, now);
    } else if (errno == EINPROGRESS) {
        event_watch(&loop, HEALTH_BASE + i, b->fd, EV_WRITE);
    } else probe_result(i, false, now);
}
static int choose_backend(void) {
    int chosen = -1;
    for (int offset = 0; offset < backend_count; offset++) {
        int i = (next_backend + offset) % backend_count;
        if (!backends[i].healthy) continue;
        if (!least_connections) { chosen = i; break; }
        if (chosen < 0 || backends[i].active < backends[chosen].active) chosen = i;
    }
    if (chosen >= 0) next_backend = (chosen + 1) % backend_count;
    return chosen;
}
static void close_connection(int i) {
    Connection *c = &connections[i];
    if (c->fd[0] < 0) return;
    close_watched(conn_slot(i, 0), &c->fd[0]);
    close_watched(conn_slot(i, 1), &c->fd[1]);
    free(c->buffer[0].data);
    free(c->buffer[1].data);
    c->buffer[0] = (Buffer){0};
    c->buffer[1] = (Buffer){0};
    backends[c->backend].active--;
    stats.active--;
    stats.completed++;
}
static void sync_connection(int i) {
    Connection *c = &connections[i];
    if (c->fd[0] < 0) return;
    for (int s = 0; s < 2; s++) {
        int mask = 0;
        if (s == 1 && c->connecting) mask = EV_WRITE;
        else {
            if (!c->eof[s] && buffered(&c->buffer[s]) < BUFFER_SIZE) mask |= EV_READ;
            if (buffered(&c->buffer[1-s])) mask |= EV_WRITE;
        }
        event_watch(&loop, conn_slot(i, s), c->fd[s], mask);
    }
}
static void connection_failed(int i) {
    Connection *c = &connections[i];
    stats.connect_errors++;
    Backend *b = &backends[c->backend];
    b->successes = 0;
    set_health(b, false);
    close_connection(i);
}
static void accept_clients(int64_t now) {
    /* Limit work per tick so bursts cannot starve existing connections. */
    for (int n = 0; n < 128; n++) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                log_event("accept_error", strerror(errno));
            return;
        }
        stats.accepted++;
        int backend = choose_backend();
        if (backend < 0 || stats.active >= max_connections) {
            stats.rejected++; close(fd); continue;
        }
        fd = setup_socket(fd);
        if (fd < 0) { stats.rejected++; continue; }
        int i = next_connection;
        while (connections[i].fd[0] >= 0) i = (i + 1) % max_connections;
        next_connection = (i + 1) % max_connections;
        Connection *c = &connections[i];
        *c = (Connection){.fd = {fd, -1}, .backend = backend, .created = now, .activity = now};
        stats.active++;
        if ((uint64_t)stats.active > stats.peak_active) stats.peak_active = (uint64_t)stats.active;
        backends[backend].active++;
        backends[backend].selected++;
        c->buffer[0].data = malloc(BUFFER_SIZE);
        c->buffer[1].data = malloc(BUFFER_SIZE);
        if (!c->buffer[0].data || !c->buffer[1].data) { stats.io_errors++; close_connection(i); continue; }
        Backend *b = &backends[backend];
        c->fd[1] = setup_socket(socket(b->addr.ss_family, SOCK_STREAM, 0));
        if (c->fd[1] < 0) { connection_failed(i); continue; }
        int result = connect(c->fd[1], (struct sockaddr *)&b->addr, b->addrlen);
        if (result < 0 && errno != EINPROGRESS) { connection_failed(i); continue; }
        c->connecting = result < 0;
        sync_connection(i);
    }
}
static bool stream_read(Connection *c, int side, int64_t now) {
    Buffer *b = &c->buffer[side];
    for (int attempt = 0; attempt < 4; attempt++) {
        size_t size = buffered(b);
        if (size == BUFFER_SIZE) return true;
        if (b->start) { memmove(b->data, b->data + b->start, size); b->start = 0; b->end = size; }
        ssize_t n = recv(c->fd[side], b->data + b->end, BUFFER_SIZE - b->end, 0);
        if (n > 0) {
            b->end += (size_t)n;
            stats.bytes[side] += (uint64_t)n;
            c->activity = now;
            if (buffered(b) == BUFFER_SIZE) stats.backpressure_events++;
        } else if (n == 0) { c->eof[side] = true; return true; }
        else if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
        else if (errno != EINTR) return false;
    }
    return true;
}
static bool stream_write(Connection *c, int side, int64_t now) {
    Buffer *b = &c->buffer[1-side];
    for (int attempt = 0; attempt < 4 && buffered(b); attempt++) {
        ssize_t n = send(c->fd[side], b->data + b->start, buffered(b), 0);
        if (n > 0) { b->start += (size_t)n; c->activity = now; }
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
        else if (n < 0 && errno == EINTR) continue;
        else return false;
    }
    if (!buffered(b)) b->start = b->end = 0;
    return true;
}
static void handle_connection(int i, int side, int mask, int64_t now) {
    Connection *c = &connections[i];
    if (c->fd[0] < 0) return;
    if (side == 1 && c->connecting) {
        if (!socket_connected(c->fd[1])) { connection_failed(i); return; }
        c->connecting = false;
    } else if (mask & EV_ERROR) { stats.io_errors++; close_connection(i); return; }
    if ((mask & EV_READ) && !c->eof[side] && !stream_read(c, side, now)) {
        stats.io_errors++; close_connection(i); return;
    }
    if ((mask & EV_WRITE) && !stream_write(c, side, now)) {
        stats.io_errors++; close_connection(i); return;
    }
    for (int s = 0; s < 2; s++) {
        if (!c->connecting && c->eof[s] && !buffered(&c->buffer[s]) && !c->write_closed[1-s]) {
            shutdown(c->fd[1-s], SHUT_WR);
            c->write_closed[1-s] = true;
        }
    }
    if (c->eof[0] && c->eof[1] && !buffered(&c->buffer[0]) && !buffered(&c->buffer[1])) close_connection(i);
    else sync_connection(i);
}

static void append(char *out, size_t capacity, size_t *length, const char *fmt, ...) {
    if (*length >= capacity) return;
    va_list args;
    va_start(args, fmt);
    int n = vsnprintf(out + *length, capacity - *length, fmt, args);
    va_end(args);
    if (n < 0 || (size_t)n >= capacity - *length) { fprintf(stderr, "metrics overflow\n"); exit(EXIT_FAILURE); }
    *length += (size_t)n;
}
static int healthy_count(void) {
    int n = 0;
    for (int i = 0; i < backend_count; i++) n += backends[i].healthy;
    return n;
}
static size_t metrics(char *out, size_t capacity) {
    size_t n = 0;
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    uint64_t resident = (uint64_t)usage.ru_maxrss;
#ifndef __APPLE__
    resident *= 1024;
#endif
    double cpu = (double)usage.ru_utime.tv_sec + (double)usage.ru_utime.tv_usec / 1e6 +
                 (double)usage.ru_stime.tv_sec + (double)usage.ru_stime.tv_usec / 1e6;
    append(out, capacity, &n, "{\"engine\":\"%s\",\"algorithm\":\"%s\",\"uptime_ms\":%" PRId64
           ",\"draining\":%s,\"active\":%d,\"max_connections\":%d,\"peak_active\":%" PRIu64
           ",\"accepted\":%" PRIu64 ",\"completed\":%" PRIu64 ",\"rejected\":%" PRIu64
           ",\"connect_errors\":%" PRIu64 ",\"io_errors\":%" PRIu64 ",\"idle_timeouts\":%" PRIu64
           ",\"connect_timeouts\":%" PRIu64 ",\"forced_shutdowns\":%" PRIu64
           ",\"client_bytes_read\":%" PRIu64 ",\"backend_bytes_read\":%" PRIu64
           ",\"backpressure_events\":%" PRIu64 ",\"buffer_capacity_bytes\":%" PRIu64
           ",\"max_rss_bytes\":%" PRIu64 ",\"cpu_seconds\":%.6f,\"backends\":[",
           event_engine(), least_connections ? "leastconn" : "roundrobin", now_ms() - started,
           draining ? "true" : "false", stats.active, max_connections, stats.peak_active,
           stats.accepted, stats.completed, stats.rejected, stats.connect_errors, stats.io_errors,
           stats.idle_timeouts, stats.connect_timeouts, stats.forced_shutdowns, stats.bytes[0], stats.bytes[1],
           stats.backpressure_events, (uint64_t)stats.active * BUFFER_SIZE * 2, resident, cpu);
    for (int i = 0; i < backend_count; i++) {
        Backend *b = &backends[i];
        append(out, capacity, &n, "%s{\"address\":\"%s\",\"healthy\":%s,\"active\":%d,\"selected\":%" PRIu64
               ",\"up_transitions\":%" PRIu64 ",\"down_transitions\":%" PRIu64 "}", i ? "," : "",
               b->name, b->healthy ? "true" : "false", b->active, b->selected, b->up_transitions, b->down_transitions);
    }
    append(out, capacity, &n, "]}\n");
    return n;
}
static void close_admin(int i) {
    close_watched(ADMIN_BASE + i, &admins[i].fd);
    free(admins[i].response);
    admins[i].response = NULL;
}
static void admin_reply(int i) {
    Admin *a = &admins[i];
    char body[ADMIN_SIZE - 512];
    size_t length;
    int status = 200;
    if (strncmp(a->request, "GET /metrics HTTP/1.", 20) == 0) length = metrics(body, sizeof(body));
    else if (strncmp(a->request, "GET /health HTTP/1.", 19) == 0)
        length = (size_t)snprintf(body, sizeof(body), "{\"status\":\"alive\"}\n");
    else if (strncmp(a->request, "GET /ready HTTP/1.", 18) == 0) {
        bool ready = !draining && healthy_count() > 0;
        status = ready ? 200 : 503;
        length = (size_t)snprintf(body, sizeof(body), "{\"ready\":%s}\n", ready ? "true" : "false");
    } else {
        status = 404;
        length = (size_t)snprintf(body, sizeof(body), "{\"error\":\"use GET /metrics, /health or /ready\"}\n");
    }
    a->response = malloc(ADMIN_SIZE);
    if (!a->response) { close_admin(i); return; }
    int header = snprintf(a->response, ADMIN_SIZE,
        "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
        status, status == 200 ? "OK" : status == 503 ? "Service Unavailable" : "Not Found", length);
    memcpy(a->response + header, body, length);
    a->length = (size_t)header + length;
    event_watch(&loop, ADMIN_BASE + i, a->fd, EV_WRITE);
}
static void accept_admin(int64_t now) {
    for (int n = 0; n < ADMIN_COUNT; n++) {
        int fd = accept(admin_fd, NULL, NULL);
        if (fd < 0) return;
        fd = setup_socket(fd);
        if (fd < 0) continue;
        int i;
        for (i = 0; i < ADMIN_COUNT; i++) if (admins[i].fd < 0) break;
        if (i == ADMIN_COUNT) { close(fd); continue; }
        admins[i] = (Admin){.fd = fd, .started = now};
        event_watch(&loop, ADMIN_BASE + i, fd, EV_READ);
    }
}
static void handle_admin(int i, int mask) {
    Admin *a = &admins[i];
    if (mask & EV_ERROR) { close_admin(i); return; }
    if (!a->response && (mask & EV_READ)) {
        ssize_t n = recv(a->fd, a->request + a->request_len, sizeof(a->request) - a->request_len - 1, 0);
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) close_admin(i);
            return;
        }
        a->request_len += (size_t)n;
        a->request[a->request_len] = 0;
        if (strstr(a->request, "\r\n\r\n")) admin_reply(i);
        else if (a->request_len == sizeof(a->request) - 1) close_admin(i);
    } else if (a->response && (mask & EV_WRITE)) {
        ssize_t n = send(a->fd, a->response + a->sent, a->length - a->sent, 0);
        if (n > 0) { a->sent += (size_t)n; if (a->sent == a->length) close_admin(i); }
        else if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) close_admin(i);
    }
}
static int number(const char *s) {
    char *end;
    errno = 0;
    long n = strtol(s, &end, 10);
    if (errno || !*s || *end || n < 1 || n > 86400000) {
        fprintf(stderr, "Invalid positive number: %s\n", s); exit(EXIT_FAILURE);
    }
    return (int)n;
}
static void usage(FILE *stream) {
    fprintf(stream,
        "StreamGate — bounded, event-driven TCP load balancer\n"
        "Usage: streamgate --backend HOST:PORT [--backend HOST:PORT ...] [options]\n"
        "  --listen HOST:PORT          default 127.0.0.1:8080\n"
        "  --admin HOST:PORT           default 127.0.0.1:9090 (local JSON HTTP)\n"
        "  --algorithm roundrobin|leastconn\n"
        "  --max-connections N         default 2048, maximum 16384\n"
        "  --connect-timeout-ms N      default 1000\n"
        "  --idle-timeout-ms N         default 30000\n"
        "  --health-interval-ms N      default 500\n"
        "  --health-timeout-ms N       default 300\n"
        "  --health-threshold N        default 2 consecutive results\n"
        "  --drain-timeout-ms N        default 5000\n"
        "  --help\n");
}
int main(int argc, char **argv) {
    const char *listen_address = "127.0.0.1:8080", *admin_address = "127.0.0.1:9090";
    started = now_ms();
    for (int i = 0; i < MAX_BACKENDS; i++) backends[i].fd = -1;
    for (int i = 0; i < ADMIN_COUNT; i++) admins[i].fd = -1;
    for (int i = 1; i < argc; i++) {
        const char *option = argv[i];
        if (!strcmp(option, "--help")) { usage(stdout); return 0; }
        if (i + 1 == argc) { usage(stderr); return 1; }
        const char *value = argv[++i];
        if (!strcmp(option, "--listen")) listen_address = value;
        else if (!strcmp(option, "--admin")) admin_address = value;
        else if (!strcmp(option, "--backend")) {
            if (backend_count == MAX_BACKENDS || strlen(value) >= sizeof(backends[0].name)) {
                fprintf(stderr, "Too many backends or address too long\n"); return 1;
            }
            Backend *b = &backends[backend_count++];
            snprintf(b->name, sizeof(b->name), "%s", value);
            if (resolve(value, &b->addr, &b->addrlen)) { fprintf(stderr, "Invalid backend: %s\n", value); return 1; }
        } else if (!strcmp(option, "--algorithm")) {
            if (strcmp(value, "roundrobin") && strcmp(value, "leastconn")) { fprintf(stderr, "Unknown algorithm\n"); return 1; }
            least_connections = !strcmp(value, "leastconn");
        } else if (!strcmp(option, "--max-connections")) max_connections = number(value);
        else if (!strcmp(option, "--connect-timeout-ms")) connect_timeout = number(value);
        else if (!strcmp(option, "--idle-timeout-ms")) idle_timeout = number(value);
        else if (!strcmp(option, "--health-interval-ms")) health_interval = number(value);
        else if (!strcmp(option, "--health-timeout-ms")) health_timeout = number(value);
        else if (!strcmp(option, "--health-threshold")) health_threshold = number(value);
        else if (!strcmp(option, "--drain-timeout-ms")) drain_timeout = number(value);
        else { fprintf(stderr, "Unknown option: %s\n", option); return 1; }
    }
    if (!backend_count || max_connections > 16384) { usage(stderr); return 1; }
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) < 0) die("getrlimit");
    rlim_t required = (rlim_t)max_connections * 2 + MAX_BACKENDS + ADMIN_COUNT + 32;
    if (limit.rlim_cur < required) {
        limit.rlim_cur = required < limit.rlim_max ? required : limit.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &limit) < 0 || limit.rlim_cur < required) {
            fprintf(stderr, "File descriptor limit too low; need %llu. Reduce --max-connections or raise ulimit -n.\n",
                    (unsigned long long)required); return 1;
        }
    }
    connections = calloc((size_t)max_connections, sizeof(Connection));
    if (!connections) die("calloc");
    for (int i = 0; i < max_connections; i++) connections[i].fd[0] = connections[i].fd[1] = -1;
    if (event_init(&loop, CONN_BASE + max_connections * 2)) die("event_init");
    listen_fd = listener(listen_address);
    if (listen_fd < 0) die("listen");
    admin_fd = listener(admin_address);
    if (admin_fd < 0) die("admin listen");
    event_watch(&loop, 0, listen_fd, EV_READ);
    event_watch(&loop, 1, admin_fd, EV_READ);
    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
    log_event("started", event_engine());
    Event events[EVENT_BATCH];
    for (;;) {
        int64_t now = now_ms();
        if (stop_requested && !draining) {
            draining = true; drain_started = now;
            close_watched(0, &listen_fd);
            log_event("draining", "stopped accepting; finishing active connections");
        }
        if (draining && (!stats.active || now - drain_started >= drain_timeout)) break;
        for (int i = 0; i < backend_count; i++) {
            Backend *b = &backends[i];
            if (b->fd >= 0 && now - b->probe_started >= health_timeout) probe_result(i, false, now);
            if (!draining && b->fd < 0 && now >= b->next_probe) probe_start(i, now);
        }
        for (int i = 0; i < max_connections; i++) {
            Connection *c = &connections[i];
            if (c->fd[0] < 0) continue;
            if (c->connecting && now - c->created >= connect_timeout) {
                stats.connect_timeouts++; connection_failed(i);
            } else if (now - c->activity >= idle_timeout) { stats.idle_timeouts++; close_connection(i); }
        }
        for (int i = 0; i < ADMIN_COUNT; i++)
            if (admins[i].fd >= 0 && now - admins[i].started >= 2000) close_admin(i);
        int count = event_wait(&loop, events, EVENT_BATCH, 50);
        if (count < 0) die("event_wait");
        now = now_ms();
        for (int e = 0; e < count; e++) {
            int slot = events[e].slot, mask = events[e].mask;
            if (loop.watches[slot].fd < 0 || loop.watches[slot].generation != events[e].generation) continue;
            if (slot == 0) { if (!draining && !stop_requested) accept_clients(now); }
            else if (slot == 1) accept_admin(now);
            else if (slot < ADMIN_BASE) {
                int i = slot - HEALTH_BASE;
                if (backends[i].fd >= 0) probe_result(i, socket_connected(backends[i].fd), now);
            } else if (slot < CONN_BASE) handle_admin(slot - ADMIN_BASE, mask);
            else handle_connection((slot - CONN_BASE) / 2, (slot - CONN_BASE) % 2, mask, now);
        }
    }
    for (int i = 0; i < max_connections; i++) {
        if (connections[i].fd[0] >= 0) { stats.forced_shutdowns++; close_connection(i); }
    }
    char final[ADMIN_SIZE];
    metrics(final, sizeof(final));
    fprintf(stderr, "%s", final);
    for (int i = 0; i < backend_count; i++) close_watched(HEALTH_BASE + i, &backends[i].fd);
    for (int i = 0; i < ADMIN_COUNT; i++) if (admins[i].fd >= 0) close_admin(i);
    close_watched(0, &listen_fd);
    close_watched(1, &admin_fd);
    event_destroy(&loop);
    free(connections);
    log_event("stopped", "shutdown complete");
    return 0;
}
