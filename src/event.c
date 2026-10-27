#include "event.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/epoll.h>
#else
#include <poll.h>
#endif

int event_init(EventLoop *l, int capacity) {
    l->capacity = capacity;
    l->watches = calloc((size_t)capacity, sizeof(Watch));
    if (!l->watches) return -1;
    for (int i = 0; i < capacity; i++) l->watches[i].fd = -1;
#ifdef __linux__
    l->fd = epoll_create1(EPOLL_CLOEXEC);
    l->scratch = calloc((size_t)capacity, sizeof(struct epoll_event));
#else
    l->fd = -1;
    l->scratch = calloc((size_t)capacity, sizeof(struct pollfd));
#endif
    if (!l->scratch) return -1;
#ifdef __linux__
    if (l->fd < 0) return -1;
#endif
    return 0;
}

void event_watch(EventLoop *l, int slot, int fd, int mask) {
    if (fd >= 0 && !mask) fd = -1; /* Suspend sockets with no useful work. */
    Watch *w = &l->watches[slot];
    if (w->fd == fd && w->mask == mask) return;
#ifdef __linux__
    int operation = EPOLL_CTL_MOD;
    if (w->fd != fd) {
        if (w->fd >= 0) epoll_ctl(l->fd, EPOLL_CTL_DEL, w->fd, NULL);
        w->generation++;
        operation = EPOLL_CTL_ADD;
    }
    if (fd >= 0) {
        struct epoll_event e = {0};
        e.events = 0;
        if (mask & EV_READ) e.events |= EPOLLIN | EPOLLRDHUP;
        if (mask & EV_WRITE) e.events |= EPOLLOUT;
        e.data.u64 = ((uint64_t)w->generation << 32) | (uint32_t)slot;
        if (epoll_ctl(l->fd, operation, fd, &e) < 0) {
            perror("epoll_ctl");
            exit(EXIT_FAILURE);
        }
    }
#else
    if (w->fd != fd) w->generation++;
#endif
    w->fd = fd;
    w->mask = mask;
}

int event_wait(EventLoop *l, Event *out, int count, int timeout) {
#ifdef __linux__
    struct epoll_event *events = l->scratch;
    int n = epoll_wait(l->fd, events, count, timeout), used = 0;
    if (n < 0) return errno == EINTR ? 0 : -1;
    for (int i = 0; i < n; i++) {
        int slot = (int)(uint32_t)events[i].data.u64;
        uint32_t generation = (uint32_t)(events[i].data.u64 >> 32);
        if (l->watches[slot].fd < 0 || l->watches[slot].generation != generation) continue;
        int mask = 0;
        if (events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) mask |= EV_READ;
        if (events[i].events & EPOLLOUT) mask |= EV_WRITE;
        if (events[i].events & EPOLLERR) mask |= EV_ERROR;
        out[used++] = (Event){slot, mask, generation};
    }
    return used;
#else
    struct pollfd *fds = l->scratch;
    for (int i = 0; i < l->capacity; i++) {
        fds[i] = (struct pollfd){.fd = l->watches[i].fd};
        if (l->watches[i].mask & EV_READ) fds[i].events |= POLLIN;
        if (l->watches[i].mask & EV_WRITE) fds[i].events |= POLLOUT;
    }
    int n = poll(fds, (nfds_t)l->capacity, timeout), used = 0;
    if (n < 0) return errno == EINTR ? 0 : -1;
    for (int i = 0; i < l->capacity && used < count; i++) {
        if (!fds[i].revents) continue;
        int mask = 0;
        if (fds[i].revents & (POLLIN | POLLHUP)) mask |= EV_READ;
        if (fds[i].revents & POLLOUT) mask |= EV_WRITE;
        if (fds[i].revents & (POLLERR | POLLNVAL)) mask |= EV_ERROR;
        out[used++] = (Event){i, mask, l->watches[i].generation};
    }
    return used;
#endif
}

void event_destroy(EventLoop *l) {
    if (l->fd >= 0) close(l->fd);
    free(l->watches);
    free(l->scratch);
}
const char *event_engine(void) {
#ifdef __linux__
    return "epoll";
#else
    return "poll";
#endif
}
