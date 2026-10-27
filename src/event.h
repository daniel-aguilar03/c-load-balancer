#ifndef EVENT_H
#define EVENT_H
#include <stdint.h>
#define EV_READ 1
#define EV_WRITE 2
#define EV_ERROR 4
typedef struct { int fd, mask; uint32_t generation; } Watch;
typedef struct { int slot, mask; uint32_t generation; } Event;
typedef struct { int fd, capacity; Watch *watches; void *scratch; } EventLoop;
int event_init(EventLoop *loop, int capacity);
void event_watch(EventLoop *loop, int slot, int fd, int mask);
int event_wait(EventLoop *loop, Event *events, int count, int timeout_ms);
void event_destroy(EventLoop *loop);
const char *event_engine(void);
#endif
