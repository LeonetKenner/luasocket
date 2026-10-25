/*=========================================================================*\
 * Epoll implementation for Linux
 * LuaSocket toolkit
 *
 * Replaces poll(2)/select(2) with epoll(7) for the wait primitives.
 * epoll waits in time proportional to the number of *ready* descriptors
 * rather than the total number of watched descriptors, and has no
 * FD_SETSIZE limit, which makes it a good fit for high-concurrency
 * servers on Linux.
 *
 * This file is a drop-in replacement for usocket.c on Linux: it exports
 * the exact same socket_* API, only socket_waitfd() and socket_select()
 * are implemented on top of epoll.
\*=========================================================================*/
#include "luasocket.h"

#include "socket.h"
#include "timeout.h"
#include "pierror.h"

#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/poll.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

/*=========================================================================*\
 * epoll instance shared by the whole process.
 * Created lazily on the first wait call and destroyed on socket_close().
 \*=========================================================================*/
static int epoll_fd = -1;

#define WAITFD_R        1
#define WAITFD_W        2
#define WAITFD_C        (WAITFD_R|WAITFD_W)

/*-------------------------------------------------------------------------*\
 * Make sure an epoll instance exists.
 \*-------------------------------------------------------------------------*/
static int epoll_ensure(void) {
    if (epoll_fd < 0)
        epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    return epoll_fd >= 0;
}

/*-------------------------------------------------------------------------*\
 * Descriptors watched by the most recent socket_select() call, so they can
 * be unregistered before the next call (EPOLLONESHOT removes descriptors
 * that reported an event, but not those that timed out).
 \*-------------------------------------------------------------------------*/
typedef struct {
    t_socket fd;
    short events;
} epoll_watch_t;

static epoll_watch_t *epoll_watch = NULL;
static int epoll_watch_count = 0;
static int epoll_watch_cap = 0;

/* scratch buffer for epoll_wait results */
static struct epoll_event *epoll_events = NULL;
static int epoll_events_cap = 0;

/*-------------------------------------------------------------------------*\
 * Wait for readable/writable/connected socket with timeout
 \*-------------------------------------------------------------------------*/
int socket_waitfd(p_socket ps, int sw, p_timeout tm) {
    int ret;
    double t;
    int ms;
    int mask = 0;

    if (timeout_iszero(tm)) return IO_TIMEOUT;

    if (!epoll_ensure()) {
        /* fall back to poll if epoll is unavailable */
        struct pollfd pfd;
        pfd.fd = *ps;
        pfd.events = 0;
        pfd.revents = 0;
        if (sw & WAITFD_R) pfd.events |= POLLIN;
        if (sw & WAITFD_W) pfd.events |= POLLOUT;
        do {
            t = timeout_getretry(tm) * 1e3;
            ret = poll(&pfd, 1, t >= 0.0 ? (int) t : -1);
        } while (ret == -1 && errno == EINTR);
        if (ret == -1) return errno;
        if (ret == 0) return IO_TIMEOUT;
        if (sw == WAITFD_C && (pfd.revents & (POLLIN|POLLERR))) return IO_CLOSED;
        return IO_DONE;
    }

    /* EPOLLONESHOT removes the descriptor from the interest list after the
     * first event is reported, so we re-register on every call. */
    {
        struct epoll_event ev;
        ev.events = EPOLLONESHOT;
        ev.data.fd = *ps;
        /* clear any stale registration (EPOLL_CTL_DEL on an absent fd is
         * harmless) */
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, *ps, &ev);
        if (sw & WAITFD_R) mask |= EPOLLIN;
        if (sw & WAITFD_W) mask |= EPOLLOUT;
        ev.events |= mask;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, *ps, &ev) < 0)
            return errno;
    }

    struct epoll_event evs[1];
    for ( ;; ) {
        t = timeout_getretry(tm);
        ms = t >= 0.0 ? (int)(t * 1e3) : -1;
        ret = epoll_wait(epoll_fd, evs, 1, ms);
        if (ret == -1) {
            if (errno == EINTR) continue;
            return errno;
        }
        break;
    }
    /* ret == 0 -> timed out (descriptor stays registered, oneshot only
     * removes on a reported event); ret == 1 -> an event was reported and
     * the descriptor was removed from the interest list. */
    if (ret == 0) return IO_TIMEOUT;
    if (sw == WAITFD_C &&
        (evs[0].events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
        return IO_CLOSED;
    return IO_DONE;
}

/*-------------------------------------------------------------------------*\
 * Select with timeout control, implemented with epoll.
 *
 * The fd sets are rebuilt on return to mark the ready descriptors, exactly
 * like select(2), so callers (e.g. src/select.c) keep working unchanged.
 * There is no FD_SETSIZE limit and the kernel only reports ready
 * descriptors.
 \*-------------------------------------------------------------------------*/
int socket_select(t_socket n, fd_set *rfds, fd_set *wfds, fd_set *efds,
        p_timeout tm) {
    int ret, i, nready = 0;
    double t;
    int ms;
    t_socket fd;

    if (!epoll_ensure()) {
        /* fall back to select if epoll is unavailable */
        do {
            struct timeval tv;
            double tt = timeout_getretry(tm);
            tv.tv_sec = (int) tt;
            tv.tv_usec = (int) ((tt - tv.tv_sec) * 1.0e6);
            ret = select(n, rfds, wfds, efds, tt >= 0.0 ? &tv : NULL);
        } while (ret < 0 && errno == EINTR);
        return ret;
    }

    /* 1. unregister descriptors watched by the previous call */
    for (i = 0; i < epoll_watch_count; i++) {
        struct epoll_event ev;
        ev.events = 0;
        ev.data.fd = epoll_watch[i].fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, epoll_watch[i].fd, &ev);
    }
    epoll_watch_count = 0;

    /* 2. build the watch list from the fd sets and register each descriptor */
    for (fd = 0; fd < n; fd++) {
        int events = 0;
        if (rfds && FD_ISSET(fd, rfds)) events |= EPOLLIN;
        if (wfds && FD_ISSET(fd, wfds)) events |= EPOLLOUT;
        if (efds && FD_ISSET(fd, efds)) events |= EPOLLERR;
        if (events == 0) continue;
        events |= EPOLLONESHOT;
        if (epoll_watch_count == epoll_watch_cap) {
            int ncap = epoll_watch_cap ? epoll_watch_cap * 2 : 32;
            epoll_watch_t *nw = (epoll_watch_t *) realloc(epoll_watch,
                    (size_t) ncap * sizeof(*nw));
            if (!nw) return ENOMEM;
            epoll_watch = nw;
            epoll_watch_cap = ncap;
        }
        epoll_watch[epoll_watch_count].fd = fd;
        epoll_watch[epoll_watch_count].events = (short) events;
        epoll_watch_count++;
        {
            struct epoll_event ev;
            ev.events = (uint32_t) events;
            ev.data.fd = fd;
            if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0)
                return errno;
        }
    }

    /* 3. clear the fd sets; they will be repopulated with ready descriptors */
    if (rfds) FD_ZERO(rfds);
    if (wfds) FD_ZERO(wfds);

    /* 4. nothing to watch: just sleep for the timeout (like select(0,...)) */
    if (epoll_watch_count == 0) {
        t = timeout_getretry(tm);
        ms = t >= 0.0 ? (int)(t * 1e3) : -1;
        if (ms > 0) {
            struct timespec req, rem;
            req.tv_sec = ms / 1000;
            req.tv_nsec = (ms % 1000) * 1000000L;
            while (nanosleep(&req, &rem) == -1 && errno == EINTR)
                req = rem;
        }
        return 0;
    }

    /* 5. wait for readiness (retry on EINTR, recomputing remaining time) */
    if (epoll_watch_count > epoll_events_cap) {
        int ncap = epoll_watch_count;
        struct epoll_event *ne = (struct epoll_event *) realloc(epoll_events,
                (size_t) ncap * sizeof(*ne));
        if (!ne) return ENOMEM;
        epoll_events = ne;
        epoll_events_cap = ncap;
    }
    for ( ;; ) {
        t = timeout_getretry(tm);
        ms = t >= 0.0 ? (int)(t * 1e3) : -1;
        ret = epoll_wait(epoll_fd, epoll_events, epoll_watch_count, ms);
        if (ret == -1) {
            if (errno == EINTR) continue;
            return errno;
        }
        break;
    }

    /* 6. EPOLLONESHOT removed each reported descriptor; mark the ready ones */
    nready = ret;
    for (i = 0; i < ret; i++) {
        t_socket rfd = epoll_events[i].data.fd;
        uint32_t rev = epoll_events[i].events;
        if (rfds && (rev & (EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP)))
            FD_SET(rfd, rfds);
        if (wfds && (rev & EPOLLOUT))
            FD_SET(rfd, wfds);
    }
    return nready;
}

/*-------------------------------------------------------------------------*\
 * Initializes module
 \*-------------------------------------------------------------------------*/
int socket_open(void) {
    /* installs a handler to ignore sigpipe or it will crash us */
    signal(SIGPIPE, SIG_IGN);
    epoll_ensure();
    return 1;
}

/*-------------------------------------------------------------------------*\
 * Close module
 \*-------------------------------------------------------------------------*/
int socket_close(void) {
    if (epoll_fd >= 0) {
        close(epoll_fd);
        epoll_fd = -1;
    }
    return 1;
}

/*-------------------------------------------------------------------------*\
 * Close and inutilize socket
 \*-------------------------------------------------------------------------*/
void socket_destroy(p_socket ps) {
    if (*ps != SOCKET_INVALID) {
        close(*ps);
        *ps = SOCKET_INVALID;
    }
}

/*-------------------------------------------------------------------------*\
 * Select with timeout control
 \*-------------------------------------------------------------------------*/
/* (implemented above with epoll) */

/*-------------------------------------------------------------------------*\
 * Creates and sets up a socket
 \*-------------------------------------------------------------------------*/
int socket_create(p_socket ps, int domain, int type, int protocol) {
    *ps = socket(domain, type, protocol);
    if (*ps != SOCKET_INVALID) return IO_DONE;
    else return errno;
}

/*-------------------------------------------------------------------------*\
 * Binds or returns error message
 \*-------------------------------------------------------------------------*/
int socket_bind(p_socket ps, SA *addr, socklen_t len) {
    int err = IO_DONE;
    socket_setblocking(ps);
    if (bind(*ps, addr, len) < 0) err = errno;
    socket_setnonblocking(ps);
    return err;
}

/*-------------------------------------------------------------------------*\
 *
 \*-------------------------------------------------------------------------*/
int socket_listen(p_socket ps, int backlog) {
    int err = IO_DONE;
    if (listen(*ps, backlog)) err = errno;
    return err;
}

void socket_shutdown(p_socket ps, int how) {
    shutdown(*ps, how);
}

/*-------------------------------------------------------------------------*\
 * Connects or returns error message
 \*-------------------------------------------------------------------------*/
int socket_connect(p_socket ps, SA *addr, socklen_t len, p_timeout tm) {
    int err;
    /* avoid calling on closed sockets */
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    /* call connect until done or failed without being interrupted */
    do if (connect(*ps, addr, len) == 0) return IO_DONE;
    while ((err = errno) == EINTR);
    /* if connection failed immediately, return error code */
    if (err != EINPROGRESS && err != EAGAIN) return err;
    /* zero timeout case optimization */
    if (timeout_iszero(tm)) return IO_TIMEOUT;
    /* wait until we have the result of the connection attempt or timeout */
    err = socket_waitfd(ps, WAITFD_C, tm);
    if (err == IO_CLOSED) {
        if (recv(*ps, (char *) &err, 0, 0) == 0) return IO_DONE;
        else return errno;
    } else return err;
}

/*-------------------------------------------------------------------------*\
 * Accept with timeout
 \*-------------------------------------------------------------------------*/
int socket_accept(p_socket ps, p_socket pa, SA *addr, socklen_t *len, p_timeout tm) {
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    for ( ;; ) {
        int err;
        if ((*pa = accept(*ps, addr, len)) != SOCKET_INVALID) return IO_DONE;
        err = errno;
        if (err == EINTR) continue;
        if (err != EAGAIN && err != ECONNABORTED) return err;
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
    /* can't reach here */
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Send with timeout
 \*-------------------------------------------------------------------------*/
int socket_send(p_socket ps, const char *data, size_t count,
        size_t *sent, p_timeout tm)
{
    int err;
    *sent = 0;
    /* avoid making system calls on closed sockets */
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    /* loop until we send something or we give up on error */
    for ( ;; ) {
        long put = (long) send(*ps, data, count, 0);
        /* if we sent anything, we are done */
        if (put >= 0) {
            *sent = put;
            return IO_DONE;
        }
        err = errno;
        /* EPIPE means the connection was closed */
        if (err == EPIPE) return IO_CLOSED;
        /* EPROTOTYPE means the connection is being closed (on Yosemite!)*/
        if (err == EPROTOTYPE) continue;
        /* we call was interrupted, just try again */
        if (err == EINTR) continue;
        /* if failed fatal reason, report error */
        if (err != EAGAIN) return err;
        /* wait until we can send something or we timeout */
        if ((err = socket_waitfd(ps, WAITFD_W, tm)) != IO_DONE) return err;
    }
    /* can't reach here */
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Sendto with timeout
 \*-------------------------------------------------------------------------*/
int socket_sendto(p_socket ps, const char *data, size_t count, size_t *sent,
        SA *addr, socklen_t len, p_timeout tm)
{
    int err;
    *sent = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    for ( ;; ) {
        long put = (long) sendto(*ps, data, count, 0, addr, len);
        if (put >= 0) {
            *sent = put;
            return IO_DONE;
        }
        err = errno;
        if (err == EPIPE) return IO_CLOSED;
        if (err == EPROTOTYPE) continue;
        if (err == EINTR) continue;
        if (err != EAGAIN) return err;
        if ((err = socket_waitfd(ps, WAITFD_W, tm)) != IO_DONE) return err;
    }
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Receive with timeout
 \*-------------------------------------------------------------------------*/
int socket_recv(p_socket ps, char *data, size_t count, size_t *got, p_timeout tm) {
    int err;
    *got = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    if (count == 0) return IO_DONE;
    for ( ;; ) {
        long taken = (long) recv(*ps, data, count, 0);
        if (taken > 0) {
            *got = taken;
            return IO_DONE;
        }
        err = errno;
        if (taken == 0) return IO_CLOSED;
        if (err == EINTR) continue;
        if (err != EAGAIN) return err;
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Recvfrom with timeout
 \*-------------------------------------------------------------------------*/
int socket_recvfrom(p_socket ps, char *data, size_t count, size_t *got,
        SA *addr, socklen_t *len, p_timeout tm) {
    int err;
    *got = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    for ( ;; ) {
        long taken = (long) recvfrom(*ps, data, count, 0, addr, len);
        if (taken > 0) {
            *got = taken;
            return IO_DONE;
        }
        err = errno;
        if (taken == 0) return IO_CLOSED;
        if (err == EINTR) continue;
        if (err != EAGAIN) return err;
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Write with timeout
 *
 * socket_read and socket_write are cut-n-paste of socket_send and socket_recv,
 * with send/recv replaced with write/read. We can't just use write/read
 * in the socket version, because behaviour when size is zero is different.
 \*-------------------------------------------------------------------------*/
int socket_write(p_socket ps, const char *data, size_t count,
        size_t *sent, p_timeout tm)
{
    int err;
    *sent = 0;
    /* avoid making system calls on closed sockets */
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    /* loop until we send something or we give up on error */
    for ( ;; ) {
        long put = (long) write(*ps, data, count);
        /* if we sent anything, we are done */
        if (put >= 0) {
            *sent = put;
            return IO_DONE;
        }
        err = errno;
        /* EPIPE means the connection was closed */
        if (err == EPIPE) return IO_CLOSED;
        /* EPROTOTYPE means the connection is being closed (on Yosemite!)*/
        if (err == EPROTOTYPE) continue;
        /* we call was interrupted, just try again */
        if (err == EINTR) continue;
        /* if failed fatal reason, report error */
        if (err != EAGAIN) return err;
        /* wait until we can send something or we timeout */
        if ((err = socket_waitfd(ps, WAITFD_W, tm)) != IO_DONE) return err;
    }
    /* can't reach here */
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Read with timeout
 * See note for socket_write
 \*-------------------------------------------------------------------------*/
int socket_read(p_socket ps, char *data, size_t count, size_t *got, p_timeout tm) {
    int err;
    *got = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    if (count == 0) return IO_DONE;
    for ( ;; ) {
        long taken = (long) read(*ps, data, count);
        if (taken > 0) {
            *got = taken;
            return IO_DONE;
        }
        err = errno;
        if (taken == 0) return IO_CLOSED;
        if (err == EINTR) continue;
        if (err != EAGAIN) return err;
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
    return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Put socket into blocking mode
 \*-------------------------------------------------------------------------*/
void socket_setblocking(p_socket ps) {
    int flags = fcntl(*ps, F_GETFL, 0);
    flags &= (~(O_NONBLOCK));
    fcntl(*ps, F_SETFL, flags);
}

/*-------------------------------------------------------------------------*\
 * Put socket into non-blocking mode
 \*-------------------------------------------------------------------------*/
void socket_setnonblocking(p_socket ps) {
    int flags = fcntl(*ps, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(*ps, F_SETFL, flags);
}

/*-------------------------------------------------------------------------*\
 * DNS helpers
 \*-------------------------------------------------------------------------*/
int socket_gethostbyaddr(const char *addr, socklen_t len, struct hostent **hp) {
    *hp = gethostbyaddr(addr, len, AF_INET);
    if (*hp) return IO_DONE;
    else if (h_errno) return h_errno;
    else if (errno) return errno;
    else return IO_UNKNOWN;
}

int socket_gethostbyname(const char *addr, struct hostent **hp) {
    *hp = gethostbyname(addr);
    if (*hp) return IO_DONE;
    else if (h_errno) return h_errno;
    else if (errno) return errno;
    else return IO_UNKNOWN;
}

/*-------------------------------------------------------------------------*\
 * Error translation functions
 * Make sure important error messages are standard
 \*-------------------------------------------------------------------------*/
const char *socket_hoststrerror(int err) {
    if (err <= 0) return io_strerror(err);
    switch (err) {
        case HOST_NOT_FOUND: return PIE_HOST_NOT_FOUND;
        default: return hstrerror(err);
    }
}

const char *socket_strerror(int err) {
    if (err <= 0) return io_strerror(err);
    switch (err) {
        case EADDRINUSE: return PIE_ADDRINUSE;
        case EISCONN: return PIE_ISCONN;
        case EACCES: return PIE_ACCESS;
        case ECONNREFUSED: return PIE_CONNREFUSED;
        case ECONNABORTED: return PIE_CONNABORTED;
        case ECONNRESET: return PIE_CONNRESET;
        case ETIMEDOUT: return PIE_TIMEDOUT;
        default: {
            return strerror(err);
        }
    }
}

const char *socket_ioerror(p_socket ps, int err) {
    (void) ps;
    return socket_strerror(err);
}

const char *socket_gaistrerror(int err) {
    if (err == 0) return NULL;
    switch (err) {
        case EAI_AGAIN: return PIE_AGAIN;
        case EAI_BADFLAGS: return PIE_BADFLAGS;
#ifdef EAI_BADHINTS
        case EAI_BADHINTS: return PIE_BADHINTS;
#endif
        case EAI_FAIL: return PIE_FAIL;
        case EAI_FAMILY: return PIE_FAMILY;
        case EAI_MEMORY: return PIE_MEMORY;
        case EAI_NONAME: return PIE_NONAME;
#ifdef EAI_OVERFLOW
        case EAI_OVERFLOW: return PIE_OVERFLOW;
#endif
#ifdef EAI_PROTOCOL
        case EAI_PROTOCOL: return PIE_PROTOCOL;
#endif
        case EAI_SERVICE: return PIE_SERVICE;
        case EAI_SOCKTYPE: return PIE_SOCKTYPE;
        case EAI_SYSTEM: return strerror(errno);
        default: return LUA_GAI_STRERROR(err);
    }
}
