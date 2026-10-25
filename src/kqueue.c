/*=========================================================================*\
 * kqueue implementation for FreeBSD
 * LuaSocket toolkit
 *
 * Replaces select(2)/poll(2) with kqueue(2) for the blocking wait
 * primitives. kqueue reports only ready descriptors, so its cost is
 * proportional to the number of events, not the total watch size, and
 * it has no FD_SETSIZE limit.
 *
 * This file is a drop-in replacement for usocket.c on FreeBSD: it exports
 * the exact same socket_* API, only socket_waitfd() and socket_select()
 * are implemented on top of kqueue.
\*=========================================================================*/
#include "luasocket.h"

#include "socket.h"
#include "timeout.h"
#include "pierror.h"

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

/*=========================================================================*\
 * kqueue instance shared by the whole process.
 * Created lazily on the first wait call and destroyed on socket_close().
 \*=========================================================================*/
static int kq_fd = -1;

#define WAITFD_R        1
#define WAITFD_W        2
#define WAITFD_C        (WAITFD_R|WAITFD_W)

/*-------------------------------------------------------------------------*\
 * Descriptors watched by the most recent socket_select() call. With
 * EV_ONESHOT the kernel removes a descriptor after reporting an event,
 * but descriptors that timed out stay registered, so we delete them
 * before re-registering on the next call.
 \*-------------------------------------------------------------------------*/
typedef struct {
    int ident;
    short filter;
} kq_watch_t;

static kq_watch_t *kq_watch = NULL;
static int kq_watch_count = 0;
static int kq_watch_cap = 0;

/*-------------------------------------------------------------------------*\
 * Make sure a kqueue instance exists.
 \*-------------------------------------------------------------------------*/
static int kqueue_ensure(void) {
    if (kq_fd < 0)
        kq_fd = kqueue();
    return kq_fd >= 0;
}

/*-------------------------------------------------------------------------*\
 * Wait for readable/writable/connected socket with timeout
 \*-------------------------------------------------------------------------*/
int socket_waitfd(p_socket ps, int sw, p_timeout tm) {
    int ret;
    double t;
    struct timespec ts;
    struct kevent ev[2];
    int n = 0, i;
    int flags = EV_ADD | EV_ENABLE | EV_ONESHOT;

    if (timeout_iszero(tm)) return IO_TIMEOUT;  /* optimize timeout == 0 case */
    if (!kqueue_ensure()) {
        /* fall back to select if kqueue is unavailable */
        fd_set rfds, wfds, *rp = NULL, *wp = NULL;
        struct timeval tv, *tp = NULL;
        if (sw & WAITFD_R) { FD_ZERO(&rfds); FD_SET(*ps, &rfds); rp = &rfds; }
        if (sw & WAITFD_W) { FD_ZERO(&wfds); FD_SET(*ps, &wfds); wp = &wfds; }
        if ((t = timeout_getretry(tm)) >= 0.0) {
            tv.tv_sec = (int) t;
            tv.tv_usec = (int) ((t - tv.tv_sec) * 1.0e6);
            tp = &tv;
        }
        ret = select(*ps + 1, rp, wp, NULL, tp);
        if (ret == -1) return errno;
        if (ret == 0) return IO_TIMEOUT;
        if (sw == WAITFD_C && FD_ISSET(*ps, &rfds)) return IO_CLOSED;
        return IO_DONE;
    }

    if (sw & WAITFD_R) {
        EV_SET(&ev[n], *ps, EVFILT_READ, flags, 0, 0, NULL);
        n++;
    }
    if (sw & WAITFD_W) {
        EV_SET(&ev[n], *ps, EVFILT_WRITE, flags, 0, 0, NULL);
        n++;
    }

    t = timeout_getretry(tm);
    ts.tv_sec = (long) t;
    ts.tv_nsec = (long) ((t - ts.tv_sec) * 1.0e9);

    /* register interest and wait in a single kevent call */
    ret = kevent(kq_fd, ev, n, ev, n, t >= 0.0 ? &ts : NULL);
    if (ret == -1) return errno;
    if (ret == 0) return IO_TIMEOUT;

    for (i = 0; i < ret; i++) {
        if (ev[i].flags & EV_ERROR) {
            /* kernel reports the errno in ev[i].data */
            return (int) ev[i].data;
        }
        if (sw == WAITFD_C) {
            if (ev[i].filter == EVFILT_READ && (ev[i].fflags & EV_EOF))
                return IO_CLOSED;
            if (ev[i].filter == EVFILT_WRITE)
                return IO_DONE;
        } else {
            return IO_DONE;
        }
    }
    return IO_DONE;
}

/*-------------------------------------------------------------------------*\
 * Select with timeout control
 *
 * Built on kqueue: interest is registered with EV_ONESHOT so the kernel
 * only reports ready descriptors. There is no FD_SETSIZE limit.
 * The fd sets are rebuilt on return to mark the ready descriptors, like
 * select(2), so callers keep working unchanged.
 \*-------------------------------------------------------------------------*/
int socket_select(t_socket n, fd_set *rfds, fd_set *wfds, fd_set *efds,
        p_timeout tm) {
    int ret, i, nready = 0;
    double t;
    struct timespec ts;
    t_socket fd;
    struct kevent *kl;
    int nkl = 0;

    if (!kqueue_ensure()) {
        /* fall back to select if kqueue is unavailable */
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
    for (i = 0; i < kq_watch_count; i++) {
        struct kevent ev;
        EV_SET(&ev, kq_watch[i].ident, kq_watch[i].filter, EV_DELETE, 0, 0, NULL);
        kevent(kq_fd, &ev, 1, NULL, 0, NULL);
    }
    kq_watch_count = 0;

    /* 2. build changelist from the fd sets */
    {
        int kl_cap = (n > 0) ? n * 2 : 32;
        kl = (struct kevent *) malloc(sizeof(struct kevent) * (size_t) kl_cap);
        if (!kl) return ENOMEM;
    }

    for (fd = 0; fd < n; fd++) {
        short filt = 0;
        if ((rfds && FD_ISSET(fd, rfds)) || (efds && FD_ISSET(fd, efds)))
            filt |= EVFILT_READ;
        if (wfds && FD_ISSET(fd, wfds))
            filt |= EVFILT_WRITE;
        if (filt == 0) continue;

        if (filt & EVFILT_READ) {
            if (kq_watch_count == kq_watch_cap) {
                int ncap = kq_watch_cap ? kq_watch_cap * 2 : 32;
                kq_watch_t *nw = (kq_watch_t *) realloc(kq_watch,
                        (size_t) ncap * sizeof(*nw));
                if (!nw) { free(kl); return ENOMEM; }
                kq_watch = nw;
                kq_watch_cap = ncap;
            }
            kq_watch[kq_watch_count].ident = (int) fd;
            kq_watch[kq_watch_count].filter = EVFILT_READ;
            kq_watch_count++;
            EV_SET(&kl[nkl], fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_ONESHOT,
                    0, 0, NULL);
            nkl++;
        }
        if (filt & EVFILT_WRITE) {
            if (kq_watch_count == kq_watch_cap) {
                int ncap = kq_watch_cap ? kq_watch_cap * 2 : 32;
                kq_watch_t *nw = (kq_watch_t *) realloc(kq_watch,
                        (size_t) ncap * sizeof(*nw));
                if (!nw) { free(kl); return ENOMEM; }
                kq_watch = nw;
                kq_watch_cap = ncap;
            }
            kq_watch[kq_watch_count].ident = (int) fd;
            kq_watch[kq_watch_count].filter = EVFILT_WRITE;
            kq_watch_count++;
            EV_SET(&kl[nkl], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_ONESHOT,
                    0, 0, NULL);
            nkl++;
        }
    }

    /* 3. clear the fd sets; they will be repopulated with ready descriptors */
    if (rfds) FD_ZERO(rfds);
    if (wfds) FD_ZERO(wfds);
    if (efds) FD_ZERO(efds);

    /* 4. nothing to watch: just sleep for the timeout */
    if (nkl == 0) {
        t = timeout_getretry(tm);
        if (t > 0.0) {
            struct timespec req, rem;
            req.tv_sec = (long) t;
            req.tv_nsec = (long) ((t - req.tv_sec) * 1.0e9);
            while (nanosleep(&req, &rem) == -1 && errno == EINTR)
                req = rem;
        }
        free(kl);
        return 0;
    }

    /* 5. register interest and wait in a single kevent call */
    t = timeout_getretry(tm);
    ts.tv_sec = (long) t;
    ts.tv_nsec = (long) ((t - ts.tv_sec) * 1.0e9);
    ret = kevent(kq_fd, kl, nkl, kl, nkl,
            t >= 0.0 ? &ts : NULL);
    if (ret == -1) {
        int err = errno;
        free(kl);
        return err;
    }
    if (ret == 0) {
        free(kl);
        return 0;
    }

    /* 6. repopulate fd sets with ready descriptors */
    nready = ret;
    for (i = 0; i < ret; i++) {
        t_socket rfd = (t_socket) kl[i].ident;
        if (kl[i].filter == EVFILT_READ) {
            if (rfds) FD_SET(rfd, rfds);
            if (efds) FD_SET(rfd, efds);
        }
        if (kl[i].filter == EVFILT_WRITE) {
            if (wfds) FD_SET(rfd, wfds);
        }
    }
    free(kl);
    return nready;
}

/*-------------------------------------------------------------------------*\
 * Initializes module
 \*-------------------------------------------------------------------------*/
int socket_open(void) {
    /* installs a handler to ignore sigpipe or it will crash us */
    signal(SIGPIPE, SIG_IGN);
    kqueue_ensure();
    return 1;
}

/*-------------------------------------------------------------------------*\
 * Close module
 \*-------------------------------------------------------------------------*/
int socket_close(void) {
    if (kq_fd >= 0) {
        close(kq_fd);
        kq_fd = -1;
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
/* (implemented above with kqueue) */

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
        /* EPROTOTYPE means the connection is being closed */
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
        /* EPROTOTYPE means the connection is being closed */
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
