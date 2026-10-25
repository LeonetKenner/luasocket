/*=========================================================================*\
* Socket compatibilization module for Win32
* LuaSocket toolkit
*
* The penalty of calling select to avoid busy-wait is only paid when
* the I/O call fail in the first place.
\*=========================================================================*/
#include "luasocket.h"

#include <string.h>
#include <stdlib.h>

#include "socket.h"
#include "pierror.h"

/* WinSock doesn't have a strerror... */
static const char *wstrerror(int err);

/*-------------------------------------------------------------------------*\
* Initializes module
\*-------------------------------------------------------------------------*/
int socket_open(void) {
    WSADATA wsaData;
    WORD wVersionRequested = MAKEWORD(2, 0);
    int err = WSAStartup(wVersionRequested, &wsaData );
    if (err != 0) return 0;
    if ((LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 0) &&
        (LOBYTE(wsaData.wVersion) != 1 || HIBYTE(wsaData.wVersion) != 1)) {
        WSACleanup();
        return 0;
    }
    return 1;
}

/*-------------------------------------------------------------------------*\
* Close module
\*-------------------------------------------------------------------------*/
int socket_close(void) {
    WSACleanup();
    return 1;
}

/*-------------------------------------------------------------------------*\
* Wait for readable/writable/connected socket with timeout
\*-------------------------------------------------------------------------*/
#define WAITFD_R        1
#define WAITFD_W        2
#define WAITFD_E        4
#define WAITFD_C        (WAITFD_E|WAITFD_W)

#if _WIN32_WINNT >= 0x0600
#define LUASOCKET_USE_WSAPOLL 1
#else
#define LUASOCKET_USE_WSAPOLL 0
#endif

/*-------------------------------------------------------------------------*\
 * Wait for readable/writable/connected socket with timeout
 *
 * WSAPoll is used on Vista+ (_WIN32_WINNT >= 0x0600): cost is proportional
 * to the number of ready descriptors, not the total watch size, and there
 * is no FD_SETSIZE limit. Older Windows falls back to select(2).
 \*-------------------------------------------------------------------------*/
int socket_waitfd(p_socket ps, int sw, p_timeout tm) {
    int ret;
    double t;
    if (timeout_iszero(tm)) return IO_TIMEOUT;  /* optimize timeout == 0 case */
#if LUASOCKET_USE_WSAPOLL
    WSAPOLLFD pfd;
    int ms;
    pfd.fd = *ps;
    pfd.events = 0;
    if (sw & WAITFD_R) pfd.events |= POLLIN;
    if (sw & WAITFD_W) pfd.events |= POLLOUT;
    t = timeout_get(tm);
    ms = t >= 0.0 ? (int)(t * 1e3) : -1;
    ret = WSAPoll(&pfd, 1, ms);
    if (ret == -1) return WSAGetLastError();
    if (ret == 0) return IO_TIMEOUT;
    if (sw == WAITFD_C && (pfd.revents & (POLLERR | POLLHUP)))
        return IO_CLOSED;
    return IO_DONE;
#else
    fd_set rfds, wfds, efds, *rp = NULL, *wp = NULL, *ep = NULL;
    struct timeval tv, *tp = NULL;
    if (sw & WAITFD_R) {
        FD_ZERO(&rfds);
        FD_SET(*ps, &rfds);
        rp = &rfds;
    }
    if (sw & WAITFD_W) { FD_ZERO(&wfds); FD_SET(*ps, &wfds); wp = &wfds; }
    if (sw & WAITFD_C) { FD_ZERO(&efds); FD_SET(*ps, &efds); ep = &efds; }
    if ((t = timeout_get(tm)) >= 0.0) {
        tv.tv_sec = (int) t;
        tv.tv_usec = (int) ((t-tv.tv_sec)*1.0e6);
        tp = &tv;
    }
    ret = select(0, rp, wp, ep, tp);
    if (ret == -1) return WSAGetLastError();
    if (ret == 0) return IO_TIMEOUT;
    if (sw == WAITFD_C && FD_ISSET(*ps, &efds)) return IO_CLOSED;
    return IO_DONE;
#endif
}

/*-------------------------------------------------------------------------*\
 * Select with timeout control
 *
 * Winsock's select(2) is O(FD_SETSIZE) and capped by FD_SETSIZE. On
 * Vista+ we use WSAPoll, which is O(ready descriptors) and has no
 * FD_SETSIZE limit. Older Windows falls back to select(2).
 \*-------------------------------------------------------------------------*/
int socket_select(t_socket n, fd_set *rfds, fd_set *wfds, fd_set *efds,
        p_timeout tm) {
#if LUASOCKET_USE_WSAPOLL
    int ret, i, nfds = 0, nready = 0;
    double t;
    int ms;
    WSAPOLLFD *pfds;
    (void) n;  /* Winsock ignores nfds; we scan the fd_sets directly */

    t = timeout_get(tm);
    ms = t >= 0.0 ? (int)(t * 1e3) : -1;

    if (n <= 0) {
        Sleep((DWORD) (1000*t));
        return 0;
    }

    /* upper bound on unique sockets across the three sets */
    {
        int count = 0;
        if (rfds) count += (int) rfds->fd_count;
        if (wfds) count += (int) wfds->fd_count;
        if (efds) count += (int) efds->fd_count;
        nfds = count;
    }

    pfds = (WSAPOLLFD *) malloc(sizeof(WSAPOLLFD) * (size_t) nfds);
    if (!pfds) return WSAENOBUFS;

    nfds = 0;
    /* add each watched socket to the poll array, merging interest bits
     * when a socket appears in more than one set */
    for (i = 0; i < 3; i++) {
        fd_set *set;
        short evbit;
        switch (i) {
            case 0: set = rfds; evbit = POLLIN; break;
            case 1: set = wfds; evbit = POLLOUT; break;
            default: set = efds; evbit = POLLERR; break;
        }
        if (!set) continue;
        for (int k = 0; k < (int) set->fd_count; k++) {
            SOCKET s = set->fd_array[k];
            int found = 0;
            for (int m = 0; m < nfds; m++) {
                if (pfds[m].fd == s) {
                    pfds[m].events |= evbit;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                pfds[nfds].fd = s;
                pfds[nfds].events = evbit;
                nfds++;
            }
        }
    }

    /* clear the fd sets; they will be repopulated with ready sockets */
    if (rfds) FD_ZERO(rfds);
    if (wfds) FD_ZERO(wfds);
    if (efds) FD_ZERO(efds);

    if (nfds == 0) {
        if (ms > 0) Sleep((DWORD) ms);
        free(pfds);
        return 0;
    }

    ret = WSAPoll(pfds, (ULONG) nfds, ms);
    if (ret == -1) {
        free(pfds);
        return -1;
    }
    if (ret == 0) {
        free(pfds);
        return 0;
    }

    /* repopulate fd sets with ready sockets */
    for (i = 0; i < nfds; i++) {
        SOCKET s = pfds[i].fd;
        short re = pfds[i].revents;
        if (rfds && (re & (POLLIN | POLLERR | POLLHUP))) FD_SET(s, rfds);
        if (wfds && (re & POLLOUT)) FD_SET(s, wfds);
        if (efds && (re & POLLERR)) FD_SET(s, efds);
    }
    nready = ret;
    free(pfds);
    return nready;
#else
    struct timeval tv;
    double t = timeout_get(tm);
    tv.tv_sec = (int) t;
    tv.tv_usec = (int) ((t - tv.tv_sec) * 1.0e6);
    if (n <= 0) {
        Sleep((DWORD) (1000*t));
        return 0;
    } else return select(0, rfds, wfds, efds, t >= 0.0? &tv: NULL);
#endif
}

/*-------------------------------------------------------------------------*\
* Close and inutilize socket
\*-------------------------------------------------------------------------*/
void socket_destroy(p_socket ps) {
    if (*ps != SOCKET_INVALID) {
        socket_setblocking(ps); /* close can take a long time on WIN32 */
        closesocket(*ps);
        *ps = SOCKET_INVALID;
    }
}

/*-------------------------------------------------------------------------*\
*
\*-------------------------------------------------------------------------*/
void socket_shutdown(p_socket ps, int how) {
    socket_setblocking(ps);
    shutdown(*ps, how);
    socket_setnonblocking(ps);
}

/*-------------------------------------------------------------------------*\
* Creates and sets up a socket
\*-------------------------------------------------------------------------*/
int socket_create(p_socket ps, int domain, int type, int protocol) {
    *ps = socket(domain, type, protocol);
    if (*ps != SOCKET_INVALID) return IO_DONE;
    else return WSAGetLastError();
}

/*-------------------------------------------------------------------------*\
* Connects or returns error message
\*-------------------------------------------------------------------------*/
int socket_connect(p_socket ps, SA *addr, socklen_t len, p_timeout tm) {
    int err;
    /* don't call on closed socket */
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    /* ask system to connect */
    if (connect(*ps, addr, len) == 0) return IO_DONE;
    /* make sure the system is trying to connect */
    err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) return err;
    /* zero timeout case optimization */
    if (timeout_iszero(tm)) return IO_TIMEOUT;
    /* we wait until something happens */
    err = socket_waitfd(ps, WAITFD_C, tm);
    if (err == IO_CLOSED) {
        int elen = sizeof(err);
        /* give windows time to set the error (yes, disgusting) */
        Sleep(10);
        /* find out why we failed */
        getsockopt(*ps, SOL_SOCKET, SO_ERROR, (char *)&err, &elen);
        /* we KNOW there was an error. if 'why' is 0, we will return
        * "unknown error", but it's not really our fault */
        return err > 0? err: IO_UNKNOWN;
    } else return err;

}

/*-------------------------------------------------------------------------*\
* Binds or returns error message
\*-------------------------------------------------------------------------*/
int socket_bind(p_socket ps, SA *addr, socklen_t len) {
    int err = IO_DONE;
    socket_setblocking(ps);
    if (bind(*ps, addr, len) < 0) err = WSAGetLastError();
    socket_setnonblocking(ps);
    return err;
}

/*-------------------------------------------------------------------------*\
*
\*-------------------------------------------------------------------------*/
int socket_listen(p_socket ps, int backlog) {
    int err = IO_DONE;
    socket_setblocking(ps);
    if (listen(*ps, backlog) < 0) err = WSAGetLastError();
    socket_setnonblocking(ps);
    return err;
}

/*-------------------------------------------------------------------------*\
* Accept with timeout
\*-------------------------------------------------------------------------*/
int socket_accept(p_socket ps, p_socket pa, SA *addr, socklen_t *len,
        p_timeout tm) {
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    for ( ;; ) {
        int err;
        /* try to get client socket */
        if ((*pa = accept(*ps, addr, len)) != SOCKET_INVALID) return IO_DONE;
        /* find out why we failed */
        err = WSAGetLastError();
        /* if we failed because there was no connectoin, keep trying */
        if (err != WSAEWOULDBLOCK && err != WSAECONNABORTED) return err;
        /* call select to avoid busy wait */
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
}

/*-------------------------------------------------------------------------*\
* Send with timeout
* On windows, if you try to send 10MB, the OS will buffer EVERYTHING
* this can take an awful lot of time and we will end up blocked.
* Therefore, whoever calls this function should not pass a huge buffer.
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
        /* try to send something */
        int put = send(*ps, data, (int) count, 0);
        /* if we sent something, we are done */
        if (put > 0) {
            *sent = put;
            return IO_DONE;
        }
        /* deal with failure */
        err = WSAGetLastError();
        /* we can only proceed if there was no serious error */
        if (err != WSAEWOULDBLOCK) return err;
        /* avoid busy wait */
        if ((err = socket_waitfd(ps, WAITFD_W, tm)) != IO_DONE) return err;
    }
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
        int put = sendto(*ps, data, (int) count, 0, addr, len);
        if (put > 0) {
            *sent = put;
            return IO_DONE;
        }
        err = WSAGetLastError();
        if (err != WSAEWOULDBLOCK) return err;
        if ((err = socket_waitfd(ps, WAITFD_W, tm)) != IO_DONE) return err;
    }
}

/*-------------------------------------------------------------------------*\
* Receive with timeout
\*-------------------------------------------------------------------------*/
int socket_recv(p_socket ps, char *data, size_t count, size_t *got,
        p_timeout tm)
{
    int err, prev = IO_DONE;
    *got = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    if (count == 0) return IO_DONE;
    for ( ;; ) {
        int taken = recv(*ps, data, (int) count, 0);
        if (taken > 0) {
            *got = taken;
            return IO_DONE;
        }
        if (taken == 0) return IO_CLOSED;
        err = WSAGetLastError();
        /* On UDP, a connreset simply means the previous send failed.
         * So we try again.
         * On TCP, it means our socket is now useless, so the error passes.
         * (We will loop again, exiting because the same error will happen) */
        if (err != WSAEWOULDBLOCK) {
            if (err != WSAECONNRESET || prev == WSAECONNRESET) return err;
            prev = err;
            continue;
        }
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
}

/*-------------------------------------------------------------------------*\
* Recvfrom with timeout
\*-------------------------------------------------------------------------*/
int socket_recvfrom(p_socket ps, char *data, size_t count, size_t *got,
        SA *addr, socklen_t *len, p_timeout tm)
{
    int err, prev = IO_DONE;
    *got = 0;
    if (*ps == SOCKET_INVALID) return IO_CLOSED;
    for ( ;; ) {
        int taken = recvfrom(*ps, data, (int) count, 0, addr, len);
        if (taken > 0) {
            *got = taken;
            return IO_DONE;
        }
        if (taken == 0) return IO_CLOSED;
        err = WSAGetLastError();
        /* a zero-length request is trivially "too small" for any
         * non-empty datagram; unlike POSIX, which truncates and succeeds
         * silently, Windows reports this as WSAEMSGSIZE even though the
         * datagram -- and its sender's address, already written to addr/
         * len above -- was still consumed. Normalize it to match POSIX's
         * silent-truncation instead of surfacing a platform-specific
         * error for what is otherwise a successful, if empty, receive. */
        if (count == 0 && err == WSAEMSGSIZE) return IO_DONE;
        /* On UDP, a connreset simply means the previous send failed.
         * So we try again.
         * On TCP, it means our socket is now useless, so the error passes.
         * (We will loop again, exiting because the same error will happen) */
        if (err != WSAEWOULDBLOCK) {
            if (err != WSAECONNRESET || prev == WSAECONNRESET) return err;
            prev = err;
            continue;
        }
        if ((err = socket_waitfd(ps, WAITFD_R, tm)) != IO_DONE) return err;
    }
}

/*-------------------------------------------------------------------------*\
* Put socket into blocking mode
\*-------------------------------------------------------------------------*/
void socket_setblocking(p_socket ps) {
    u_long argp = 0;
    ioctlsocket(*ps, FIONBIO, &argp);
}

/*-------------------------------------------------------------------------*\
* Put socket into non-blocking mode
\*-------------------------------------------------------------------------*/
void socket_setnonblocking(p_socket ps) {
    u_long argp = 1;
    ioctlsocket(*ps, FIONBIO, &argp);
}

/*-------------------------------------------------------------------------*\
* DNS helpers
\*-------------------------------------------------------------------------*/
int socket_gethostbyaddr(const char *addr, socklen_t len, struct hostent **hp) {
    *hp = gethostbyaddr(addr, len, AF_INET);
    if (*hp) return IO_DONE;
    else return WSAGetLastError();
}

int socket_gethostbyname(const char *addr, struct hostent **hp) {
    *hp = gethostbyname(addr);
    if (*hp) return IO_DONE;
    else return  WSAGetLastError();
}

/*-------------------------------------------------------------------------*\
* Error translation functions
\*-------------------------------------------------------------------------*/
const char *socket_hoststrerror(int err) {
    if (err <= 0) return io_strerror(err);
    switch (err) {
        case WSAHOST_NOT_FOUND: return PIE_HOST_NOT_FOUND;
        default: return wstrerror(err);
    }
}

const char *socket_strerror(int err) {
    if (err <= 0) return io_strerror(err);
    switch (err) {
        case WSAEADDRINUSE: return PIE_ADDRINUSE;
        case WSAECONNREFUSED : return PIE_CONNREFUSED;
        case WSAEISCONN: return PIE_ISCONN;
        case WSAEACCES: return PIE_ACCESS;
        case WSAECONNABORTED: return PIE_CONNABORTED;
        case WSAECONNRESET: return PIE_CONNRESET;
        case WSAETIMEDOUT: return PIE_TIMEDOUT;
        default: return wstrerror(err);
    }
}

const char *socket_ioerror(p_socket ps, int err) {
    (void) ps;
    return socket_strerror(err);
}

static const char *wstrerror(int err) {
    switch (err) {
        case WSAEINTR: return "Interrupted function call";
        case WSAEACCES: return PIE_ACCESS; /* "Permission denied"; */
        case WSAEFAULT: return "Bad address";
        case WSAEINVAL: return "Invalid argument";
        case WSAEMFILE: return "Too many open files";
        case WSAEWOULDBLOCK: return "Resource temporarily unavailable";
        case WSAEINPROGRESS: return "Operation now in progress";
        case WSAEALREADY: return "Operation already in progress";
        case WSAENOTSOCK: return "Socket operation on nonsocket";
        case WSAEDESTADDRREQ: return "Destination address required";
        case WSAEMSGSIZE: return "Message too long";
        case WSAEPROTOTYPE: return "Protocol wrong type for socket";
        case WSAENOPROTOOPT: return "Bad protocol option";
        case WSAEPROTONOSUPPORT: return "Protocol not supported";
        case WSAESOCKTNOSUPPORT: return PIE_SOCKTYPE; /* "Socket type not supported"; */
        case WSAEOPNOTSUPP: return "Operation not supported";
        case WSAEPFNOSUPPORT: return "Protocol family not supported";
        case WSAEAFNOSUPPORT: return PIE_FAMILY; /* "Address family not supported by protocol family"; */
        case WSAEADDRINUSE: return PIE_ADDRINUSE; /* "Address already in use"; */
        case WSAEADDRNOTAVAIL: return "Cannot assign requested address";
        case WSAENETDOWN: return "Network is down";
        case WSAENETUNREACH: return "Network is unreachable";
        case WSAENETRESET: return "Network dropped connection on reset";
        case WSAECONNABORTED: return "Software caused connection abort";
        case WSAECONNRESET: return PIE_CONNRESET; /* "Connection reset by peer"; */
        case WSAENOBUFS: return "No buffer space available";
        case WSAEISCONN: return PIE_ISCONN; /* "Socket is already connected"; */
        case WSAENOTCONN: return "Socket is not connected";
        case WSAESHUTDOWN: return "Cannot send after socket shutdown";
        case WSAETIMEDOUT: return PIE_TIMEDOUT; /* "Connection timed out"; */
        case WSAECONNREFUSED: return PIE_CONNREFUSED; /* "Connection refused"; */
        case WSAEHOSTDOWN: return "Host is down";
        case WSAEHOSTUNREACH: return "No route to host";
        case WSAEPROCLIM: return "Too many processes";
        case WSASYSNOTREADY: return "Network subsystem is unavailable";
        case WSAVERNOTSUPPORTED: return "Winsock.dll version out of range";
        case WSANOTINITIALISED:
            return "Successful WSAStartup not yet performed";
        case WSAEDISCON: return "Graceful shutdown in progress";
        case WSAHOST_NOT_FOUND: return PIE_HOST_NOT_FOUND; /* "Host not found"; */
        case WSATRY_AGAIN: return "Nonauthoritative host not found";
        case WSANO_RECOVERY: return PIE_FAIL; /* "Nonrecoverable name lookup error"; */
        case WSANO_DATA: return "Valid name, no data record of requested type";
        default: return "Unknown error";
    }
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
#ifdef EAI_SYSTEM
        case EAI_SYSTEM: return strerror(errno);
#endif
        default: return LUA_GAI_STRERROR(err);
    }
}
