#include "mylib_error_functions.h"
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <time.h>
#define BUF_SPACE 1024
#define SHARED_CAP 65536
#define BACKLOG_LIMIT 4 * BUF_SPACE
#define STALL_TIMEOUT 10
#define MAX_SERVICE 100
u8 shared_buf[65536];
u16 write_end = 0;

typedef struct
{
    int fd;
    u16 pos, inlen;
    char buf[BUF_SPACE];
    struct sockaddr client;
    struct timespec last_progress;
} Record;

int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
    {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        return -1;
    }
    return 0;
}

int create_listner(int port)
{
    int fd, opt = 1;

    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1)
    {
        return -1;
    }
    addr.sin_port = htons((uint16_t)port);

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        return -1;
    }
    if (set_nonblocking(fd) == -1)
    {
        close(fd);
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1)
    {
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&addr, len) == -1)
    {
        close(fd);
        return -1;
    }
    if (listen(fd, MAX_SERVICE) == -1)
    {
        close(fd);
        return -1;
    }
    printf("Listner Created: %d\n", fd);
    return fd;
}

void print_addr(const struct sockaddr *addr, socklen_t addrlen)
{
    char ip[NI_MAXHOST];
    char port[NI_MAXSERV];

    int ret = getnameinfo(addr, addrlen,
                          ip, sizeof(ip),
                          port, sizeof(port),
                          NI_NUMERICHOST | NI_NUMERICSERV);

    if (ret != 0)
    {
        fprintf(stderr, "getnameinfo: %s\n", gai_strerror(ret));
        return;
    }

    printf("Connected IP:%s:%s\n", ip, port);
}

int add_to_epoll_instance(int epfd, int fd, struct sockaddr *addr, uint32_t event)
{
    struct epoll_event revent;

    revent.events = event;
    Record *tmp = malloc(sizeof(Record));
    if (tmp == NULL)
    {
        return -1;
    }
    tmp->fd = fd;
    tmp->pos = 0;
    tmp->inlen = 0;
    tmp->last_progress.tv_sec = 0;
    tmp->last_progress.tv_nsec = 0;
    revent.data.ptr = tmp;
    if (addr != NULL)
    {
        tmp->client = *(addr);
    }
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &revent) == -1)
    {
        free(tmp);
        return -1;
    }
    printf("Added %d to epoll_instance\n", fd);
    return 0;
}

void process_listner(int epfd, int fd)
{
    do
    {
        struct sockaddr addr;
        socklen_t len = sizeof(addr);

        int client_fd = accept(fd, &addr, &len);
        if (client_fd == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {

                return;
            }
            if (errno == EINTR || errno == ECONNABORTED)
            {
                continue;
            }
            ml_print_err_no("Process_Client: ");
            return;
        }

        if (set_nonblocking(client_fd) == -1)
        {
            ml_warn("Failed to set Non-Blocking. Skipping...!");
            close(client_fd);
            break;
        }
        if (add_to_epoll_instance(epfd, client_fd, &addr, EPOLLIN | EPOLLET) == -1)
        {
            ml_warn("Failed to set add to epoll_instance. Skipping...!");
            close(client_fd);
            break;
        }
        print_addr(&addr, len);

    } while (true);

    return;
}

int remove_from_epoll_instance(int epfd, Record *c)
{
    int res = epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);

    close(c->fd);
    printf("Closed %d\n", c->fd);
    free(c);

    return res;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        ml_fatal("Usage: ./server <port>");
    }

    int port = ml_str_to_num(argv[1]);

    if (!IN_RANGE(port, 0, 65535))
    {
        ml_fatal("Invalid Port.");
    }

    int listner_fd;

    if ((listner_fd = create_listner(port)) == -1)
    {
        ml_err_exit("Listner Socket: ");
    }

    int epfd = epoll_create1(0);

    if (epfd == -1)
    {
        ml_err_exit("Epoll: ");
    }

    if (add_to_epoll_instance(epfd, listner_fd, NULL, EPOLLIN | EPOLLET) == -1)
    {
        ml_err_exit("Failed to add listner fd in epoll instance.");
    }

    int m, i;
    struct epoll_event events[MAX_SERVICE];
    do
    {
        m = epoll_wait(epfd, events, MAX_SERVICE, -1);
        if (m == -1)
        {
            ml_print_err_no("EPOLL_WAIT: ");
        }
        for (i = 0; i < m; i++)
        {
            if (((Record *)events[i].data.ptr)->fd == listner_fd)
            {
                printf("Events: 0x%x\n", events[i].events);
                process_listner(epfd, listner_fd);
            }
            if (events[i].events & EPOLLERR)
            {
                remove_from_epoll_instance(epfd, events[i].events.data.ptr);
            }
            /*
            if(events[i].events & EPOLLIN)
            {
                process_reading(events[i].events.data.ptr)
            }
                */
        }

    } while (true);

    return 0;
}
