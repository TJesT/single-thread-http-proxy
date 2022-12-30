#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <poll.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#include "picohttpparser.h"

#define ERROR_INVALID_ARGS 1
#define ERROR_PORT_CONVERSATION 2
#define ERROR_ALLOC 3
#define ERROR_SOCKET_INIT 4
#define ERROR_BIND 5
#define ERROR_LISTEN 6
#define ERROR_PIPE_OPEN 7
#define ERROR_SIG_HANDLER_INIT 8

#define TIMEOUT 1200
#define START_REQUEST_SIZE BUFSIZ
#define START_RESPONSE_SIZE (BUFSIZ * 2)


size_t POLL_TABLE_SIZE = 32;
int poll_last_index = -1;
struct pollfd *poll_fds;

int WRITE_STOP_FD = -1;
int READ_STOP_FD = -1;

double average_write_size_to_client = 0;
int num_writes_client = 0;
double average_write_size_to_server = 0;
int num_writes_server = 0;

void destroyPollFds() {
    for (int i = 0; i < poll_last_index; i++) {
        if (poll_fds[i].fd > 0) {
            int close_res = close(poll_fds[i].fd);
            if (close_res < 0) {
                fprintf(stderr, "destroy poll_fds[%d].fd ", i);
                perror("close");
            }
            poll_fds[i].fd = -1;
        }
    }
    free(poll_fds);
    poll_last_index = -1;
}

void removeFromPollFds(int fd) {
    if (fd < 0) {
        return;
    }
    int i;
    for (i = 0; i < poll_last_index; i++) {
        if (poll_fds[i].fd == fd) {
            int close_res = close(poll_fds[i].fd);
            if (close_res < 0) {
                fprintf(stderr, "remove poll_fds[%d].fd ", i);
                perror("close");
            }
            poll_fds[i].fd = -1;
            poll_fds[i].events = 0;
            poll_fds[i].revents = 0;
            break;
        }
    }
    if (i == poll_last_index - 1) {
        poll_last_index -= 1;
    }
    for (i = (int)poll_last_index - 1; i > 0; i--) {
        if (poll_fds[i].fd == -1) {
            poll_last_index -= 1;
        }
        else {
            break;
        }
    }
}

typedef struct cache {
    char *url;
    char *request;
    char *response;
    int *subscribers;
    size_t URL_LEN;
    size_t SUBSCRIBERS_SIZE;
    size_t REQUEST_SIZE;
    size_t RESPONSE_SIZE;
    size_t response_index;
    int server_index;
    bool full;
    bool valid;
    bool private;
} cache_t;

size_t CACHE_SIZE = 16;
cache_t *cache_table;
bool valid_cache = false;

void destroyCacheTable() {
    for (int i = 0; i < CACHE_SIZE; i++) {

        if (cache_table[i].request != NULL) {
            free(cache_table[i].request);
        }
        if (cache_table[i].response != NULL) {
            free(cache_table[i].response);
        }
        if (cache_table[i].subscribers != NULL) {
            free(cache_table[i].subscribers);
        }
    }
    free(cache_table);
    valid_cache = false;
}

typedef struct client {
    char *request;
    size_t REQUEST_SIZE;
    size_t request_index;

    int cache_index;
    int write_response_index;

    int fd;
} client_t;

size_t CLIENTS_SIZE = 16;
client_t *clients;
bool valid_clients = false;

void destroyClients() {
    for (int i = 0; i < CLIENTS_SIZE; i++) {
        if (clients[i].request != NULL) {
            free(clients[i].request);
            clients[i].request = NULL;
        }
        if (clients[i].fd != -1) {
            removeFromPollFds(clients[i].fd);
            clients[i].fd = -1;
        }
        clients[i].REQUEST_SIZE = 0;
        clients[i].request_index = 0;
        clients[i].cache_index = -1;
        clients[i].write_response_index = -1;
    }
    free(clients);
    valid_clients = false;
}

typedef struct server {
    int fd;
    int cache_index;
    int write_request_index;
} server_t;

size_t SERVERS_SIZE = 8;
server_t *servers;
bool valid_servers = false;

void destroyServers() {
    for (int i = 0; i < SERVERS_SIZE; i++) {
        if (servers[i].fd != -1) {
            removeFromPollFds(servers[i].fd);
            servers[i].fd = -1;
        }
    }
    free(servers);
    valid_servers = false;
}

void cleanUp() {
    fprintf(stderr, "\ncleaning up...\n");
    fprintf(stderr, "client: avg_write_size = %lf num_writes = %d\n", average_write_size_to_client, num_writes_client);
    fprintf(stderr, "server: avg_write_size = %lf num_writes = %d\n", average_write_size_to_server, num_writes_server);
    sleep(1);
    if (READ_STOP_FD != -1) {
        removeFromPollFds(READ_STOP_FD);
        READ_STOP_FD = -1;
    }
    if (WRITE_STOP_FD != -1) {
        int close_res = close(WRITE_STOP_FD);
        if (close_res < 0) {
            perror("cleanUp: close WRITE_STOP_FD");
        }
        WRITE_STOP_FD = -1;
    }
    if (valid_clients) {
        destroyClients();
    }
    if (valid_servers) {
        destroyServers();
    }
    if (poll_last_index >= 0) {
        destroyPollFds();
    }
    if (valid_cache) {
        destroyCacheTable();
    }
}

void initEmptyServer(size_t i) {
    servers[i].fd = -1;
    servers[i].cache_index = -1;
    servers[i].write_request_index = -1;
}

void initServers() {
    servers = (server_t *)calloc(SERVERS_SIZE, sizeof(server_t));
    if (servers == NULL) {
        fprintf(stderr, "failed to alloc memory for servers\n");
        cleanUp();
        exit(ERROR_ALLOC);
    }
    for (size_t i = 0; i < SERVERS_SIZE; i++) {
        initEmptyServer(i);
    }
}

void reallocServers() {
    size_t prev_size = SERVERS_SIZE;
    SERVERS_SIZE *= 2;
    fprintf(stderr, "realloc servers, new expected servers %lu\n", SERVERS_SIZE);
    servers = realloc(servers, SERVERS_SIZE * sizeof(server_t));
    for (size_t i = prev_size; i < SERVERS_SIZE; i++) {
        initEmptyServer(i);
    }
}

int findFreeServer(int server_fd) {
    if (server_fd < 0) {
        return -1;
    }
    for (int i = 0; i < SERVERS_SIZE; i++) {
        if (servers[i].fd == -1) {
            servers[i].fd = server_fd;
            return i;
        }
    }
    size_t prev_size = SERVERS_SIZE;
    reallocServers();
    servers[prev_size].fd = server_fd;
    return (int)prev_size;
}

int findServerByFd(int fd) {
    if (fd < 0) {
        return -1;
    }
    for (int i = 0; i < SERVERS_SIZE; i++) {
        if (servers[i].fd == fd) {
            return i;
        }
    }
    return -1;
}

void initEmptyClient(size_t i) {
    if (i >= CLIENTS_SIZE) {
        return;
    }
    clients[i].fd = -1;
    clients[i].request_index = 0;
    clients[i].request = NULL;
    clients[i].REQUEST_SIZE = 0;

    clients[i].cache_index = -1;
    clients[i].write_response_index = -1;
}

void initClients() {
    clients = (client_t *) calloc(CLIENTS_SIZE, sizeof(client_t));
    if (clients == NULL) {
        fprintf(stderr, "failed to alloc memory for clients\n");
        cleanUp();
        exit(ERROR_ALLOC);
    }
    for (size_t i = 0; i < CLIENTS_SIZE; i++) {
        initEmptyClient(i);
    }
    valid_clients = true;
}

void reallocClients() {
    size_t prev_size = CLIENTS_SIZE;
    CLIENTS_SIZE *= 2;
    fprintf(stderr, "realloc clients, new expected clients size %lu\n", CLIENTS_SIZE);
    clients = realloc(clients, CLIENTS_SIZE * sizeof(client_t));
    for (size_t i = prev_size; i < CLIENTS_SIZE; i++) {
        initEmptyClient(i);
    }
}

int findFreeClient(int client_fd) {
    if (client_fd < 0) {
        return -1;
    }
   for (int i = 0; i < CLIENTS_SIZE; i++) {
       if (clients[i].fd == -1) {
           clients[i].fd = client_fd;
           return i;
       }
   }
   size_t prev_size = CLIENTS_SIZE;
   reallocClients();
   clients[prev_size].fd = client_fd;
   return (int)prev_size;
}

int findClientByFd(int fd) {
    if (fd < 0) {
        return -1;
    }
    for (int i = 0; i < CLIENTS_SIZE; i++) {
        if (clients[i].fd == fd) {
            return i;
        }
    }
    return -1;
}

void initEmptyCache(size_t i) {
    if (i >= CACHE_SIZE) {
        return;
    }
    cache_table[i].request = NULL;
    cache_table[i].response = NULL;
    cache_table[i].response_index = 0;
    cache_table[i].RESPONSE_SIZE = 0;
    cache_table[i].subscribers = NULL;
    cache_table[i].full = false;
    cache_table[i].url = NULL;
    cache_table[i].URL_LEN = 0;
    cache_table[i].SUBSCRIBERS_SIZE = 0;
    cache_table[i].server_index = -1;
    cache_table[i].private = true;
    cache_table[i].valid = false;
}

void initCacheTable() {
    cache_table = (cache_t *)calloc(CACHE_SIZE, sizeof(cache_t));
    if (cache_table == NULL) {
        fprintf(stderr, "failed to alloc memory for cache_table\n");
        cleanUp();
        exit(ERROR_ALLOC);
    }
    for (size_t i = 0; i < CACHE_SIZE; i++) {
        initEmptyCache(i);
    }
    valid_cache = true;
}

void reallocCacheTable() {
    size_t prev_size = CACHE_SIZE;
    CACHE_SIZE *= 2;
    fprintf(stderr, "realloc cache, new expected cache %lu\n", CACHE_SIZE);
    cache_table = realloc(cache_table, CACHE_SIZE * sizeof(cache_t));
    for (size_t i = prev_size; i < CACHE_SIZE; i++) {
        initEmptyCache(i);
    }
}

int findAtCache(char *url, size_t url_size, bool *exists) {
    int free_cache_index = -1;
    for (int i = 0; i < CACHE_SIZE; i++) {
        if (cache_table[i].valid && cache_table[i].URL_LEN == url_size &&
            strncmp(url, cache_table[i].url, url_size) == 0) {
            *exists = true;
            return i;
        }
        if (!cache_table[i].valid && free_cache_index == -1) {
            free_cache_index = i;
        }
    }
    *exists = false;
    if (free_cache_index < CACHE_SIZE && free_cache_index >= 0) {
        return free_cache_index;
    }
    int prev_size = (int)CACHE_SIZE;
    reallocCacheTable();
    return prev_size;
}

void initPollFds() {
    poll_fds = (struct pollfd *)calloc(POLL_TABLE_SIZE, sizeof(struct pollfd));
    if (poll_fds == NULL) {
        fprintf(stderr, "failed to alloc memory for poll_fds\n");
        cleanUp();
        exit(ERROR_ALLOC);
    }
    for (int i = 0; i < POLL_TABLE_SIZE; i++) {
        poll_fds[i].fd = -1;
    }
    poll_last_index = 0;
}

void reallocPollFds() {
    POLL_TABLE_SIZE *= 2;
    //fprintf(stderr, "realloc poll_fds, new expected poll_fds %lu\n", POLL_TABLE_SIZE);
    poll_fds = (struct pollfd *)realloc(poll_fds, POLL_TABLE_SIZE * (sizeof(struct pollfd)));
    for (size_t i = poll_last_index; i < POLL_TABLE_SIZE; i++) {
        poll_fds[i].fd = -1;
    }
}

void addFdToPollFds(int fd, short events) {
    for (int i = 0; i < poll_last_index; i++) {
        if (poll_fds[i].fd == -1) {
            poll_fds[i].fd = fd;
            poll_fds[i].events = events;
            return;
        }
    }
    if (poll_last_index >= POLL_TABLE_SIZE) {
        reallocPollFds();
    }
    poll_fds[poll_last_index].fd = fd;
    poll_fds[poll_last_index].events = events;
    poll_last_index += 1;
}

int connectToServerHost(char *hostname, int port) {
    if (hostname == NULL || port < 0) {
        return -1;
    }
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        return -1;
    }

    struct hostent *h = gethostbyname(hostname);
    if (h == NULL) {
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;
    memcpy(&addr.sin_addr, h->h_addr, h->h_length);

    int connect_res = connect(server_sock, (struct sockaddr*)&addr, sizeof(struct sockaddr_in));
    if (connect_res != 0) {
        perror("connect");
        return -1;
    }

    return server_sock;
}

int initListener(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        cleanUp();
        exit(ERROR_SOCKET_INIT);
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    int bind_res = bind(listen_fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_in));
    if (bind_res != 0) {
        perror("bind");
        int close_res = close(listen_fd);
        if (close_res < 0) {
            perror("close listen_fd");
        }
        cleanUp();
        exit(ERROR_BIND);
    }

    int listen_res = listen(listen_fd, (int)CLIENTS_SIZE);
    if (listen_res == -1) {
        perror("listen");
        int close_res = close(listen_fd);
        if (close_res < 0) {
            perror("close listen_fd");
        }
        cleanUp();
        exit(ERROR_LISTEN);
    }
    return listen_fd;
}

void acceptNewClient(int listen_fd) {
    int new_client_fd = accept(listen_fd, NULL, NULL);
    if (new_client_fd == -1) {
        perror("new client accept");
        return;
    }
    int fcntl_res = fcntl(new_client_fd, F_SETFL, O_NONBLOCK);
    if (fcntl_res < 0) {
        perror("make new client nonblock");
        int close_res = close(new_client_fd);
        if (close_res < 0) {
            perror("close client");
        }
        return;
    }
    int index = findFreeClient(new_client_fd);
    addFdToPollFds(new_client_fd, POLLIN);
    fprintf(stderr, "new client %d accepted\n", index);
}

void changeEventForFd(int fd, short new_events) {
    for (int i = 0; i < poll_last_index; i++) {
        if (poll_fds[i].fd == fd) {
            poll_fds[i].events = new_events;
        }
    }
}

void removeSubscriber(int client_num, int cache_index) {
    if (cache_index >= CACHE_SIZE || client_num >= CLIENTS_SIZE || client_num < 0 || cache_index < 0) {
        return;
    }
    for (int i = 0; i < cache_table[cache_index].SUBSCRIBERS_SIZE; i++) {
        if (cache_table[cache_index].subscribers[i] == client_num) {
            cache_table[cache_index].subscribers[i] = -1;
        }
    }
}

void disconnectClient(int client_num) {
    if (client_num < 0 || client_num >= CLIENTS_SIZE) {
        return;
    }
    fprintf(stderr, "disconnecting client %d...\n", client_num);
    if (clients[client_num].request != NULL) {
        memset(clients[client_num].request, 0, clients[client_num].request_index * sizeof(char));
        clients[client_num].request_index = 0;
    }

    if (clients[client_num].cache_index != -1) {
        removeSubscriber(client_num, clients[client_num].cache_index);
        clients[client_num].cache_index = -1;
        clients[client_num].write_response_index = -1;
    }
    if (clients[client_num].fd != -1) {
        removeFromPollFds(clients[client_num].fd);
        clients[client_num].fd = -1;
    }
}

void addSubscriber(int client_num, int cache_index) {
    if (cache_index >= CACHE_SIZE || client_num >= CLIENTS_SIZE || client_num < 0 || cache_index < 0 ||
        !cache_table[cache_index].valid) {
        return;
    }
    if (cache_table[cache_index].SUBSCRIBERS_SIZE == 0) {
        cache_table[cache_index].SUBSCRIBERS_SIZE = 4;
        //fprintf(stderr, "calloc subscribers for cache %d subs_size %lu\n", cache_index,
        //        cache_table[cache_index].SUBSCRIBERS_SIZE);
        cache_table[cache_index].subscribers = (int *) calloc(cache_table[cache_index].SUBSCRIBERS_SIZE, sizeof(int));
        if (cache_table[cache_index].subscribers == NULL) {
            disconnectClient(client_num);
        }
        for (int i = 0; i < cache_table[cache_index].SUBSCRIBERS_SIZE; i++) {
            cache_table[cache_index].subscribers[i] = -1;
        }
    }
    for (int i = 0; i < cache_table[cache_index].SUBSCRIBERS_SIZE; i++) {
        if (cache_table[cache_index].subscribers[i] == -1 ||
            cache_table[cache_index].subscribers[i] == client_num) {
            cache_table[cache_index].subscribers[i] = client_num;
            return;
        }
    }
    size_t prev_index = cache_table[cache_index].SUBSCRIBERS_SIZE;
    cache_table[cache_index].SUBSCRIBERS_SIZE *= 2;
    cache_table[cache_index].subscribers = realloc(cache_table[cache_index].subscribers,
                                                   cache_table[cache_index].SUBSCRIBERS_SIZE * sizeof(int));
    for (size_t i = prev_index; i < cache_table[cache_index].SUBSCRIBERS_SIZE; i++) {
        cache_table[cache_index].subscribers[i] = -1;
    }
    cache_table[cache_index].subscribers[prev_index] = client_num;
}

void notifySubscribers(int cache_num, short new_events) {
    for (int i = 0; i < cache_table[cache_num].SUBSCRIBERS_SIZE; i++) {
        if (cache_table[cache_num].subscribers[i] != -1) {
            changeEventForFd(clients[cache_table[cache_num].subscribers[i]].fd, new_events);
            if (clients[cache_table[cache_num].subscribers[i]].write_response_index < 0) {
                clients[cache_table[cache_num].subscribers[i]].write_response_index = 0;
            }
        }
    }
}

void disconnectServer(int server_num) {
    if (server_num < 0 || server_num > SERVERS_SIZE) {
        return;
    }
    servers[server_num].write_request_index = -1;
    if (servers[server_num].cache_index >= 0) {
        cache_table[servers[server_num].cache_index].server_index = -1;
        notifySubscribers(servers[server_num].cache_index, POLLIN | POLLOUT);
        servers[server_num].cache_index = -1;
    }
    if (servers[server_num].fd != -1) {
        removeFromPollFds(servers[server_num].fd);
        servers[server_num].fd = -1;
    }
}

void freeCacheRecord(int cache_num) {
    if (cache_num < 0 || cache_num > CACHE_SIZE) {
        return;
    }
    cache_table[cache_num].valid = false;
    cache_table[cache_num].private = true;
    if (cache_table[cache_num].url != NULL) {
        free(cache_table[cache_num].url);
        cache_table[cache_num].url = NULL;
        cache_table[cache_num].URL_LEN = 0;
    }
    if (cache_table[cache_num].request != NULL) {
        free(cache_table[cache_num].request);
        cache_table[cache_num].request = NULL;
    }
    cache_table[cache_num].REQUEST_SIZE = 0;
    if (cache_table[cache_num].response != NULL) {
        free(cache_table[cache_num].response);
        cache_table[cache_num].response = NULL;
    }
    cache_table[cache_num].response_index = 0;
    cache_table[cache_num].RESPONSE_SIZE = 0;
    if (cache_table[cache_num].subscribers != NULL) {
        for(int i = 0; i < cache_table[cache_num].SUBSCRIBERS_SIZE; i++) {
            if (cache_table[cache_num].subscribers[i] != -1) {
                disconnectClient(cache_table[cache_num].subscribers[i]);
            }
        }
        free(cache_table[cache_num].subscribers);
        cache_table[cache_num].subscribers = NULL;
        cache_table[cache_num].SUBSCRIBERS_SIZE = 0;
    }
    if (cache_table[cache_num].server_index != -1) {
        disconnectServer(cache_table[cache_num].server_index);
        cache_table[cache_num].server_index = -1;
    }

}

void shiftRequest(int client_num, int pret) {
    if (client_num < 0 || client_num >= CLIENTS_SIZE || pret <= 0) {
        return;
    }
    for (int i = pret; i < clients[client_num].request_index; i++) {
        clients[client_num].request[i] = clients[client_num].request[i - pret];
    }
    memset(&clients[client_num].request[clients[client_num].request_index - pret], 0, pret);
    clients[client_num].request_index -= pret;
}

void readFromClient(int client_num) {
    if (client_num < 0 || client_num > CLIENTS_SIZE) {
        return;
    }
    char buf[BUFSIZ];
    ssize_t was_read = read(clients[client_num].fd, buf, BUFSIZ);
    if (was_read < 0) {
        fprintf(stderr, "error in client %d ", client_num);
        perror("read");
        disconnectClient(client_num);
        return;
    }
    else if (was_read == 0) {
        fprintf(stderr, "client %d closed connection\n", client_num);
        disconnectClient(client_num);
        return;
    }
    if (clients[client_num].REQUEST_SIZE == 0) {
        clients[client_num].REQUEST_SIZE = START_REQUEST_SIZE;
        clients[client_num].request = (char *)calloc(clients[client_num].REQUEST_SIZE, sizeof(char));
        if (clients[client_num].request == NULL) {
            fprintf(stderr, "calloc returned NULL\n");
            disconnectClient(client_num);
        }
    }
    if (clients[client_num].request_index + was_read >= clients[client_num].REQUEST_SIZE) {
        clients[client_num].REQUEST_SIZE *= 2;
        clients[client_num].request = realloc(clients[client_num].request,
                                                    clients[client_num].REQUEST_SIZE * sizeof(char));
    }
    memcpy(&clients[client_num].request[clients[client_num].request_index], buf, was_read);
    clients[client_num].request_index += was_read;
    char *method;
    char *path;
    size_t method_len, path_len;
    int minor_version;
    size_t num_headers = 100;
    struct phr_header headers[num_headers];
    int pret = phr_parse_request(clients[client_num].request, clients[client_num].request_index,
                                 (const char **)&method, &method_len, (const char **)&path, &path_len,
                                 &minor_version, headers, &num_headers, 0);
    if (pret > 0) {
        if (strncmp(method, "GET", method_len) != 0) {
            disconnectClient(client_num);
            return;
        }
        size_t url_len = path_len;
        //fprintf(stderr, "calloc for url in client %d expected size %lu\n", client_num, path_len);
        char *url = (char *)calloc(url_len, sizeof(char));
        if (url == NULL) {
            disconnectClient(client_num);
            return;
        }
        memcpy(url, path, path_len);

        bool is_request_in_cache;
        int cache_index = findAtCache(url, url_len, &is_request_in_cache);
        if (is_request_in_cache) {
            addSubscriber(client_num, cache_index);
            clients[client_num].cache_index = cache_index;
            clients[client_num].write_response_index = 0;
            if (cache_table[cache_index].response_index != 0) {
                changeEventForFd(clients[client_num].fd, POLLIN | POLLOUT);
            }
            shiftRequest(client_num, pret);
            free(url);
            return;
        }
        char *host = NULL;
        for (size_t i = 0; i < num_headers; i++) {
            if (strncmp(headers[i].name, "Host", 4) == 0) {
                host = calloc(headers[i].value_len + 1, sizeof(char));
                if (host == NULL) {
                    free(url);
                    disconnectClient(client_num);
                    return;
                }
                memcpy(host, headers[i].value, headers[i].value_len);
                break;
            }
        }
        if (host == NULL) {
            free(url);
            disconnectClient(client_num);
            return;
        }
        cache_table[cache_index].valid = true;
        cache_table[cache_index].request = (char *)calloc(pret, sizeof(char));

        if (cache_table[cache_index].request == NULL) {
            disconnectClient(client_num);
            free(url);
            free(host);
            cache_table[cache_index].valid = false;
            return;
        }
        memcpy(cache_table[cache_index].request, clients[client_num].request, pret);
        cache_table[cache_index].REQUEST_SIZE = pret;
        shiftRequest(client_num, pret);
        int server_fd = connectToServerHost(host, 80);
        if (server_fd == -1) {
            fprintf(stderr, "failed to connect to remote host: %s\n", host);
            disconnectClient(client_num);
        
            freeCacheRecord(cache_index);
            free(host);
            free(url);
            return;
        }
        int fcntl_res = fcntl(server_fd, F_SETFL, O_NONBLOCK);
        if (fcntl_res < 0) {
            perror("make new server fd nonblock");
            int close_res = close(server_fd);
            if (close_res < 0) {
                fprintf(stderr, "client %d ", client_num);
                perror("close");
            }
            disconnectClient(client_num);
        
            freeCacheRecord(cache_index);
            free(host);
            free(url);
            return;
        }
        int server_num = findFreeServer(server_fd);
        servers[server_num].cache_index = cache_index;
        servers[server_num].write_request_index = 0;

        cache_table[cache_index].server_index = server_num;
        addSubscriber(client_num, cache_index);
        cache_table[cache_index].url = url;
        cache_table[cache_index].URL_LEN = url_len;

        clients[client_num].cache_index = cache_index;
        clients[client_num].write_response_index = 0;
        addFdToPollFds(server_fd, POLLIN | POLLOUT);
        free(host);
    }
    else if (pret == -1) {
        disconnectClient(client_num);
    }
}

void writeToServer(int server_num) {
    if (server_num < 0 || server_num >= SERVERS_SIZE) {
        return;
    }
    ssize_t written = write(servers[server_num].fd,
                            &cache_table[servers[server_num].cache_index].
                                request[servers[server_num].write_request_index],
                            cache_table[servers[server_num].cache_index].REQUEST_SIZE -
                            servers[server_num].write_request_index);
    
    if (written < 0) {
        fprintf(stderr, "error in server %d ", server_num);
        perror("write");
        disconnectServer(server_num);
        return;
    }
    average_write_size_to_server = (average_write_size_to_server * num_writes_server + (double)written) /
            (num_writes_server + 1);
    num_writes_server += 1;
    servers[server_num].write_request_index += (int)written;
    if (servers[server_num].write_request_index == cache_table[servers[server_num].cache_index].REQUEST_SIZE) {
        changeEventForFd(servers[server_num].fd, POLLIN);
    }
}

void readFromServer(int server_num) {
    if (server_num < 0 || server_num >= SERVERS_SIZE) {
        return;
    }
    char buf[BUFSIZ];
    ssize_t was_read = read(servers[server_num].fd, buf, BUFSIZ);
    if (was_read < 0) {
        fprintf(stderr, "error in server %d ", server_num);
        perror("read");
        return;
    }
    else if (was_read == 0) {
        cache_table[servers[server_num].cache_index].full = true;
        cache_table[servers[server_num].cache_index].response = realloc(
                cache_table[servers[server_num].cache_index].response,
                cache_table[servers[server_num].cache_index].response_index * sizeof(char));
        cache_table[servers[server_num].cache_index].RESPONSE_SIZE = cache_table[servers[server_num].cache_index].response_index;
        notifySubscribers(servers[server_num].cache_index, POLLIN | POLLOUT);
        disconnectServer(server_num);
        return;
    }
    if (cache_table[servers[server_num].cache_index].RESPONSE_SIZE == 0) {
        cache_table[servers[server_num].cache_index].RESPONSE_SIZE = START_RESPONSE_SIZE;
        cache_table[servers[server_num].cache_index].response = (char *)calloc(
                cache_table[servers[server_num].cache_index].RESPONSE_SIZE, sizeof(char));
        if (cache_table[servers[server_num].cache_index].response == NULL) {
            disconnectServer(server_num);
        }
    }
    if (was_read + cache_table[servers[server_num].cache_index].response_index >=
        cache_table[servers[server_num].cache_index].RESPONSE_SIZE) {
        cache_table[servers[server_num].cache_index].RESPONSE_SIZE *= 4;
        cache_table[servers[server_num].cache_index].response = realloc(
                cache_table[servers[server_num].cache_index].response,
                cache_table[servers[server_num].cache_index].RESPONSE_SIZE * sizeof(char));
    }
    memcpy(&cache_table[servers[server_num].cache_index].
        response[cache_table[servers[server_num].cache_index].response_index], buf, was_read);
    size_t prev_len = cache_table[servers[server_num].cache_index].response_index;
    cache_table[servers[server_num].cache_index].response_index += was_read;
    int minor_version, status;
    char *msg;
    size_t msg_len;
    size_t num_headers = 100;
    struct phr_header headers[num_headers];
    int pret = phr_parse_response(cache_table[servers[server_num].cache_index].response,
                                  cache_table[servers[server_num].cache_index].response_index,
                                  &minor_version, &status, (const char **)&msg, &msg_len, headers,
                                  &num_headers, prev_len);
    notifySubscribers(servers[server_num].cache_index, POLLIN | POLLOUT);
    if (pret > 0) {
        if (status >= 200 && status < 300) {
            cache_table[servers[server_num].cache_index].private = false;
        }
    }
}

void writeToClient(int client_num) {
    if (client_num < 0 || client_num >= CLIENTS_SIZE) {
        return;
    }
    if (cache_table[clients[client_num].cache_index].server_index == -1 &&
        !cache_table[clients[client_num].cache_index].full) {
        int cache_index = clients[client_num].cache_index;
        freeCacheRecord(cache_index);
        return;
    }
    ssize_t written = write(clients[client_num].fd,
                            &cache_table[clients[client_num].cache_index].
                                response[clients[client_num].write_response_index],
                            cache_table[clients[client_num].cache_index].response_index -
                                clients[client_num].write_response_index);
    if (written < 0) {
        fprintf(stderr, "error in client %d ", client_num);
        perror("write");
        disconnectClient(client_num);
        return;
    }
    average_write_size_to_client = (average_write_size_to_client * num_writes_client + (double)written) /
            (num_writes_client + 1);
    num_writes_client += 1;
    clients[client_num].write_response_index += (int)written;
    if (clients[client_num].write_response_index == cache_table[clients[client_num].cache_index].response_index) {
        changeEventForFd(clients[client_num].fd, POLLIN);
    }
    if (cache_table[clients[client_num].cache_index].private && cache_table[clients[client_num].cache_index].full) {
        freeCacheRecord(clients[client_num].cache_index);
        clients[client_num].cache_index = -1;
    }

}

static void sigCatch(int sig) {
    if (sig == SIGINT) {
        if (WRITE_STOP_FD != -1) {
            char a = 'a';
            write(WRITE_STOP_FD, &a, 1);
            close(WRITE_STOP_FD);
            WRITE_STOP_FD = -1;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Error wrong amount of arguments\n");
        exit(ERROR_INVALID_ARGS);
    }
    char *invalid_sym;
    errno = 0;
    int port = (int)strtol(argv[1], &invalid_sym, 10);
    if (errno != 0 || *invalid_sym != '\0') {
        fprintf(stderr, "Error wrong port\n");
        exit(ERROR_PORT_CONVERSATION);
    }

    initPollFds();

    int pipe_fds[2];
    int pipe_res = pipe(pipe_fds);
    if (pipe_res != 0) {
        perror("pipe:");
        exit(ERROR_PIPE_OPEN);
    }
    READ_STOP_FD = pipe_fds[0];
    WRITE_STOP_FD = pipe_fds[1];
    addFdToPollFds(READ_STOP_FD, POLLIN);

    struct sigaction sig_act = { 0 };
    sig_act.sa_handler = sigCatch;
    sigemptyset(&sig_act.sa_mask);
    int sigact_res = sigaction(SIGINT, &sig_act, NULL);
    if (sigact_res != 0) {
        perror("sigaction");
        cleanUp();
        exit(ERROR_SIG_HANDLER_INIT);
    }

    initClients();
    initServers();
    initCacheTable();

    int listen_fd = initListener(port);
    addFdToPollFds(listen_fd, POLLIN);

    while (1) {
        int poll_res = poll(poll_fds, poll_last_index, TIMEOUT * 1000);
        if (poll_res < 0) {
            perror("poll");
            break;
        }
        else if (poll_res == 0) {
            fprintf(stdout, "proxy timeout\n");
            break;
        }
        int num_handled_fd = 0;
        size_t i = 0;
        size_t prev_last_index = poll_last_index;
        while (num_handled_fd < poll_res && i < prev_last_index) {
            if (poll_fds[i].fd == READ_STOP_FD && (poll_fds[i].revents & POLLIN)) {
                cleanUp();
                exit(0);
            }
            if (poll_fds[i].fd == listen_fd && (poll_fds[i].revents & POLLIN)) {
                acceptNewClient(listen_fd);
                num_handled_fd += 1;
                i += 1;
                continue;
            }

            int client_num = findClientByFd(poll_fds[i].fd);
            int server_num = -1;
            if (client_num == -1) {
                server_num = findServerByFd(poll_fds[i].fd);
            }

            bool handled = false;
            if (poll_fds[i].revents & POLLIN) {
                if (client_num != -1) {
                    readFromClient(client_num);
                }
                else if (server_num != -1) {
                    readFromServer(server_num);
                }
                handled = true;
            }
            if (poll_fds[i].revents & POLLOUT) {
                if (client_num != -1) {
                    writeToClient(client_num);
                }
                else if (server_num != -1) {
                    writeToServer(server_num);
                }
                handled = true;
            }
            if (poll_fds[i].fd >= 0 && client_num == -1 && server_num == -1 && poll_fds[i].fd != listen_fd &&
                poll_fds[i].fd != READ_STOP_FD) {
                removeFromPollFds(poll_fds[i].fd);
            }
            if (handled) {
                num_handled_fd += 1;
            }
            i += 1;
        }
    }
    cleanUp();
    return 0;
}
