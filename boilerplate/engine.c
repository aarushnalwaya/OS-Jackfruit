/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "/tmp/container_logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Pipe reader thread arg */
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} pipe_reader_arg_t;

/* ---------------------------------------------------------------
 * Container metadata helpers
 * --------------------------------------------------------------- */
static container_record_t *container_record_new(const char *id,
                                                 pid_t host_pid,
                                                 unsigned long soft_limit_bytes,
                                                 unsigned long hard_limit_bytes)
{
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec)
        return NULL;

    strncpy(rec->id, id, CONTAINER_ID_LEN - 1);
    rec->host_pid         = host_pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_STARTING;
    rec->soft_limit_bytes = soft_limit_bytes;
    rec->hard_limit_bytes = hard_limit_bytes;
    rec->exit_code        = -1;
    rec->exit_signal      = 0;
    rec->next             = NULL;

    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, id);
    return rec;
}

static void container_list_add(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next       = ctx->containers;
    ctx->containers = rec;
}

static container_record_t *container_list_find(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, CONTAINER_ID_LEN) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

/* ---------------------------------------------------------------
 * Usage
 * --------------------------------------------------------------- */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

/* ---------------------------------------------------------------
 * Flag parsing
 * --------------------------------------------------------------- */
static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded buffer
 * --------------------------------------------------------------- */
static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * Logging consumer thread
 * --------------------------------------------------------------- */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char path[PATH_MAX];
        snprintf(path, PATH_MAX, "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }
        ssize_t written = write(fd, item.data, item.length);
        if (written < 0)
            perror("logging_thread: write");
        close(fd);
    }
    return NULL;
}

/* ---------------------------------------------------------------
 * Pipe reader thread (producer): reads container stdout/stderr
 * and pushes chunks into the bounded buffer.
 * --------------------------------------------------------------- */
static void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *pr = (pipe_reader_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pr->container_id, CONTAINER_ID_LEN - 1);

        n = read(pr->read_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0)
            break;

        item.length = (size_t)n;
        bounded_buffer_push(pr->log_buffer, &item);
    }

    close(pr->read_fd);
    free(pr);
    return NULL;
}

/* ---------------------------------------------------------------
 * Child entrypoint (runs inside the cloned container)
 * --------------------------------------------------------------- */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (cfg == NULL)
        return 1;

    /* 1) Redirect stdout and stderr to the log pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* 2) chroot into the container's rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("child_fn: chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("child_fn: chdir");
        return 1;
    }

    /* 3) Mount /proc */
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("child_fn: mount /proc");

    /* 4) Set nice value */
    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) != 0)
            perror("child_fn: setpriority");
    }

    /* 5) Split command string into argv and execv */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, CHILD_COMMAND_LEN - 1);

    char *args[64];
    int nargs = 0;
    char *token = strtok(cmd_copy, " ");
    while (token && nargs < 63) {
        args[nargs++] = token;
        token = strtok(NULL, " ");
    }
    args[nargs] = NULL;

    if (nargs == 0) {
        fprintf(stderr, "child_fn: empty command\n");
        return 1;
    }

    execv(args[0], args);
    perror("child_fn: execv");
    return 1;
}

/* ---------------------------------------------------------------
 * Monitor ioctl helpers
 * --------------------------------------------------------------- */
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ---------------------------------------------------------------
 * Signal handlers
 * --------------------------------------------------------------- */
static supervisor_ctx_t *g_ctx = NULL;

static void sigchld_handler(int signo)
{
    (void)signo;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (g_ctx == NULL)
            continue;
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *cur = g_ctx->containers;
        while (cur) {
            if (cur->host_pid == pid) {
                if (WIFEXITED(status)) {
                    cur->state     = CONTAINER_EXITED;
                    cur->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    cur->state       = CONTAINER_KILLED;
                    cur->exit_signal = WTERMSIG(status);
                }
                break;
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int signo)
{
    (void)signo;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * Helper: spawn a container (shared by CMD_START and CMD_RUN)
 * Returns the child pid on success, -1 on failure.
 * pipefd[0] is the read end left open for the caller to drain.
 * --------------------------------------------------------------- */
static pid_t spawn_container(supervisor_ctx_t *ctx,
                              const control_request_t *req,
                              int pipefd[2])
{
    if (pipe(pipefd) < 0) {
        perror("spawn_container: pipe");
        return -1;
    }

    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    char *stack_top = stack + STACK_SIZE;

    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack_top, flags, cfg);

    /* Close write end in parent regardless */
    close(pipefd[1]);
    free(stack);
    free(cfg);

    if (pid < 0) {
        perror("spawn_container: clone");
        close(pipefd[0]);
        return -1;
    }

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, req->container_id,
                              pid, req->soft_limit_bytes, req->hard_limit_bytes);

    /* Add metadata record */
    container_record_t *rec = container_record_new(req->container_id, pid,
                                                    req->soft_limit_bytes,
                                                    req->hard_limit_bytes);
    if (rec) {
        rec->state = CONTAINER_RUNNING;
        pthread_mutex_lock(&ctx->metadata_lock);
        container_list_add(ctx, rec);
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    /* Spawn pipe reader thread */
    pipe_reader_arg_t *pr = calloc(1, sizeof(*pr));
    if (pr) {
        pr->read_fd    = pipefd[0];
        pr->log_buffer = &ctx->log_buffer;
        strncpy(pr->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pthread_t reader_tid;
        pthread_create(&reader_tid, NULL, pipe_reader_thread, pr);
        pthread_detach(reader_tid);
        /* pipefd[0] is now owned by the reader thread */
        pipefd[0] = -1;
    } else {
        close(pipefd[0]);
        pipefd[0] = -1;
    }

    return pid;
}

/* ---------------------------------------------------------------
 * Supervisor event loop
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    /* Install signal handlers before anything else */
    g_ctx = &ctx;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = sigterm_handler;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    mkdir(LOG_DIR, 0755);

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        perror("open /dev/container_monitor"); /* non-fatal */

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto cleanup; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen"); goto cleanup;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create"); goto cleanup; }

    fprintf(stderr, "[supervisor] started, rootfs=%s, socket=%s\n",
            rootfs, CONTROL_PATH);

    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        control_request_t req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        switch (req.kind) {

        /* ----- CMD_START: launch and return immediately ----- */
        case CMD_START: {
            pthread_mutex_lock(&ctx.metadata_lock);
            int dup = (container_list_find(&ctx, req.container_id) != NULL);
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (dup) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' already exists", req.container_id);
                break;
            }

            int pipefd[2];
            pid_t pid = spawn_container(&ctx, &req, pipefd);
            if (pid < 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message), "Failed to start container");
                break;
            }

            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' started, pid=%d", req.container_id, pid);
            break;
        }

        /* ----- CMD_RUN: launch and block until exit ----- */
        case CMD_RUN: {
            pthread_mutex_lock(&ctx.metadata_lock);
            int dup = (container_list_find(&ctx, req.container_id) != NULL);
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (dup) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' already exists", req.container_id);
                ssize_t w = write(client_fd, &resp, sizeof(resp));
                if (w < 0) perror("write response");
                close(client_fd);
                continue;
            }

            int pipefd[2];
            pid_t pid = spawn_container(&ctx, &req, pipefd);
            if (pid < 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message), "Failed to start container");
                ssize_t w = write(client_fd, &resp, sizeof(resp));
                if (w < 0) perror("write response");
                close(client_fd);
                continue;
            }

            /* Block until container exits */
            int wstatus;
            pid_t exited = waitpid(pid, &wstatus, 0);

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *r = container_list_find(&ctx, req.container_id);
            if (r) {
                if (WIFEXITED(wstatus)) {
                    r->state     = CONTAINER_EXITED;
                    r->exit_code = WEXITSTATUS(wstatus);
                } else if (WIFSIGNALED(wstatus)) {
                    r->state       = CONTAINER_KILLED;
                    r->exit_signal = WTERMSIG(wstatus);
                }
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (ctx.monitor_fd >= 0)
                unregister_from_monitor(ctx.monitor_fd, req.container_id, pid);

            resp.status = (exited > 0 && WIFEXITED(wstatus))
                          ? WEXITSTATUS(wstatus) : -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' exited with code %d",
                     req.container_id, resp.status);

            ssize_t w = write(client_fd, &resp, sizeof(resp));
            if (w < 0) perror("write response");
            close(client_fd);
            continue;
        }

        /* ----- CMD_PS ----- */
        case CMD_PS: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *cur = ctx.containers;
            char *p = resp.message;
            int rem = (int)sizeof(resp.message);
            while (cur && rem > 1) {
                int written = snprintf(p, rem, "%s %s pid=%d\n",
                                       cur->id,
                                       state_to_string(cur->state),
                                       cur->host_pid);
                p   += written;
                rem -= written;
                cur  = cur->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;
            break;
        }

        /* ----- CMD_STOP ----- */
        case CMD_STOP: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = container_list_find(&ctx, req.container_id);
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (!rec) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' not found", req.container_id);
                break;
            }

            kill(rec->host_pid, SIGTERM);
            rec->state = CONTAINER_STOPPED;

            if (ctx.monitor_fd >= 0)
                unregister_from_monitor(ctx.monitor_fd,
                                        req.container_id, rec->host_pid);

            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' stopped", req.container_id);
            break;
        }

        /* ----- CMD_LOGS ----- */
        case CMD_LOGS: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = container_list_find(&ctx, req.container_id);
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (!rec) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' not found", req.container_id);
                break;
            }
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Log file: %s", rec->log_path);
            break;
        }

        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "Unknown command");
            break;
        }

        ssize_t w = write(client_fd, &resp, sizeof(resp));
        if (w < 0) perror("write response");
        close(client_fd);
    }

cleanup:
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/* ---------------------------------------------------------------
 * Client-side: send a control request to the supervisor
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is the supervisor running?");
        close(fd);
        return 1;
    }

    ssize_t w = write(fd, req, sizeof(*req));
    if (w != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    control_response_t resp;
    ssize_t n = read(fd, &resp, sizeof(resp));
    close(fd);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Short response from supervisor\n");
        return 1;
    }

    printf("%s\n", resp.message);
    return resp.status == 0 ? 0 : 1;
}

/* ---------------------------------------------------------------
 * CLI command handlers
 * --------------------------------------------------------------- */
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ---------------------------------------------------------------
 * main
 * --------------------------------------------------------------- */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}