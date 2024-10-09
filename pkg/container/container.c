
#define _GNU_SOURCE

#include <stdio.h>
#include <unistd.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <linux/sched.h>
#include <sys/syscall.h>

#include <err.h>
#include <string.h>
#include <stdbool.h>

#define DEFAULT_HOSTNAME "container"

typedef struct {
        /* required */
        char *cmd;
        char *id;
        char *root;

        /* optional */
        char *hostname;
        bool detach;
} ContainerConfig;

#define STACK_LEN (sizeof(ContainerConfig) * 5)

int __container_init_run(void *cfg)
{
        printf("running in child\n");
        ContainerConfig *config = cfg;
        if (sethostname(config->hostname, strlen(config->hostname)) != 0) {
                err(1, "could not sethostname() to %s", config->hostname);
        }

        if (chroot(config->root) != 0) {
                err(1, "could not chroot() to %s", config->root);
        }

        if (chdir("/") != 0) {
                err(1, "could not chdir()");
        }

        if (mount("proc", "proc", "proc", 0, "") != 0) {
                err(1, "could not mount() proc");
        }

        execlp(config->cmd, NULL);
        return 0;
}

int verfiy_config(ContainerConfig *config)
{
        if (!config->cmd || !config->id || !config->root)
                return -1;

        if (config->hostname)
                config->hostname = DEFAULT_HOSTNAME;

        return 0;
}

int create_container(ContainerConfig *config)
{
        if (verfiy_config(config) != 0) {
                return -1;
        }

        char stack[STACK_LEN];
        int pid = clone(__container_init_run, (void *)(stack + STACK_LEN - 1),
                        CLONE_NEWUTS | CLONE_NEWPID | SIGCHLD, config);
        if (pid < 0) {
                /* indicating an error code by clone() */
                return pid;
        }
        /* success */
        return 0;
}

int clone3(int flags)
{
        int stack_size = 1024 / sizeof(int);
        int stack[stack_size];
        struct clone_args args = {
                .flags = flags,
                .stack = (unsigned long long)stack,
                .stack_size = stack_size,
        };
        return syscall(SYS_clone3, &args, sizeof(args));
}

int main(int argc, char *argv[])
{
        ContainerConfig cfg = {
                .cmd = "/bin/bash",
                .id = "bueno",
                .root = "/var/lib/atlas/container",
                .detach = false,
                .hostname = "container",
        };

        create_container(&cfg);
        wait(NULL);

        /*
        int res = clone(&child, (void *)(stack + STACK_LEN - 1),
                        CLONE_NEWUTS | CLONE_NEWPID | SIGCHLD, NULL);
        if (res < 0) {
                err(1, "could not clone()");
        }
        wait(NULL);
        */
        return 0;
}
