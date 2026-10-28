/* Fixed-work, integer CPU workload for tests/linux_performance.py. */
#define _GNU_SOURCE
#include <inttypes.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <time.h>

static double seconds(clockid_t clock) {
    struct timespec t;
    if (clock_gettime(clock, &t)) exit(2);
    return t.tv_sec + t.tv_nsec / 1e9;
}

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    uint64_t loops = strtoull(argv[1], NULL, 10), value = 1;
    cpu_set_t cpus;
    if (sched_getaffinity(0, sizeof(cpus), &cpus)) return 2;
    double cpu = seconds(CLOCK_PROCESS_CPUTIME_ID);
    for (uint64_t i = 0; i < loops; ++i) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
    }
    cpu = seconds(CLOCK_PROCESS_CPUTIME_ID) - cpu;
    printf("{\"checksum\":\"%" PRIu64 "\",\"cpu_seconds\":%.9f,"
           "\"nice\":%d,\"policy\":%d,\"affinity\":[", value, cpu,
           getpriority(PRIO_PROCESS, 0), sched_getscheduler(0));
    int comma = 0;
    for (int i = 0; i < CPU_SETSIZE; ++i) {
        if (CPU_ISSET(i, &cpus)) {
            printf("%s%d", comma ? "," : "", i);
            comma = 1;
        }
    }
    puts("]}");
    return 0;
}
