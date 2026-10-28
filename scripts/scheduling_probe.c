/* Native macOS QoS/core-placement probe. Build with clang -O2 -pthread.
 * CPU samples on arm64 use the XNU userspace TPIDR_EL0 CPU-number encoding:
 * https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/arm64/machine_machdep.h
 * Compare IDs with IODeviceTree logical-cpu-id, not assumed CPU ordering.
 * These are execution-point samples, not a time-weighted residency percentage.
 * This diagnostic never changes affinity. Optional QoS affects only its threads.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <pthread/qos.h>
#include <sys/resource.h>
#include <libproc.h>
#include <unistd.h>
#include <time.h>

typedef struct { double seconds; int raised; uint64_t iterations, samples[64], unknown; } worker;
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec/1e9; }
static unsigned cpu_number(void) {
#if defined(__aarch64__)
    uint64_t value;
    __asm__ volatile("mrs %0, tpidr_el0" : "=r"(value));
    return value & 0xfff;
#else
    return 4096;
#endif
}
static void *burn(void *arg) {
    worker *w=arg;
    if(w->raised && pthread_set_qos_class_self_np(QOS_CLASS_USER_INITIATED,0)) return (void*)1;
    double start=now(); volatile uint64_t x=1;
    while(now()-start < w->seconds) {
        for(unsigned n=0;n<100000;n++) x=x*6364136223846793005ULL+1;
        w->iterations+=100000;
        unsigned cpu=cpu_number();
        if(cpu<64)w->samples[cpu]++;else w->unknown++;
    }
    return NULL;
}
int main(int argc,char **argv) {
    double seconds=argc>1?atof(argv[1]):2;
    int threads=argc>2?atoi(argv[2]):1;
    int raised=argc>3 && !strcmp(argv[3],"user-initiated");
    if(seconds<.1 || seconds>30 || threads<1 || threads>64) return 2;
    worker w[64]={0}; pthread_t tids[64];
    struct rusage_info_v4 before={0},after={0};
    if(proc_pid_rusage(getpid(),RUSAGE_INFO_V4,(rusage_info_t*)&before))return 3;
    double start=now();
    for(int i=0;i<threads;i++) {
        w[i].seconds=seconds; w[i].raised=raised;
        if(pthread_create(&tids[i],NULL,burn,&w[i]))return 4;
    }
    uint64_t iterations=0,samples[64]={0},unknown=0;
    for(int i=0;i<threads;i++) {
        void *error=NULL;
        if(pthread_join(tids[i],&error) || error)return 5;
        iterations+=w[i].iterations;unknown+=w[i].unknown;
        for(int c=0;c<64;c++)samples[c]+=w[i].samples[c];
    }
    double elapsed=now()-start;
    if(proc_pid_rusage(getpid(),RUSAGE_INFO_V4,(rusage_info_t*)&after))return 6;
    printf("{\"pid\":%d,\"threads\":%d,\"requested\":\"%s\",\"elapsed_s\":%.6f,\"iterations\":%llu,\"cpu_samples\":{",getpid(),threads,raised?"user_initiated":"inherited",elapsed,(unsigned long long)iterations);
    int first=1;
    for(int c=0;c<64;c++)if(samples[c]) {printf("%s\"%d\":%llu",first?"":",",c,(unsigned long long)samples[c]);first=0;}
    printf("},\"unknown_cpu_samples\":%llu,\"effective_qos_cpu_ticks\":{",(unsigned long long)unknown);
#define Q(field) printf("\"" #field "\":%llu,",(unsigned long long)(after.ri_cpu_time_qos_##field-before.ri_cpu_time_qos_##field))
    Q(default);Q(maintenance);Q(background);Q(utility);Q(legacy);Q(user_initiated);
    printf("\"user_interactive\":%llu}}\n",(unsigned long long)(after.ri_cpu_time_qos_user_interactive-before.ri_cpu_time_qos_user_interactive));
    return 0;
}
