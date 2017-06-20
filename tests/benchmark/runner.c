#include <stdio.h>
#include <getopt.h>
#include "art.c"

#ifndef TIMER_T
#include <sys/time.h>
#define TIMER_T                         struct timeval
#define TIMER_READ(time)                gettimeofday(&(time), NULL)
#define TIMER_DIFF_SECONDS(start, stop) \
        (((double)(stop.tv_sec)  + (double)(stop.tv_usec / 1000000.0)) - \
              ((double)(start.tv_sec) + (double)(start.tv_usec / 1000000.0)))
#endif /* TIMER_T */

void *pmemaddr;
size_t mapped_len;
int is_pmem;
VMEM *vmem;

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

# define no_argument        0
# define required_argument  1
# define optional_argument  2

#define DEFAULT_SEED         1234
#define DEFAULT_TREE_SIZE    10000
#define DEFAULT_RANGE        1000000
#define DEFAULT_DURATION     1000000
#define DEFAULT_UPDATE_RATE  50

// TODO: parameters
uintptr_t BENCH_SEED        = DEFAULT_SEED;
uintptr_t BENCH_TREE_SIZE   = DEFAULT_TREE_SIZE;
uintptr_t BENCH_RANGE       = DEFAULT_RANGE;
uintptr_t BENCH_DURATION    = DEFAULT_DURATION;
uintptr_t BENCH_UPDATE_RATE = DEFAULT_UPDATE_RATE;

static void benchmark_lookups();
static void create_pmem_vmem();
static void destroy_vmem_pmem();

int main(int argc, char *argv[])
{
    int i, c;
	struct option long_options[] = {
        // These options don't set a flag
        {"help",                no_argument,       NULL, 'h'},
        {"update-rate",         required_argument, NULL, 'u'},
        {"seed",                required_argument, NULL, 's'},
        {"tree-size",           required_argument, NULL, 't'},
        {"range",               required_argument, NULL, 'r'},
        {"duration",            required_argument, NULL, 'd'},
        {NULL, 0, NULL, 0}
    };
	
	while(1) {
        i = 0;
        c = getopt_long(argc, argv, "hu:s:t:r:d:", long_options, &i);

        if(c == -1)
            break;

        if(c == 0 && long_options[i].flag == 0)
            c = long_options[i].val;

        switch(c) {
        case 0:
            /* Flag is automatically set */
            break;
        case 'h':
            printf("bank -- STM stress test "
"\n"
"Usage:\n"
"  bank [options...]\n"
"\n"
"Options:\n"
"  -h, --help\n"
"        Print this message\n"
"  -u, --update-rate <int>\n"
"        Percentage of updates (default=" XSTR(DEFAULT_UPDATE_RATE) ")\n"
"  -s, --seed <int>\n"
"        Seed (default=" XSTR(DEFAULT_SEED) ")\n"
"  -t, --tree-size <int>\n"
"        Size of the tree (default=" XSTR(DEFAULT_TREE_SIZE) ")\n"
"  -r, --range <int>\n"
"        Key range (default=" XSTR(DEFAULT_RANGE) ")\n"
"  -d, --duration <int>\n"
"        Transfer limit (default=" XSTR(DEFAULT_DURATION) ")\n"
                );
            exit(EXIT_SUCCESS);
        case 'u':
            BENCH_UPDATE_RATE = atoi(optarg);
            break;
        case 's':
            BENCH_SEED = atoi(optarg);
            break;
        case 't':
            BENCH_TREE_SIZE = atoi(optarg);
            break;
        case 'r':
            BENCH_RANGE = atoi(optarg);
            break;
        case 'd':
            BENCH_DURATION = atoi(optarg);
            break;
        case '?':
            printf("Use -h or --help for help\n");
            exit(EXIT_SUCCESS);
        default:
            exit(EXIT_FAILURE);
        }
    }

    printf(" ####### PARAMETERS \n"
            "UPDATE_RATE =%lu\n"
            "       SEED =%lu\n"
            "  TREE_SIZE =%lu\n"
            "      RANGE =%lu\n"
            "   DURATION =%lu\n\n\n",
            BENCH_UPDATE_RATE, BENCH_SEED, BENCH_TREE_SIZE,
            BENCH_RANGE, BENCH_DURATION);

    // run the benchmark
    benchmark_lookups();

    return EXIT_SUCCESS;
}

static void create_pmem_vmem(){
	/* create a pmem file and memory map it */
	if ((pmemaddr = pmem_map_file(PATH, PMEM_LEN, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
			perror("pmem_map_file\n");
			exit(EXIT_FAILURE);
	}

	if ((vmem = vmem_create_in_region(pmemaddr, mapped_len)) == NULL){
		perror("vmem_create_in_region\n");
		exit(EXIT_FAILURE);
	}
}

static void destroy_vmem_pmem(){
	vmem_delete(vmem);
	pmem_unmap(pmemaddr, mapped_len);
}

static void benchmark_lookups()
{
    art_tree t;
    uintptr_t i, count_insert = 0, count_remove = 0, count_lookup = 0,
              count_fail_remove = 0, count_fail_insert = 0, count_fail_lookup = 0;
    TIMER_T start, end;
    create_pmem_vmem();

    art_tree_init(&t,pmemaddr, vmem);
    srand(BENCH_SEED);

    for (i = 0; i < BENCH_TREE_SIZE; ++i) {
        char buffer[32];
        uintptr_t str = i % BENCH_RANGE;
        sprintf(buffer, "%lu", str); // TODO: random size string and repetitions
        art_insert(&t, (unsigned char*)buffer, strlen(buffer)+1, (void*)(i+1));
    }

    TIMER_READ(start);

    for (i = 0; i < BENCH_DURATION; ++i) {
        char buffer[32];
        uintptr_t str = rand() % BENCH_RANGE;
        int is_lookup = (rand() % 100) > BENCH_UPDATE_RATE; // insert/remove or lookup
        int is_remove = rand() % 2; // insert or remove
        sprintf(buffer, "%lu", str);
        uintptr_t idx;
        if (is_lookup) { 
            idx = (uintptr_t)art_search(&t, (unsigned char*)buffer, strlen(buffer)+1);
            count_lookup++;
            if (idx == 0) {
                // not-found remove
                count_fail_lookup++;
            }
        } else if (is_remove) {
            // idx = (uintptr_t)art_search(&t, (unsigned char*)buffer, strlen(buffer)+1);
            idx = (uintptr_t)art_delete(&t, (unsigned char*)buffer, strlen(buffer)+1);
            count_remove++;
            if (idx == 0) {
                // not-found remove
                count_fail_remove++;
            }
        } else { // insert
            idx = (uintptr_t)art_insert(&t, (unsigned char*)buffer, strlen(buffer)+1, (void*)-1);
            count_insert++;
            if (idx != 0) {
                // repeated insert
                count_fail_insert++;
            }
        }
    }

    TIMER_READ(end);
    
    char res_buf[8192];
    FILE *fp = fopen("benchmark_info.txt", "w");

    sprintf(res_buf,
            " ###### BENCHMARK RESULTS #######\n"
            "time taken: %f s\n"
            "lookups=%lu, inserts=%lu, removes=%lu\n"
            "fail_lookups=%lu, fail_inserts=%lu, fail_removes=%lu\n"
            " ################################\n", 
            TIMER_DIFF_SECONDS(start, end), count_lookup,
            count_insert, count_remove, count_fail_lookup,
            count_fail_insert, count_fail_remove);
    fprintf(stdout, "%s", res_buf);
    fprintf(fp, "%s", res_buf);
    printf("see results in \"benchmark_info.txt\"\n");

    art_tree_destroy(&t); 
    destroy_vmem_pmem();
}

