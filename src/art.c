#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include "art.h"
#include "rdtsc.h"


#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
    #include <emmintrin.h>
#endif
#endif


#define CPU_MAX_FREQ 3800000


#define NOP_X10 __asm__ volatile("nop\n\t\nnop\n\t\nnop\n\t\nnop\n\t\nnop"\
		"\n\t\nnop\n\t\nnop\n\t\nnop\n\t\nnop\n\t\nnop" ::: "memory")

#define SPIN_10NOPS(nb_spins) ({ \
    volatile int i; \
    for (i = 0; i < nb_spins; ++i) NOP_X10; \
    i; \
})

#define SPIN_PER_WRITE(nb_writes) ({ \
    SPIN_10NOPS(SPINS_PER_100NS * nb_writes); \
    0; \
})


/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))
#define PAGE_OFFSET 12
#define PAGE_SIZE 5120
#define FLUSH_TIME 100e-6

int SPINS_PER_100NS;
void* pmemaddr;
VMEM* vmem;
intptr_t page;
void** operations;
int idx_op;


void num_loops(){
	unsigned long long ts1, ts2;
	double time;
	double ns100 = FLUSH_TIME;
	const unsigned int test = 99999999;
	ts1 = rdtscp();

	SPIN_10NOPS(test);

	ts2 = rdtscp();

	time = (double) (ts2 - ts1) / (double) CPU_MAX_FREQ;
	SPINS_PER_100NS = time / (double) test*(time/ns100) + 1; // round up
}


//method that returns current page
intptr_t memory_page(void *addr){


	intptr_t i=(intptr_t)addr;

	i=i>>PAGE_OFFSET;
	i=i<<PAGE_OFFSET;
	return i;
}

void persist(void* addr){

	for(int i=0; i<idx_op; i++){
		pmem_flush(operations[i], sizeof(void*));
		SPIN_PER_WRITE(1);
	}

	idx_op=0;
	page=memory_page(addr);
	pmem_flush((void*)page, sizeof(intptr_t));
	SPIN_PER_WRITE(1);
	pmem_drain();
	operations[idx_op++]=addr;
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(uint8_t type) {
    art_node* n;
    switch (type) {
        case NODE4:
            n = (art_node*)vmem_calloc(vmem, 1, sizeof(art_node4));
            break;
        case NODE16:
            n = (art_node*)vmem_calloc(vmem, 1, sizeof(art_node16));
            break;
        case NODE48:
            n = (art_node*)vmem_calloc(vmem, 1, sizeof(art_node48));
            break;
        case NODE256:
            n = (art_node*)vmem_calloc(vmem, 1, sizeof(art_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    if(memory_page((void*)n)!=page){
    	persist((void*)n);
    }else{
    	operations[idx_op++]=(void*)n;
    }
    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t, void* addr, VMEM* v) {
	pmemaddr=addr;
	vmem=v;

	t->root=(art_node*)vmem_malloc(vmem, sizeof(art_node));
	page=memory_page((void*)t->root);

	pmem_persist((void*)page, sizeof(intptr_t));
	SPIN_PER_WRITE(1);

	operations = vmem_malloc(vmem, PAGE_SIZE*sizeof(void*));
	idx_op=0;

    t->root = NULL;
    t->size = 0;
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        //free(LEAF_RAW(n));
        return;
    }

    // Handle each node type
    int i;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0;i<n->num_instructions;i++) {
                destroy_node(p.p1->children[i]);
            }
            break;

        case NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_instructions;i++) {
                destroy_node(p.p2->children[i]);
            }
            break;

        case NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<n->num_instructions;i++) {
                destroy_node(p.p3->children[i]);
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i=0;i<256;i++) {
                if (p.p4->children[i])
                    destroy_node(p.p4->children[i]);
            }
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    //free(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static art_node** find_child(art_node *n, unsigned char c) {
    int i, lo,hi,mid,found=0;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_instructions; i++) {
		/* this cast works around a bug in gcc 5.1 when unrolling loops
		 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
		 */
                if (((unsigned char*)p.p1->keys)[p.p1->offsets[i]] == c){
                	if(i<n->num_instructions-1){
                		if(((unsigned char*)p.p1->keys)[p.p1->offsets[i]] == ((unsigned char*)p.p1->keys)[p.p1->offsets[i+1]]){
                			i++;
                		}else
                			return &p.p1->children[p.p1->offsets[i]];
                	}
                	else
                		return &p.p1->children[p.p1->offsets[i]];
                }
            }
            break;

        {
        case NODE16:
            p.p2 = (art_node16*)n;
            found =0;
			lo=0;
			hi=n->num_instructions-1;

			while (lo <= hi){
				mid = lo + (hi-lo)/2;
				if (p.p2->keys[p.p2->offsets[mid]] == c){
					found=1;
					break;
				}
				if (p.p2->keys[p.p2->offsets[mid]] < c)
					lo = ++mid;
				else
					hi = --mid;
			}

			if(found == 1){
				int cont=0;

				for(i=mid-1; i >=0; i--){
					if(p.p2->keys[p.p2->offsets[i]] == c)
						cont++;
					else
						break;
				}

				for(i=mid+1; i<n->num_instructions; i++){
					if(p.p2->keys[p.p2->offsets[i]] == c)
						cont++;
					else
						break;
				}

				if(cont == 0)
					return &p.p2->children[p.p2->offsets[mid]];
				if((cont+1) % 2 != 0)
					return &p.p2->children[p.p2->offsets[i-1]];
			}
			break;
        }

        case NODE48:
            p.p3 = (art_node48*)n;
			found =0;
			lo=0;
			hi=n->num_instructions-1;

			while (lo <= hi){
				mid = lo + (hi-lo)/2;
				if (p.p3->keys[p.p3->offsets[mid]] == c){
					found=1;
					break;
				}
				if (p.p3->keys[p.p3->offsets[mid]] < c)
					lo = ++mid;
				else
					hi = --mid;
			}

			if(found == 1){
				int cont=0;

				for(i=mid-1; i >=0; i--){
					if(p.p3->keys[p.p3->offsets[i]] == c)
						cont++;
					else
						break;
				}

				for(i=mid+1; i<n->num_instructions; i++){
					if(p.p3->keys[p.p3->offsets[i]] == c)
						cont++;
					else
						break;
				}

				if(cont == 0)
					return &p.p3->children[p.p3->offsets[mid]];
				if((cont+1) % 2 != 0)
					return &p.p3->children[p.p3->offsets[i-1]];
			}
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            found =0;
			lo=0;
			hi=n->num_instructions-1;

			while (lo <= hi){
				mid = lo + (hi-lo)/2;
				if (p.p4->keys[p.p4->offsets[mid]] == c){
					found=1;
					break;
				}
				if (p.p4->keys[p.p4->offsets[mid]] < c)
					lo = ++mid;
				else
					hi = --mid;
			}

			if(found == 1){
				int cont=0;

				for(i=mid-1; i >=0; i--){
					if(p.p4->keys[p.p4->offsets[i]] == c)
						cont++;
					else
						break;
				}

				for(i=mid+1; i<n->num_instructions; i++){
					if(p.p4->keys[p.p4->offsets[i]] == c)
						cont++;
					else
						break;
				}

				if(cont == 0)
					return &p.p4->children[p.p4->offsets[mid]];
				if((cont+1) % 2 != 0)
					return &p.p4->children[p.p4->offsets[i-1]];
			}
            break;

        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned char *key, int key_len) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
                return ((art_leaf*)n)->value;
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}

// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    switch (n->type) {
        case NODE4:
            return minimum(((const art_node4*)n)->children[((const art_node4*)n)->offsets[0]]);
        case NODE16:
            return minimum(((const art_node16*)n)->children[((const art_node16*)n)->offsets[0]]);
        case NODE48:
            return minimum(((const art_node48*)n)->children[((const art_node48*)n)->offsets[0]]);
        case NODE256:
            return minimum(((const art_node256*)n)->children[((const art_node256*)n)->offsets[0]]);
        default:
            abort();
    }
}

// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    switch (n->type) {
		case NODE4:
			return maximum(((const art_node4*)n)->children[((const art_node4*)n)->offsets[n->num_instructions-1]]);
		case NODE16:
			return maximum(((const art_node16*)n)->children[((const art_node16*)n)->offsets[n->num_instructions-1]]);
		case NODE48:
			return maximum(((const art_node48*)n)->children[((const art_node48*)n)->offsets[n->num_instructions-1]]);
		case NODE256:
			return maximum(((const art_node256*)n)->children[((const art_node256*)n)->offsets[n->num_instructions-1]]);
		default:
			abort();
    }
}

/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
    return minimum((art_node*)t->root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    return maximum((art_node*)t->root);
}

static art_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    art_leaf *l = (art_leaf*)vmem_malloc(vmem, sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    if(memory_page((void*)l)!=page){
    	persist((void*)l);
    }else{
    	operations[idx_op]=(void*)l;
    }
    return l;
}

static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(l1->key_len, l2->key_len) - depth;
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}

static void copy_header(art_node *dest, art_node *src) {
    dest->num_instructions = src->num_instructions;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static art_node* clean_node(art_node *n){

	int i;
	union {
		art_node4 *p1;
		art_node16 *p2;
		art_node48 *p3;
		art_node256 *p4;
	} p;

	art_node *new_node;


	switch(n->type){
	case NODE4:
		p.p1=(art_node4*)n;
		new_node=alloc_node(NODE4);
		copy_header(new_node, n);
		new_node->num_instructions=0;

		for(i=0;i<n->num_instructions;i++){
			if(i < n->num_instructions-1){
				if(p.p1->keys[p.p1->offsets[i]] == p.p1->keys[p.p1->offsets[i+1]]){
					++i;
				}else{
					memcpy(&((art_node4*)new_node)->children[new_node->num_instructions],&p.p1->children[p.p1->offsets[i]], sizeof(void*)*1);
					memcpy(&((art_node4*)new_node)->keys[new_node->num_instructions],&p.p1->keys[p.p1->offsets[i]],sizeof(unsigned char)*1);
					((art_node4*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
					new_node->num_instructions++;
				}
			}else{
				memcpy(&((art_node4*)new_node)->children[new_node->num_instructions],&p.p1->children[p.p1->offsets[i]], sizeof(void*)*1);
				memcpy(&((art_node4*)new_node)->keys[new_node->num_instructions],&p.p1->keys[p.p1->offsets[i]],sizeof(unsigned char)*1);
				((art_node4*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
				new_node->num_instructions++;
			}

			if(n->num_instructions - n->num_remotions - 1 == new_node->num_instructions)
				break;
		}
		return new_node;
	case NODE16:
			p.p2=(art_node16*)n;
			new_node=alloc_node(NODE16);
			copy_header(new_node, n);
			new_node->num_instructions=0;

			for(i=0;i<n->num_instructions;i++){
				if(i < n->num_instructions-1){
					if(p.p2->keys[p.p2->offsets[i]] == p.p2->keys[p.p2->offsets[i+1]]){
						i++;
					}else{
						memcpy(&((art_node16*)new_node)->children[new_node->num_instructions],&p.p2->children[p.p2->offsets[i]], sizeof(void*)*1);
						memcpy(&((art_node16*)new_node)->keys[new_node->num_instructions],&p.p2->keys[p.p2->offsets[i]],sizeof(unsigned char)*1);
						((art_node16*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
						new_node->num_instructions++;
					}
				}else{
					memcpy(&((art_node16*)new_node)->children[new_node->num_instructions],&p.p2->children[p.p2->offsets[i]], sizeof(void*)*1);
					memcpy(&((art_node16*)new_node)->keys[new_node->num_instructions],&p.p2->keys[p.p2->offsets[i]],sizeof(unsigned char)*1);
					((art_node16*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
					new_node->num_instructions++;
				}

				if(n->num_instructions - n->num_remotions - 1 == new_node->num_instructions)
					break;
			}
			return new_node;
	case NODE48:
			p.p3=(art_node48*)n;
			new_node=alloc_node(NODE48);
			copy_header(new_node, n);
			new_node->num_instructions=0;

			for(i=0;i<n->num_instructions;i++){
				if(i < n->num_instructions-1){
					if(p.p3->keys[p.p3->offsets[i]] == p.p3->keys[p.p3->offsets[i+1]]){
						i++;
					}else{
						memcpy(&((art_node48*)new_node)->children[new_node->num_instructions],&p.p3->children[p.p3->offsets[i]], sizeof(void*)*1);
						memcpy(&((art_node48*)new_node)->keys[new_node->num_instructions],&p.p3->keys[p.p3->offsets[i]],sizeof(unsigned char)*1);
						((art_node48*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
						new_node->num_instructions++;
					}
				}else{
					memcpy(&((art_node48*)new_node)->children[new_node->num_instructions],&p.p3->children[p.p3->offsets[i]], sizeof(void*)*1);
					memcpy(&((art_node48*)new_node)->keys[new_node->num_instructions],&p.p3->keys[p.p3->offsets[i]],sizeof(unsigned char)*1);
					((art_node48*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
					new_node->num_instructions++;
				}

				if(n->num_instructions - n->num_remotions - 1 == new_node->num_instructions)
					break;
			}

			return new_node;
	case NODE256:
			p.p4=(art_node256*)n;
			new_node=alloc_node(NODE256);
			copy_header(new_node, n);
			new_node->num_instructions=0;

			for(i=0;i<n->num_instructions;i++){
				if(i < n->num_instructions-1){
					if(p.p4->keys[p.p4->offsets[i]] == p.p4->keys[p.p4->offsets[i+1]]){
						i++;
					}else{
						memcpy(&((art_node256*)new_node)->children[new_node->num_instructions],&p.p4->children[p.p4->offsets[i]], sizeof(void*)*1);
						memcpy(&((art_node256*)new_node)->keys[new_node->num_instructions],&p.p4->keys[p.p4->offsets[i]],sizeof(unsigned char)*1);
						((art_node256*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
						new_node->num_instructions++;
					}
				}else{
					memcpy(&((art_node256*)new_node)->children[new_node->num_instructions],&p.p4->children[p.p4->offsets[i]], sizeof(void*)*1);
					memcpy(&((art_node256*)new_node)->keys[new_node->num_instructions],&p.p4->keys[p.p4->offsets[i]],sizeof(unsigned char)*1);
					((art_node256*)new_node)->offsets[new_node->num_instructions]=new_node->num_instructions;
					new_node->num_instructions++;
				}

				if(n->num_instructions - n->num_remotions - 1 == new_node->num_instructions)
					break;
			}
			return new_node;
	}
	return NULL;

}


static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {
    (void)ref;
    if(n->n.num_instructions < 256){
    	n->children[n->n.num_instructions]=(art_node*)child;
    	n->keys[n->n.num_instructions]=c;
    	n->offsets[n->n.num_instructions]=n->n.num_instructions;
    	for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
    	n->n.num_instructions++;

    	operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
    	operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
    	operations[idx_op++]=(void*)&n->n.num_instructions;
    }else{
    	if(n->n.num_remotions > 0){
    		*ref=clean_node((art_node*)n);
    		pmem_flush(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
    		add_child256((art_node256*)*ref, ref, c, child);
    	}
    }
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {

    if (n->n.num_instructions < 48) {

        n->children[n->n.num_instructions] = (art_node*)child;
        n->keys[n->n.num_instructions] = c;
        n->offsets[n->n.num_instructions]=n->n.num_instructions;
        for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
        n->n.num_instructions++;
        operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_instructions;
    } else {
    	if(n->n.num_remotions > 0){
    		*ref=clean_node((art_node*)n);
    		pmem_flush(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
    		add_child48((art_node48*)*ref, ref, c, child);
    	}
    	else{
			art_node256 *new_node = (art_node256*)alloc_node(NODE256);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(new_node, sizeof(art_node*));
			pmem_persist(*ref, sizeof(art_node*));
			SPIN_PER_WRITE(2);
			//free(n);
			add_child256(new_node, ref, c, child);
    	}
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {

	if (n->n.num_instructions < 16) {

		n->children[n->n.num_instructions] = (art_node*)child;
		n->keys[n->n.num_instructions] = c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_instructions;
	} else {
		if(n->n.num_remotions > 0){
			*ref=clean_node((art_node*)n);
    		pmem_flush(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			add_child16((art_node16*)*ref, ref, c, child);
		}else{
			art_node48 *new_node = (art_node48*)alloc_node(NODE48);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(new_node, sizeof(art_node*));
			pmem_persist(*ref, sizeof(art_node*));
			SPIN_PER_WRITE(2);
			//free(n);
			add_child48(new_node, ref, c, child);
		}
	}
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
	if (n->n.num_instructions < 4) {

		n->children[n->n.num_instructions] = (art_node*)child;
		n->keys[n->n.num_instructions] = c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_instructions;
	} else {
		if(n->n.num_remotions > 0){
			*ref=clean_node((art_node*)n);
    		pmem_flush(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			add_child4((art_node4*)*ref, ref, c, child);
		}else{
			art_node16 *new_node = (art_node16*)alloc_node(NODE16);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(new_node, sizeof(art_node*));
			pmem_persist(*ref, sizeof(art_node*));
			SPIN_PER_WRITE(2);
			//free(n);
			add_child16(new_node, ref, c, child);
		}
	}
}

static void add_child(art_node *n, art_node **ref, unsigned char c, void *child) {
    switch (n->type) {
        case NODE4:
            return add_child4((art_node4*)n, ref, c, child);
        case NODE16:
            return add_child16((art_node16*)n, ref, c, child);
        case NODE48:
            return add_child48((art_node48*)n, ref, c, child);
        case NODE256:
            return add_child256((art_node256*)n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        art_leaf *l = minimum(n);
        max_cmp = min(l->key_len, key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}

static void* recursive_insert(art_node *n, art_node **ref, const unsigned char *key, int key_len, void *value, int depth, int *old) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        *ref = (art_node*)SET_LEAF(make_leaf(key, key_len, value));
        pmem_persist(*ref, sizeof(art_node*));
        SPIN_PER_WRITE(1);
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value
        if (!leaf_matches(l, key, key_len, depth)) {
            *old = 1;
            void *old_val = l->value;
            l->value = value;
            return old_val;
        }

        // New value, we must split the leaf into a node4
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);

        // Create a new leaf
        art_leaf *l2 = make_leaf(key, key_len, value);

        // Determine longest prefix
        int longest_prefix = longest_common_prefix(l, l2, depth);
        new_node->n.partial_len = longest_prefix;
        memcpy(new_node->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));

        add_child4(new_node, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        add_child4(new_node, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
        // Add the leafs to the new node4
        *ref = (art_node*)new_node;
        pmem_persist(new_node, sizeof(art_node*));
        pmem_persist(*ref, sizeof(art_node*));
        SPIN_PER_WRITE(2);
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_node;
        new_node->n.partial_len = prefix_diff;
        memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_node, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_node, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        }
        pmem_persist(new_node, sizeof(art_node*));
        pmem_persist(*ref, sizeof(art_node*));
        SPIN_PER_WRITE(2);

        // Insert the new leaf
        art_leaf *l = make_leaf(key, key_len, value);
        add_child4(new_node, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert(*child, child, key, key_len, value, depth+1, old);
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(key, key_len, value);
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned char *key, int key_len, void *value) {
    int old_val = 0;
    void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, unsigned char c) {

	if(n->n.num_instructions < 257){
		art_node **child=find_child((art_node*)n, c);

		if(!child)
			return;
		int pos=child-n->children;
		tombstone* g=vmem_calloc(vmem, 1, sizeof(tombstone));
		g->dead=n->children[pos];
		operations[idx_op++]=(void*)g;
		operations[idx_op++]=(void*)g->dead;

		n->children[n->n.num_instructions]=(void*)g;
		n->keys[n->n.num_instructions]=c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		n->n.num_remotions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_remotions;
		operations[idx_op++]=(void*)&n->n.num_instructions;
		if(n->n.num_instructions - n->n.num_remotions*2 == 37){
			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			art_node48 *new_node = (art_node48*)alloc_node(NODE48);
			copy_header((art_node*)new_node, *ref);
			memcpy(new_node->children, ((art_node256*)*ref)->children, sizeof(void*)*((art_node256*)*ref)->n.num_instructions);
			memcpy(new_node->keys, ((art_node256*)*ref)->keys, sizeof(unsigned char)*((art_node256*)*ref)->n.num_instructions);
			memcpy(new_node->offsets, ((art_node256*)*ref)->offsets, sizeof(int)*((art_node256*)*ref)->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
		}
	}else{
		if(n->n.num_remotions > 0){
			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child256((art_node256*)*ref, ref, c);
		}
	}
}

static void remove_child48(art_node48 *n, art_node **ref, unsigned char c) {

	if (n->n.num_instructions < 48) {
		art_node **child=find_child((art_node*)n, c);

		if(!child)
			return;
		int pos=child-n->children;
		tombstone* g=vmem_calloc(vmem, 1, sizeof(tombstone));
		g->dead=n->children[pos];
		operations[idx_op++]=(void*)g;
		operations[idx_op++]=(void*)g->dead;

		n->children[n->n.num_instructions] = (void*)g;
		n->keys[n->n.num_instructions] = c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		n->n.num_remotions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_remotions;
		operations[idx_op++]=(void*)&n->n.num_instructions;

		if(n->n.num_instructions - n->n.num_remotions*2 == 12){
			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			art_node16 *new_node = (art_node16*)alloc_node(NODE16);
			copy_header((art_node*)new_node, *ref);
			memcpy(new_node->children, ((art_node48*)*ref)->children, sizeof(void*)*((art_node48*)*ref)->n.num_instructions);
			memcpy(new_node->keys, ((art_node48*)*ref)->keys, sizeof(unsigned char)*((art_node48*)*ref)->n.num_instructions);
			memcpy(new_node->offsets, ((art_node48*)*ref)->offsets, sizeof(int)*((art_node48*)*ref)->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
		}
	} else {
		if(n->n.num_remotions > 0){

			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child48((art_node48*)*ref, ref, c);
		}else{
			art_node256 *new_node = (art_node256*)alloc_node(NODE256);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child256(new_node, ref, c);
		}
	}
}

static void remove_child16(art_node16 *n, art_node **ref, unsigned char c) {

	if (n->n.num_instructions < 16) {

		art_node **child=find_child((art_node*)n, c);
		if(!child)
			return;
		int pos = child - n->children;

		tombstone *g=vmem_calloc(vmem, 1, sizeof(tombstone));
		g->dead=n->children[pos];
		operations[idx_op++]=(void*)g;
		operations[idx_op++]=(void*)g->dead;

		n->children[n->n.num_instructions] = (void*)g;
		n->keys[n->n.num_instructions] = c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		n->n.num_remotions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_remotions;
		operations[idx_op++]=(void*)&n->n.num_instructions;

		if(n->n.num_instructions - n->n.num_remotions*2 == 3){

			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);

			art_node4 *new_node = (art_node4*)alloc_node(NODE4);
			copy_header((art_node*)new_node, *ref);
			memcpy(new_node->children, ((art_node16*)*ref)->children, sizeof(void*)*((art_node16*)*ref)->n.num_instructions);
			memcpy(new_node->keys, ((art_node16*)*ref)->keys, sizeof(unsigned char)*((art_node16*)*ref)->n.num_instructions);
			memcpy(new_node->offsets, ((art_node16*)*ref)->offsets, sizeof(int)*((art_node16*)*ref)->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
		}
	} else {

		if(n->n.num_remotions > 0){
			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child16((art_node16*)*ref, ref, c);
		}else{
			art_node48 *new_node = (art_node48*)alloc_node(NODE48);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child48(new_node, ref, c);
		}
	}
}

static void remove_child4(art_node4 *n, art_node **ref, unsigned char c) {

	if (n->n.num_instructions < 4) {

		art_node **child=find_child((art_node*)n, c);
		if(!child)
			return;
		int pos = child - n->children;
		tombstone *g=vmem_calloc(vmem, 1, sizeof(tombstone));
		g->dead=n->children[pos];
		operations[idx_op++]=(void*)g;
		operations[idx_op++]=(void*)g->dead;

		n->children[n->n.num_instructions] = (void*)g;
		n->keys[n->n.num_instructions] = c;
		n->offsets[n->n.num_instructions]=n->n.num_instructions;
		for(int i=n->n.num_instructions; i >= 0; i--){
			for(int j=1; j <= i; j++){
				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
					int tmp=n->offsets[j-1];
					n->offsets[j-1]=n->offsets[j];
					n->offsets[j]=tmp;
				}
			}
		}
		n->n.num_instructions++;
		n->n.num_remotions++;
		operations[idx_op++]=(void*)&n->children[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->keys[n->n.num_instructions];
		operations[idx_op++]=(void*)&n->n.num_remotions;
		operations[idx_op++]=(void*)&n->n.num_instructions;


		if(n->n.num_instructions - n->n.num_remotions*2== 1){

			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			art_node *child = ((art_node4*)*ref)->children[0];

			if (!IS_LEAF(child)) {
				// Concatenate the prefixes
				int prefix = ((art_node4*)*ref)->n.partial_len;
				if (prefix < MAX_PREFIX_LEN) {
					((art_node4*)*ref)->n.partial[prefix] = ((art_node4*)*ref)->keys[0];
					prefix++;
				}
				if (prefix < MAX_PREFIX_LEN) {
					int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
					memcpy(((art_node4*)*ref)->n.partial+prefix, child->partial, sub_prefix);
					prefix += sub_prefix;
				}

				// Store the prefix in the child
				memcpy(child->partial, ((art_node4*)*ref)->n.partial, min(prefix, MAX_PREFIX_LEN));
				child->partial_len += ((art_node4*)*ref)->n.partial_len + 1;
			}
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			*ref = child;
			//free(n);
		}
	} else {

		if(n->n.num_remotions > 0){
			*ref=clean_node((art_node*)n);
			pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child4((art_node4*)*ref, ref, c);
		}else{
			art_node16 *new_node = (art_node16*)alloc_node(NODE16);
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->children, n->children, sizeof(void*)*n->n.num_instructions);
			memcpy(new_node->keys, n->keys, sizeof(unsigned char)*n->n.num_instructions);
			memcpy(new_node->offsets, n->offsets, sizeof(int)*n->n.num_instructions);
			*ref = (art_node*)new_node;
    		pmem_persist(*ref, sizeof(art_node*));
    		SPIN_PER_WRITE(1);
			//free(n);
			return remove_child16(new_node, ref, c);
		}
	}
}

static void remove_child(art_node *n, art_node **ref, unsigned char c, art_node **l) {
    switch (n->type) {
        case NODE4:
            return remove_child4((art_node4*)n, ref, c);
        case NODE16:
            return remove_child16((art_node16*)n, ref, c);
        case NODE48:
            return remove_child48((art_node48*)n, ref, c);
        case NODE256:
            return remove_child256((art_node256*)n, ref, c);
        default:
            abort();
    }
}

static art_leaf* recursive_delete(art_node *n, art_node **ref, const unsigned char *key, int key_len, int depth) {

	// Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        int prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    art_node **child = find_child(n, key[depth]);
    if (!child) return NULL;

    // If the child is leaf, delete from this node
    if (IS_LEAF(*child)) {
        art_leaf *l = LEAF_RAW(*child);
        if (!leaf_matches(l, key, key_len, depth)) {
            remove_child(n, ref, key[depth], child);
            return l;
        }
        return NULL;

    // Recurse
    } else {
        return recursive_delete(*child, child, key, key_len, depth+1);
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len) {
    art_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
    if (l) {
        t->size--;
        void *old = l->value;
        //free(l);
        return old;
    }
    return NULL;
}

// Recursively iterates over the tree
static int recursive_iter(art_node *n, art_callback cb, void *data) {


	// Handle base cases
    if (!n) return 0;
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
    }

    int res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_instructions; i++) {
                res = recursive_iter(((art_node4*)n)->children[((art_node4*)n)->offsets[i]], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_instructions; i++) {
                res = recursive_iter(((art_node16*)n)->children[((art_node16*)n)->offsets[i]], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < n->num_instructions; i++) {
                res = recursive_iter(((art_node48*)n)->children[((art_node48*)n)->offsets[i]], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < n->num_instructions; i++) {
                res = recursive_iter(((art_node256*)n)->children[((art_node256*)n)->offsets[i]], cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
    }
    return 0;
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree *t, art_callback cb, void *data) {
    return recursive_iter(t->root, cb, data);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const unsigned char *prefix, int prefix_len) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t)prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void *data) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            if (!leaf_prefix_matches(l, key, key_len))
               return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

            // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}
