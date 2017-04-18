#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include "art.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>


#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
    #include <emmintrin.h>
#endif
#endif

void *page_in_use;
void *pmemaddr;
VMEM* vmem;

int page;
affected_node *head, *tail;
cemetery *first, *last;


/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))



void system_fail(void *addr){
	mprotect(addr, sysconf(_SC_PAGESIZE),PROT_NONE);
}

int is_writeable(void *p)
{
    int fd = open("/dev/zero", O_RDONLY);
    int writeable;

    if (fd < 0)
        return -1; /* Should not happen */

    writeable = read(fd, p, 1) == 1;
    close(fd);

    return writeable;
}


int search_list(affected_node *head, art_node *elem){
	affected_node*n=head;

	while(n->next!=NULL){
		if(n->affected == elem)
			return 0;
		n=(affected_node*)n->next;
	}
	return -1;
}



void persist_tree(){
	art_node *aux, *delete;
	grave *g;
	cemetery *del;

	while(head->next!=NULL){
		aux=head->affected;
		delete=aux;

		while(aux->substitute!=NULL)aux=(art_node*)aux->substitute;

		switch(aux->type){
		case NODE4:
			for(int i=0; i < aux->num_children+aux->num_remotions; i++){
				pmem_flush(&((art_node4*)aux)->children[i], sizeof(art_node4));
				pmem_flush(&((art_node4*)aux)->keys[i], sizeof(unsigned char));
				pmem_flush(&((art_node4*)aux)->offsets[i], sizeof(unsigned char));
			}
			break;
		case NODE16:
			for(int i=0; i < aux->num_children+aux->num_remotions; i++){
				pmem_flush(&((art_node16*)aux)->children[i], sizeof(art_node16));
				pmem_flush(&((art_node16*)aux)->keys[i], sizeof(unsigned char));
				pmem_flush(&((art_node16*)aux)->offsets[i], sizeof(unsigned char));
			}
			break;
		case NODE48:
			for(int i=0; i < aux->num_children+aux->num_remotions; i++){
				pmem_flush(&((art_node48*)aux)->children[i], sizeof(art_node48));
				pmem_flush(&((art_node48*)aux)->keys[i], sizeof(unsigned char));
				pmem_flush(&((art_node48*)aux)->offsets[i], sizeof(unsigned char));
			}
			break;
		case NODE256:
			for(int i=0; i < aux->num_children+aux->num_remotions; i++){
				pmem_flush(&((art_node256*)aux)->children[i], sizeof(art_node256));
				pmem_flush(&((art_node256*)aux)->keys[i], sizeof(unsigned char));
				pmem_flush(&((art_node256*)aux)->offsets[i], sizeof(unsigned char));
			}
			break;
		default:break;
		}
		*(aux->ref)=(void *)aux;
		pmem_flush(aux->ref, sizeof(art_node **));
		head=(affected_node*)head->next;
		vmem_free(vmem, delete);
	}
	head=NULL;
	tail=NULL;

	while(first->next!=NULL){
		del=first;
		first=(cemetery*)first->next;
		g=del->grave;
		vmem_free(vmem, g->dead);
		vmem_free(vmem, del);
	}
	first=NULL;
	last=NULL;
}

long memory_page(void *addr){

	char *a=vmem_calloc(vmem, 15,sizeof(char));
	sprintf(a,"%p",addr);
	int cont = 0;
	long decimal=0;


	for(int i=4;i > 1; i--){

		switch(a[i]){
			case 'a':decimal+=10*((int)pow(16,cont));break;
			case 'b':decimal+=11*((int)pow(16,cont));break;
			case 'c':decimal+=12*((int)pow(16,cont));break;
			case 'd':decimal+=13*((int)pow(16,cont));break;
			case 'e':decimal+=14*((int)pow(16,cont));break;
			case 'f':decimal+=15*((int)pow(16,cont));break;
			case '1':decimal+=1*((int)pow(16,cont));break;
			case '2':decimal+=2*((int)pow(16,cont));break;
			case '3':decimal+=3*((int)pow(16,cont));break;
			case '4':decimal+=4*((int)pow(16,cont));break;
			case '5':decimal+=5*((int)pow(16,cont));break;
			case '6':decimal+=6*((int)pow(16,cont));break;
			case '7':decimal+=7*((int)pow(16,cont));break;
			case '8':decimal+=8*((int)pow(16,cont));break;
			case '9':decimal+=9*((int)pow(16,cont));break;
			default:break;

		}
		cont++;
    }
	vmem_free(vmem, a);
	return decimal;
}

void insert_grave(grave*g){
	if(first == NULL){
		first->grave=g;
		last=first;
		last->next=NULL;
	}else{
		last->next=(cemetery *)vmem_malloc(vmem, sizeof(cemetery));
		((cemetery *)last->next)->grave=g;
		last=last->next;
	}
}

void insert_list(art_node *n){
	if(n->ref!=NULL)
		return;
	if(head == NULL){
		if(n->ref == NULL){
			head->affected=n;
			tail=head;
			tail->next=NULL;

		}
	}else{
		if(search_list(head, n) != 0){
			tail->next=(affected_node*)vmem_malloc(vmem, sizeof(affected_node));
			((affected_node*)tail->next)->affected=n;
			tail=tail->next;

			if(memory_page(tail->next)!=page){
				persist_tree();
				page=memory_page(tail->next);
				page_in_use=tail->next;
				pmem_flush(page_in_use, sizeof(void*));
				pmem_flush(&page, sizeof(uint8_t));
			}
		}
	}
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
    n->substitute=NULL;
    n->ref=NULL;
    n->num_remotions=0;
    n->type = type;

    if(memory_page(n)!=page){
    	persist_tree();
    	page=memory_page(n);
    	page_in_use=n;
    	pmem_flush(page_in_use, sizeof(void*));
    	pmem_flush(&page, sizeof(uint8_t));
    }

    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t, void *addr, VMEM*v) {
	pmemaddr=addr;
	vmem=v;
    t->root = NULL;
    t->size = 0;
    page=memory_page(t->root);
    page_in_use=t->root;
    pmem_flush(&page, sizeof(uint8_t));
    pmem_flush(page_in_use, sizeof(void*));
    head=NULL;
    tail=NULL;
    first=NULL;
    last=NULL;
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        free(LEAF_RAW(n));
        return;
    }
    while(n->substitute!=NULL)n=(art_node*)n->substitute;
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
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p1->children[i]);
            }
            break;

        case NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p2->children[i]);
            }
            break;

        case NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<n->num_children;i++) {
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
    free(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}


art_node* clean_node(art_node* n){
	art_node *aux;
	int idx=0;
	switch(n->type){
	case NODE4:

		aux=alloc_node(NODE4);
		for(int i=0; i < n->num_children+n->num_remotions-1; i++){
			if(((art_node4*)n)->keys[((art_node4*)n)->offsets[i]]==((art_node4*)n)->keys[((art_node4*)n)->offsets[i+1]]){
				i++;
			}else{
				((art_node4*)aux)->keys[idx]=((art_node4*)n)->keys[((art_node4*)n)->offsets[i]];
				((art_node4*)aux)->children[idx]=((art_node4*)n)->children[((art_node4*)n)->offsets[i]];
				((art_node4*)aux)->offsets[idx]=idx;
				((art_node4*)aux)->n.num_children++;
				idx++;
			}
		}
		if(((art_node4*)aux)->n.num_children > 1){
			for(int i=((art_node4*)aux)->n.num_children-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(((art_node4*)aux)->keys[((art_node4*)aux)->offsets[j-1]] > ((art_node4*)aux)->keys[((art_node4*)aux)->offsets[j]]){
						int tmp=((art_node4*)aux)->offsets[j-1];
						((art_node4*)aux)->offsets[j-1]=((art_node4*)aux)->offsets[j];
						((art_node4*)aux)->offsets[j]=tmp;
					}
				}
			}
		}

		return aux;
	case NODE16:

		aux=alloc_node(NODE16);
		for(int i=0; i < n->num_children+n->num_remotions-1; i++){
			if(((art_node16*)n)->keys[((art_node16*)n)->offsets[i]]==((art_node16*)n)->keys[((art_node16*)n)->offsets[i+1]]){
				i++;
			}else{
				((art_node16*)aux)->keys[idx]=((art_node16*)n)->keys[((art_node16*)n)->offsets[i]];
				((art_node16*)aux)->children[idx]=((art_node16*)n)->children[((art_node16*)n)->offsets[i]];
				((art_node16*)aux)->offsets[idx]=idx;
				((art_node16*)aux)->n.num_children++;
				idx++;
			}
		}
		if(((art_node16*)aux)->n.num_children > 1){
			for(int i=((art_node16*)aux)->n.num_children-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(((art_node16*)aux)->keys[((art_node16*)aux)->offsets[j-1]] > ((art_node16*)aux)->keys[((art_node16*)aux)->offsets[j]]){
						int tmp=((art_node16*)aux)->offsets[j-1];
						((art_node16*)aux)->offsets[j-1]=((art_node16*)aux)->offsets[j];
						((art_node16*)aux)->offsets[j]=tmp;
					}
				}
			}
		}

		return aux;
	case NODE48:

		aux=alloc_node(NODE48);
		for(int i=0; i < n->num_children+n->num_remotions-1; i++){
			if(((art_node48*)n)->keys[((art_node48*)n)->offsets[i]]==((art_node48*)n)->keys[((art_node48*)n)->offsets[i+1]]){
				i++;
			}else{
				((art_node48*)aux)->keys[idx]=((art_node48*)n)->keys[((art_node48*)n)->offsets[i]];
				((art_node48*)aux)->children[idx]=((art_node48*)n)->children[((art_node48*)n)->offsets[i]];
				((art_node48*)aux)->offsets[idx]=idx;
				((art_node48*)aux)->n.num_children++;
				idx++;
			}
		}
		if(((art_node48*)aux)->n.num_children > 1){
			for(int i=((art_node48*)aux)->n.num_children-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(((art_node48*)aux)->keys[((art_node48*)aux)->offsets[j-1]] > ((art_node48*)aux)->keys[((art_node48*)aux)->offsets[j]]){
						int tmp=((art_node48*)aux)->offsets[j-1];
						((art_node48*)aux)->offsets[j-1]=((art_node48*)aux)->offsets[j];
						((art_node48*)aux)->offsets[j]=tmp;
					}
				}
			}
		}

		return aux;
	case NODE256:

		aux=alloc_node(NODE256);
		for(int i=0; i < n->num_children+n->num_remotions-1; i++){
			if(((art_node256*)n)->keys[((art_node256*)n)->offsets[i]]==((art_node256*)n)->keys[((art_node256*)n)->offsets[i+1]]){
				i++;
			}else{
				((art_node256*)aux)->keys[idx]=((art_node256*)n)->keys[((art_node256*)n)->offsets[i]];
				((art_node256*)aux)->children[idx]=((art_node256*)n)->children[((art_node256*)n)->offsets[i]];
				((art_node256*)aux)->offsets[idx]=idx;
				((art_node256*)aux)->n.num_children++;
				idx++;
			}
		}
		if(((art_node256*)aux)->n.num_children > 1){
			for(int i=((art_node256*)aux)->n.num_children-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(((art_node256*)aux)->keys[((art_node256*)aux)->offsets[j-1]] > ((art_node256*)aux)->keys[((art_node256*)aux)->offsets[j]]){
						int tmp=((art_node256*)aux)->offsets[j-1];
						((art_node256*)aux)->offsets[j-1]=((art_node256*)aux)->offsets[j];
						((art_node256*)aux)->offsets[j]=tmp;
					}
				}
			}
		}

		return aux;
	default:return NULL;
	}
}
/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static art_node** find_child(art_node *n, unsigned char c) {
    int i,low,high,middle;

    if(n->num_remotions > 0)
    	n=clean_node((art_node*)n);
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_children; i++) {
		/* this cast works around a bug in gcc 5.1 when unrolling loops
		 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
		 */
                if (((unsigned char*)p.p1->keys)[i] == c)
                    return &p.p1->children[i];
            }
            break;
        case NODE16:
        	 p.p2 = (art_node16*)n;

			low=0;
			high=p.p2->n.num_children-1;
			while(low <= high){
				middle=(low+high)/2;
				if(p.p2->keys[p.p2->offsets[middle]] == c)
					return &p.p2->children[p.p2->offsets[middle]];

				if(p.p2->keys[p.p2->offsets[middle]] < c)
					low=++middle;
				else
					high=--middle;
			}
			break;
        case NODE48:
        	p.p3 = (art_node48*)n;

			low=0;
			high=p.p3->n.num_children-1;
			while(low <= high){
				middle=(low+high)/2;
				if(p.p3->keys[p.p3->offsets[middle]] == c)
					return &p.p3->children[p.p3->offsets[middle]];

				if(p.p3->keys[p.p3->offsets[middle]] < c)
					low=++middle;
				else
					high=--middle;
			}
			break;

        case NODE256:
        	p.p4 = (art_node256*)n;

			low=0;
			high=p.p4->n.num_children-1;
			while(low <= high){
				middle=(low+high)/2;
				if(p.p4->keys[p.p4->offsets[middle]] == c)
					return &p.p4->children[p.p4->offsets[middle]];

				if(p.p4->keys[p.p4->offsets[middle]] < c)
					low=++middle;
				else
					high=--middle;
			}
			break;

        default: abort();
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

    	while(n->substitute!=NULL)n=(art_node*)n->substitute;
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


    while(n->substitute!=NULL)n=(art_node*)n->substitute;
    switch (n->type) {
        case NODE4:
            return minimum(((const art_node4*)n)->children[((art_node4 *)n)->offsets[0]]);
        case NODE16:
            return minimum(((const art_node16*)n)->children[((art_node16 *)n)->offsets[0]]);
        case NODE48:
            return minimum(((const art_node48*)n)->children[((art_node48 *)n)->offsets[0]]);
        case NODE256:
            return minimum(((const art_node256*)n)->children[((art_node256 *)n)->offsets[0]]);
        default:
            abort();
    }
}

// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    while(n->substitute!=NULL)n=(art_node*)n->substitute;
    if(n->num_remotions >0)
        	n=clean_node((art_node*)n);

    switch (n->type) {
        case NODE4:
            return maximum(((const art_node4*)n)->children[((art_node4 *)n)->offsets[n->num_children-1]]);
        case NODE16:
            return maximum(((const art_node16*)n)->children[((art_node16 *)n)->offsets[n->num_children-1]]);
        case NODE48:
            return maximum(((const art_node48*)n)->children[((art_node48 *)n)->offsets[n->num_children-1]]);
        case NODE256:
            return maximum(((const art_node256*)n)->children[((art_node256 *)n)->offsets[n->num_children-1]]);
        default:
            abort();
    }
}

/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
	art_node *root=t->root;
	while(root->substitute!=NULL)root=(art_node*)root->substitute;
    return minimum(root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
	art_node *root=t->root;
	while(root->substitute!=NULL)root=(art_node*)root->substitute;
    return maximum(root);
}

static art_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    art_leaf *l = (art_leaf*)vmem_malloc(vmem, sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    if(memory_page(l)!=page){
    	persist_tree();
    	page=memory_page(l);
    	page_in_use=l;
    	pmem_flush(page_in_use, sizeof(void*));
    	pmem_flush(&page, sizeof(uint8_t));
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
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {

	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
    if(n->n.num_children+n->n.num_remotions < 256){
    	n->children[n->n.num_children+n->n.num_remotions]=child;
    	n->keys[n->n.num_children+n->n.num_remotions]=c;
    	n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;
    	n->n.num_children++;

    	if(n->n.num_children > 1){
    		for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
    			for(int j=1; j <= i; j++){
    				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
    					int tmp=n->offsets[j-1];
    					n->offsets[j-1]=n->offsets[j];
    					n->offsets[j]=tmp;
    				}
    			}
    		}
    	}

    }else{
    	n->n.substitute=(void*)clean_node((art_node*)n);
    	((art_node*)n->n.substitute)->ref=(void**)ref;
    	add_child256((art_node256*)n->n.substitute, ref, c, child);
    }

}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 48){
	    	n->children[n->n.num_children+n->n.num_remotions]=child;
	    	n->keys[n->n.num_children+n->n.num_remotions]=c;
	    	n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;
	    	n->n.num_children++;

	    	if(n->n.num_children > 1){
	    		for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
	    			for(int j=1; j <= i; j++){
	    				if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
	    					int tmp=n->offsets[j-1];
	    					n->offsets[j-1]=n->offsets[j];
	    					n->offsets[j]=tmp;
	    				}
	    			}
	    		}
	    	}

	    } else {

	    	if(n->n.num_remotions > 0){
	    		n->n.substitute=(void*)clean_node((art_node*)n);
	    		((art_node*)n->n.substitute)->ref=(void**)ref;
	    		add_child48((art_node48*)n->n.substitute, ref, c, child);
	    	}else{
	    		art_node256 *new_node = (art_node256*)alloc_node(NODE256);
	    		memcpy(new_node->children, n->children,sizeof(void*)*n->n.num_children);
				memcpy(new_node->keys, n->keys,sizeof(unsigned char)*n->n.num_children);
				memcpy(new_node->offsets, n->offsets,sizeof(unsigned char)*n->n.num_children);
				copy_header((art_node*)new_node, (art_node*)n);
				//*ref = (art_node*)new_node;
				//free(n);
				n->n.substitute=(art_node*)new_node;
				new_node->n.ref=(void**)ref;
				add_child256(new_node, ref, c, child);
	    	}
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {

	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 16){
		n->children[n->n.num_children+n->n.num_remotions]=child;
		n->keys[n->n.num_children+n->n.num_remotions]=c;
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;
		n->n.num_children++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}

	} else {

		if(n->n.num_remotions > 0){
			n->n.substitute=(void*)clean_node((art_node*)n);
			((art_node*)n->n.substitute)->ref=(void**)ref;
			add_child16((art_node16*)n->n.substitute, ref, c, child);
		}else{
			art_node48 *new_node = (art_node48*)alloc_node(NODE48);
			memcpy(new_node->children, n->children,sizeof(void*)*n->n.num_children);
			memcpy(new_node->keys, n->keys,sizeof(unsigned char)*n->n.num_children);
			memcpy(new_node->offsets, n->offsets,sizeof(unsigned char)*n->n.num_children);
			copy_header((art_node*)new_node, (art_node*)n);
			//*ref = (art_node*)new_node;
			//free(n);
			n->n.substitute=(art_node*)new_node;
			new_node->n.ref=(void**)ref;
			add_child48(new_node, ref, c, child);
		}
	}
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 4){
		n->children[n->n.num_children+n->n.num_remotions]=child;
		n->keys[n->n.num_children+n->n.num_remotions]=c;
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;
		n->n.num_children++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}

	} else {

		if(n->n.num_remotions > 0){
			n->n.substitute=(void*)clean_node((art_node*)n);
			((art_node*)n->n.substitute)->ref=(void**)ref;
			add_child4((art_node4*)n->n.substitute, ref, c, child);
		}else{
			art_node16 *new_node = (art_node16*)alloc_node(NODE16);
			memcpy(new_node->children, n->children,sizeof(void*)*n->n.num_children);
			memcpy(new_node->keys, n->keys,sizeof(unsigned char)*n->n.num_children);
			memcpy(new_node->offsets, n->offsets,sizeof(unsigned char)*n->n.num_children);
			copy_header((art_node*)new_node, (art_node*)n);
			//*ref = (art_node*)new_node;
			//free(n);
			n->n.substitute=(art_node*)new_node;
			new_node->n.ref=(void**)ref;
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
        // Add the leafs to the new node4
        //*ref = (art_node*)new_node;
        n->substitute=(void*)new_node;
        insert_list((art_node*)n);
        add_child4(new_node, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        add_child4(new_node, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
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
        //*ref = (art_node*)new_node;
        n->substitute=(art_node*)new_node;
        insert_list((art_node*)n);
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
    art_node*root=t->root;
    while(root->substitute!=NULL)root=(art_node*)root->substitute;
    void *old = recursive_insert(root, &root, key, key_len, value, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, art_node **l) {

	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 256){

		int idx=l - n->children;
		grave *g=vmem_malloc(vmem, sizeof(grave));
		g->dead=(void*)n->children[idx];
		insert_grave(g);
		n->children[n->n.num_children+n->n.num_remotions]=(void*)g;
		n->keys[n->n.num_children+n->n.num_remotions]=n->keys[idx];
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;

		n->n.num_children--;
		n->n.num_remotions++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}

		if(memory_page(g)!=page){
			persist_tree();
			page=memory_page(g);
			page_in_use=g;
			pmem_flush(page_in_use, sizeof(void*));
			pmem_flush(&page, sizeof(uint8_t));
		}

		if(n->n.num_children == 37){
			n->n.substitute=(void*)clean_node((art_node*)n);
			n=(art_node256*)n->n.substitute;
			art_node48 *new_node = (art_node48*)alloc_node(NODE48);
			//*ref = (art_node*)new_node;
			n->n.substitute=new_node;
			new_node->n.ref=(void**)ref;
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->keys, n->keys, 48);
			memcpy(new_node->children, n->children, 48*sizeof(void*));
			memcpy(new_node->offsets, n->offsets, 48);
		}

	} else {


		n->n.substitute=(void*)clean_node((art_node*)n);
		((art_node*)n->n.substitute)->ref=(void**)ref;
		remove_child256((art_node256*)n->n.substitute, ref, l);

	}
}

static void remove_child48(art_node48 *n, art_node **ref, art_node **l) {
	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 48){
		int idx=l - n->children;
		grave *g=vmem_malloc(vmem, sizeof(grave));
		g->dead=(void*)n->children[idx];
		insert_grave(g);
		n->children[n->n.num_children+n->n.num_remotions]=(void*)g;
		n->keys[n->n.num_children+n->n.num_remotions]=n->keys[idx];
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;

		n->n.num_children--;
		n->n.num_remotions++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}

		if(memory_page(g)!=page){
			persist_tree();
			page=memory_page(g);
			page_in_use=g;
			pmem_flush(page_in_use, sizeof(void*));
			pmem_flush(&page, sizeof(uint8_t));
		}

		if(n->n.num_children == 12){
			n->n.substitute=(void*)clean_node((art_node*)n);
			n=(art_node48*)n->n.substitute;
			art_node16 *new_node = (art_node16*)alloc_node(NODE16);
			//*ref = (art_node*)new_node;
			n->n.substitute=new_node;
			new_node->n.ref=(void**)ref;
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->keys, n->keys, 16);
			memcpy(new_node->children, n->children, 16*sizeof(void*));
			memcpy(new_node->offsets, n->offsets, 16);
		}

	} else {


		n->n.substitute=(void*)clean_node((art_node*)n);
		((art_node*)n->n.substitute)->ref=(void**)ref;
		remove_child48((art_node48*)n->n.substitute, ref, l);

	}
}

static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 16){
		int idx=l - n->children;
		grave *g=vmem_malloc(vmem, sizeof(grave));
		g->dead=(void*)n->children[idx];
		insert_grave(g);
		n->children[n->n.num_children+n->n.num_remotions]=(void*)g;
		n->keys[n->n.num_children+n->n.num_remotions]=n->keys[idx];
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;

		n->n.num_children--;
		n->n.num_remotions++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}
		if(memory_page(g)!=page){
			persist_tree();
			page=memory_page(g);
			page_in_use=g;
			pmem_flush(page_in_use, sizeof(void*));
			pmem_flush(&page, sizeof(uint8_t));
		}
		if(n->n.num_children == 3){
			n->n.substitute=(void*)clean_node((art_node*)n);
			n=(art_node16*)n->n.substitute;
			art_node4 *new_node = (art_node4*)alloc_node(NODE4);
			//*ref = (art_node*)new_node;
			n->n.substitute=new_node;
			new_node->n.ref=(void**)ref;
			copy_header((art_node*)new_node, (art_node*)n);
			memcpy(new_node->keys, n->keys, 4);
			memcpy(new_node->children, n->children, 4*sizeof(void*));
			memcpy(new_node->offsets, n->offsets, 4);
		}

	} else {


		n->n.substitute=(void*)clean_node((art_node*)n);
		((art_node*)n->n.substitute)->ref=(void**)ref;
		remove_child16((art_node16*)n->n.substitute, ref, l);

	}
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
	if(is_writeable(&n->children[n->n.num_children+n->n.num_remotions])){
		printf("unstable node!!!!");
		return;
	}
	insert_list((art_node*)n);
	if(n->n.num_children+n->n.num_remotions < 16){
		int idx=l - n->children;
		grave *g=vmem_malloc(vmem, sizeof(grave));
		g->dead=(void*)n->children[idx];
		insert_grave(g);
		n->children[n->n.num_children+n->n.num_remotions]=(void*)g;
		n->keys[n->n.num_children+n->n.num_remotions]=n->keys[idx];
		n->offsets[n->n.num_children+n->n.num_remotions]=n->n.num_children+n->n.num_remotions;

		n->n.num_children--;
		n->n.num_remotions++;

		if(n->n.num_children > 1){
			for(int i=n->n.num_children+n->n.num_remotions-1; i >= 0; i--){
				for(int j=1; j <= i; j++){
					if(n->keys[n->offsets[j-1]] > n->keys[n->offsets[j]]){
						int tmp=n->offsets[j-1];
						n->offsets[j-1]=n->offsets[j];
						n->offsets[j]=tmp;
					}
				}
			}
		}
		if(memory_page(g)!=page){
			persist_tree();
			page=memory_page(g);
			page_in_use=g;
			pmem_flush(page_in_use, sizeof(void*));
			pmem_flush(&page, sizeof(uint8_t));
		}
		 // Remove nodes with only a single child
		 if (n->n.num_children == 1) {
			art_node *child = n->children[0];
			if (!IS_LEAF(child)) {
				// Concatenate the prefixes
				int prefix = n->n.partial_len;
				if (prefix < MAX_PREFIX_LEN) {
					n->n.partial[prefix] = n->keys[0];
					prefix++;
				}
				if (prefix < MAX_PREFIX_LEN) {
					int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
					memcpy(n->n.partial+prefix, child->partial, sub_prefix);
					prefix += sub_prefix;
				}

				// Store the prefix in the child
				memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
				child->partial_len += n->n.partial_len + 1;
			}
			//*ref = child;
			//free(n);
			n->n.substitute=(void*)child;
			//new_node->n.ref=(void**)ref;
		}
	} else {


		n->n.substitute=(void*)clean_node((art_node*)n);
		((art_node*)n->n.substitute)->ref=(void**)ref;
		remove_child4((art_node4*)n->n.substitute, ref, l);

	}

}

static void remove_child(art_node *n, art_node **ref, unsigned char c, art_node **l) {
    switch (n->type) {
        case NODE4:
            return remove_child4((art_node4*)n, ref, l);
        case NODE16:
            return remove_child16((art_node16*)n, ref, l);
        case NODE48:
            return remove_child48((art_node48*)n, ref, l);
        case NODE256:
            return remove_child256((art_node256*)n, ref, l);
        default:
            abort();
    }
}

static art_leaf* recursive_delete(art_node *n, art_node **ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (!n) return NULL;


    while(n->substitute!=NULL)n=(art_node*)n->substitute;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            //*ref = NULL;
        	n->substitute=NULL;
        	insert_list((art_node*)n);
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

	art_node*root=t->root;
	while(root->substitute!=NULL)root=(art_node*)root->substitute;

    art_leaf *l = recursive_delete(root, &root, key, key_len, 0);
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

    int idx, res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node4*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node16*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                res = recursive_iter(((art_node48*)n)->children[idx-1], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                res = recursive_iter(((art_node256*)n)->children[i], cb, data);
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
	art_node *n=t->root;
	while(n->substitute!=NULL)n=(art_node*)n->substitute;
    return recursive_iter(n, cb, data);
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
    	while(n->substitute!=NULL)n=(art_node*)n->substitute;
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
