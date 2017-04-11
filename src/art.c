#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include "art.h"

#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
    #include <emmintrin.h>
#endif
#endif

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

affected_node *head, *tail;
long page;
void* pmemaddr;
VMEM* vmem;

long memory_page(void *addr){

	char *a=calloc(15,sizeof(char));
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
	return decimal;
}

void init_list(){
	head=(affected_node*)vmem_calloc(vmem, 1, sizeof(affected_node));
	tail=head;
}

void empty_list(){
	if(head == NULL){}
	else{
		affected_node*aux;
		while(head){
			aux=head;
			head=head->next;
			vmem_free(vmem, aux);
		}
		tail=head;
	}
}

int search_list(art_node *node){
	if(head == NULL)
		return -1;
	affected_node *aux=head;
	while(aux){
		if((art_node*)aux->node == node)
			return 0;
		aux=aux->next;
	}
	return -1;
}

void * remove_head(){
	if(head != NULL){
		void *aux=head;
		head=head->next;
		return aux;
	}
	return NULL;
}

void insert_list(art_node *node){

	if(search_list(node) == -1)
	{
		if(head == NULL){
			head->node=(void*)node;
			tail=head;
		}else{
			tail->next=(affected_node*)vmem_calloc(vmem, 1,sizeof(affected_node));
			((affected_node*)tail->next)->node=(art_node*)node;
			tail=tail->next;
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
            n = (art_node*)vmem_calloc(vmem,1, sizeof(art_node4));
            break;
        case NODE16:
            n = (art_node*)vmem_calloc(vmem,1, sizeof(art_node16));
            break;
        case NODE48:
            n = (art_node*)vmem_calloc(vmem,1, sizeof(art_node48));
            break;
        case NODE256:
            n = (art_node*)vmem_calloc(vmem,1, sizeof(art_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t, void* addr, VMEM* v) {
	pmemaddr=addr;
	vmem=v;
	init_list();
    t->root = NULL;
    t->size = 0;
    page=*((long *)vmem_calloc(vmem,1, sizeof(long)));
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;
    while(n->substitute!=NULL)n=n->substitute;
    // Special case leafs
    if (IS_LEAF(n)) {
        vmem_free(vmem, LEAF_RAW(n));
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
    //free(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
	empty_list();
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
    int i,low=0,high=n->num_children-1,middle;

    while(n->substitute!=NULL)n=n->substitute;

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
                	return &p.p1->children[1];

            }
            break;
        case NODE16:
            p.p2 = (art_node16*)n;

            while(low <= high){
            	middle=(low+high)/2;

            	if(p.p2->keys[p.p2->offsets[middle]] == c)
            		return &p.p2->children[p.p2->offsets[middle]];

            	if(p.p2->keys[p.p2->offsets[middle]] > c)
            		low=++middle;
            	else
            		high=--middle;
            }
            break;
        case NODE48:
            p.p3 = (art_node48*)n;

            while(low <= high){
            	middle=(low+high)/2;

            	if(p.p2->keys[p.p2->offsets[middle]] == c)
            		return &p.p2->children[p.p2->offsets[middle]];

            	if(p.p2->keys[p.p2->offsets[middle]] > c)
            		low=++middle;
            	else
            		high=--middle;
            }
            break;
        case NODE256:
            p.p4 = (art_node256*)n;

            while(low <= high){
            	middle=(low+high)/2;

            	if(p.p2->keys[p.p2->offsets[middle]] == c)
            		return &p.p2->children[p.p2->offsets[middle]];

            	if(p.p2->keys[p.p2->offsets[middle]] > c)
            		low=++middle;
            	else
            		high=--middle;
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
    		while(n->substitute!=NULL)n=n->substitute;
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

    while(n->substitute != NULL)
    	n=(art_node*)n->substitute;
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
    while(n->substitute!=NULL)n=n->substitute;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    switch (n->type) {
        case NODE4:
            return maximum(((const art_node4*)n)->children[((const art_node4*)n)->offsets[n->num_children-1]]);
        case NODE16:
            return maximum(((const art_node16*)n)->children[((const art_node16*)n)->offsets[n->num_children-1]]);
        case NODE48:
            return maximum(((const art_node48*)n)->children[((const art_node48*)n)->offsets[n->num_children-1]]);
        case NODE256:
            return maximum(((const art_node256*)n)->children[((const art_node256*)n)->offsets[n->num_children-1]]);
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
    if(page == 0){
    	page=memory_page((void*)l);
    	pmem_flush(&page,sizeof(long));
    }

    insert_list((void*)l);
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

void persist(){

	void* aux=remove_head();
	while(head!=NULL){
		if(IS_LEAF(aux)){
			pmem_flush(aux, sizeof(art_leaf));
		}else{
			while(((art_node*)aux)->substitute != NULL)
				aux=((art_node*)aux)->substitute;

			switch(((art_node*)aux)->type){
			case NODE4:
				if(((art_node*)aux)->num_children == 0){
					((art_node*)aux)->parent=NULL;
					pmem_flush(((art_node*)aux)->parent, sizeof(art_node**));
					vmem_free(vmem, aux);
				}else{
					if(((art_node*)aux)->num_remotion == 0){
						for(int i=((art_node*)aux)->stable; i < ((art_node*)aux)->num_children;i++){
							pmem_flush(&((art_node4*)aux)->keys[i], sizeof(char));
							pmem_flush(&((art_node4*)aux)->children[i], sizeof(art_node));
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						pmem_flush(&((art_node*)aux)->stable, sizeof(uint8_t));
						((art_node*)aux)->parent=aux;
						pmem_flush(((art_node*)aux)->parent,sizeof(art_node**));
					}else{
						for(int i=0; i < ((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1; i++){
							if(((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i]] == ((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i+1]]){
								memmove(&((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i]], &((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i]], &((art_node4*)aux)->keys[((art_node4*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node4*)aux)->children[((art_node4*)aux)->offsets[i]], &((art_node4*)aux)->children[((art_node4*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node4*)aux)->children[((art_node4*)aux)->offsets[i]], &((art_node4*)aux)->children[((art_node4*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node4*)aux)->offsets[i], &((art_node4*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
								memmove(&((art_node4*)aux)->offsets[i], &((art_node4*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
							}
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						((art_node*)aux)->num_remotion=0;
						((art_node*)aux)->parent=aux;

						pmem_flush(aux, sizeof(art_node4));
					}
				}
				break;
			case NODE16:
				if(((art_node*)aux)->num_children == 0){
					((art_node*)aux)->parent=NULL;
					pmem_flush(((art_node*)aux)->parent, sizeof(art_node**));
					vmem_free(vmem, aux);
				}else{
					if(((art_node*)aux)->num_remotion == 0){
						for(int i=((art_node*)aux)->stable; i < ((art_node*)aux)->num_children;i++){
							pmem_flush(&((art_node16*)aux)->keys[i], sizeof(char));
							pmem_flush(&((art_node16*)aux)->children[i], sizeof(art_node));
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						pmem_flush(&((art_node*)aux)->stable, sizeof(uint8_t));
						((art_node*)aux)->parent=aux;
						pmem_flush(((art_node*)aux)->parent,sizeof(art_node**));
					}else{
						for(int i=0; i < ((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1; i++){
							if(((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i]] == ((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i+1]]){
								memmove(&((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i]], &((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i]], &((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node16*)aux)->children[((art_node16*)aux)->offsets[i]], &((art_node16*)aux)->children[((art_node16*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node16*)aux)->children[((art_node16*)aux)->offsets[i]], &((art_node16*)aux)->children[((art_node16*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node16*)aux)->offsets[i], &((art_node16*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
								memmove(&((art_node16*)aux)->offsets[i], &((art_node16*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
							}
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						((art_node*)aux)->num_remotion=0;
						((art_node*)aux)->parent=aux;

						pmem_flush(aux, sizeof(art_node16));
					}
				}
				break;
			case NODE48:
				if(((art_node*)aux)->num_children == 0){
					((art_node*)aux)->parent=NULL;
					pmem_flush(((art_node*)aux)->parent, sizeof(art_node**));
					vmem_free(vmem, aux);
				}else{
					if(((art_node*)aux)->num_remotion == 0){
						for(int i=((art_node*)aux)->stable; i < ((art_node*)aux)->num_children;i++){
							pmem_flush(&((art_node16*)aux)->keys[i], sizeof(char));
							pmem_flush(&((art_node16*)aux)->children[i], sizeof(art_node));
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						pmem_flush(&((art_node*)aux)->stable, sizeof(uint8_t));
						((art_node*)aux)->parent=aux;
						pmem_flush(((art_node*)aux)->parent,sizeof(art_node**));
					}else{
						for(int i=0; i < ((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1; i++){
							if(((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i]] == ((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i+1]]){
								memmove(&((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i]], &((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i]], &((art_node48*)aux)->keys[((art_node48*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node48*)aux)->children[((art_node48*)aux)->offsets[i]], &((art_node48*)aux)->children[((art_node48*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node48*)aux)->children[((art_node48*)aux)->offsets[i]], &((art_node48*)aux)->children[((art_node48*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node48*)aux)->offsets[i], &((art_node48*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
								memmove(&((art_node48*)aux)->offsets[i], &((art_node48*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
							}
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						((art_node*)aux)->num_remotion=0;
						((art_node*)aux)->parent=aux;

						pmem_flush(aux, sizeof(art_node48));
					}
				}
				break;
			case NODE256:
				if(((art_node*)aux)->num_children == 0){
					((art_node*)aux)->parent=NULL;
					pmem_flush(((art_node*)aux)->parent, sizeof(art_node**));
					vmem_free(vmem, aux);
				}else{
					if(((art_node*)aux)->num_remotion == 0){
						for(int i=((art_node*)aux)->stable; i < ((art_node*)aux)->num_children;i++){
							pmem_flush(&((art_node256*)aux)->keys[i], sizeof(char));
							pmem_flush(&((art_node256*)aux)->children[i], sizeof(art_node));
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						pmem_flush(&((art_node*)aux)->stable, sizeof(uint8_t));
						((art_node*)aux)->parent=aux;
						pmem_flush(((art_node*)aux)->parent,sizeof(art_node**));
					}else{
						for(int i=0; i < ((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1; i++){
							if(((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i]] == ((art_node16*)aux)->keys[((art_node16*)aux)->offsets[i+1]]){
								memmove(&((art_node256*)aux)->keys[((art_node256*)aux)->offsets[i]], &((art_node256*)aux)->keys[((art_node256*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node256*)aux)->keys[((art_node256*)aux)->offsets[i]], &((art_node256*)aux)->keys[((art_node256*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node256*)aux)->children[((art_node256*)aux)->offsets[i]], &((art_node256*)aux)->children[((art_node256*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);
								memmove(&((art_node256*)aux)->children[((art_node256*)aux)->offsets[i]], &((art_node256*)aux)->children[((art_node256*)aux)->offsets[i+1]],((art_node*)aux)->num_children+((art_node*)aux)->num_remotion-1);

								memmove(&((art_node256*)aux)->offsets[i], &((art_node256*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
								memmove(&((art_node256*)aux)->offsets[i], &((art_node256*)aux)->offsets[i+1],((art_node*)aux)->num_children-1);
							}
						}
						((art_node*)aux)->stable=((art_node*)aux)->num_children;
						((art_node*)aux)->num_remotion=0;
						((art_node*)aux)->parent=aux;

						pmem_flush(aux, sizeof(art_node256));
					}
				}
				break;
			}
		}
	}

	page=0;
	pmem_flush(&page, sizeof(page));
	empty_list();
}


art_node* clean_node(art_node* node, char instruction){
	art_node* new_node;

	switch(node->type){
	case NODE4:
		if(instruction == 1){
			if(node->num_children < 4){
				new_node = (art_node*) alloc_node(NODE4);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node4*)node)->keys[((art_node4*)node)->offsets[i]] == ((art_node4*)node)->keys[((art_node4*)node)->offsets[i+1]])
						i++;
					else{
						((art_node4*)new_node)->keys[new_node->num_children]=((art_node4*)node)->keys[((art_node4*)node)->offsets[i]];
						((art_node4*)new_node)->children[new_node->num_children]=((art_node4*)node)->children[((art_node4*)node)->offsets[i]];
						((art_node4*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE16);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node4*)node)->keys[((art_node4*)node)->offsets[i]] == ((art_node4*)node)->keys[((art_node4*)node)->offsets[i+1]])
						i++;
					else{
						((art_node16*)new_node)->keys[new_node->num_children]=((art_node4*)node)->keys[((art_node4*)node)->offsets[i]];
						((art_node16*)new_node)->children[new_node->num_children]=((art_node4*)node)->children[((art_node4*)node)->offsets[i]];
						((art_node16*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}else{
			new_node = (art_node*) alloc_node(NODE4);
			for(int i=0; i < node->num_children+node->num_remotion-1; i++){
				if(((art_node4*)node)->keys[((art_node4*)node)->offsets[i]] == ((art_node4*)node)->keys[((art_node4*)node)->offsets[i+1]])
					i++;
				else{
					((art_node4*)new_node)->keys[new_node->num_children]=((art_node4*)node)->keys[((art_node4*)node)->offsets[i]];
					((art_node4*)new_node)->children[new_node->num_children]=((art_node4*)node)->children[((art_node4*)node)->offsets[i]];
					((art_node4*)new_node)->offsets[new_node->num_children]=new_node->num_children;
					new_node->num_children++;
				}
			}
		}
		break;
	case NODE16:
		if(instruction == 1){
			if(node->num_children < 16){
				new_node = (art_node*) alloc_node(NODE16);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node16*)node)->keys[((art_node16*)node)->offsets[i]] == ((art_node16*)node)->keys[((art_node16*)node)->offsets[i+1]])
						i++;
					else{
						((art_node16*)new_node)->keys[new_node->num_children]=((art_node16*)node)->keys[((art_node16*)node)->offsets[i]];
						((art_node16*)new_node)->children[new_node->num_children]=((art_node16*)node)->children[((art_node16*)node)->offsets[i]];
						((art_node16*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE48);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node16*)node)->keys[((art_node16*)node)->offsets[i]] == ((art_node16*)node)->keys[((art_node16*)node)->offsets[i+1]])
						i++;
					else{
						((art_node48*)new_node)->keys[new_node->num_children]=((art_node16*)node)->keys[((art_node16*)node)->offsets[i]];
						((art_node48*)new_node)->children[new_node->num_children]=((art_node16*)node)->children[((art_node16*)node)->offsets[i]];
						((art_node48*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}else{
			if(node->num_children > 3){
				new_node = (art_node*) alloc_node(NODE16);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node16*)node)->keys[((art_node16*)node)->offsets[i]] == ((art_node16*)node)->keys[((art_node16*)node)->offsets[i+1]])
						i++;
					else{
						((art_node16*)new_node)->keys[new_node->num_children]=((art_node16*)node)->keys[((art_node16*)node)->offsets[i]];
						((art_node16*)new_node)->children[new_node->num_children]=((art_node16*)node)->children[((art_node16*)node)->offsets[i]];
						((art_node16*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE4);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node16*)node)->keys[((art_node16*)node)->offsets[i]] == ((art_node16*)node)->keys[((art_node16*)node)->offsets[i+1]])
						i++;
					else{
						((art_node4*)new_node)->keys[new_node->num_children]=((art_node16*)node)->keys[((art_node16*)node)->offsets[i]];
						((art_node4*)new_node)->children[new_node->num_children]=((art_node16*)node)->children[((art_node16*)node)->offsets[i]];
						((art_node4*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}
		break;
	case NODE48:
		if(instruction == 1){
			if(node->num_children < 48){
				new_node = (art_node*) alloc_node(NODE48);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node48*)node)->keys[((art_node48*)node)->offsets[i]] == ((art_node48*)node)->keys[((art_node48*)node)->offsets[i+1]])
						i++;
					else{
						((art_node48*)new_node)->keys[new_node->num_children]=((art_node48*)node)->keys[((art_node48*)node)->offsets[i]];
						((art_node48*)new_node)->children[new_node->num_children]=((art_node48*)node)->children[((art_node48*)node)->offsets[i]];
						((art_node48*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE256);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node48*)node)->keys[((art_node48*)node)->offsets[i]] == ((art_node48*)node)->keys[((art_node48*)node)->offsets[i+1]])
						i++;
					else{
						((art_node256*)new_node)->keys[new_node->num_children]=((art_node48*)node)->keys[((art_node48*)node)->offsets[i]];
						((art_node256*)new_node)->children[new_node->num_children]=((art_node48*)node)->children[((art_node48*)node)->offsets[i]];
						((art_node256*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}else{
			if(node->num_children > 12){
				new_node = (art_node*) alloc_node(NODE48);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node48*)node)->keys[((art_node48*)node)->offsets[i]] == ((art_node48*)node)->keys[((art_node48*)node)->offsets[i+1]])
						i++;
					else{
						((art_node48*)new_node)->keys[new_node->num_children]=((art_node48*)node)->keys[((art_node48*)node)->offsets[i]];
						((art_node48*)new_node)->children[new_node->num_children]=((art_node48*)node)->children[((art_node48*)node)->offsets[i]];
						((art_node48*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE16);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node48*)node)->keys[((art_node48*)node)->offsets[i]] == ((art_node48*)node)->keys[((art_node48*)node)->offsets[i+1]])
						i++;
					else{
						((art_node16*)new_node)->keys[new_node->num_children]=((art_node48*)node)->keys[((art_node48*)node)->offsets[i]];
						((art_node16*)new_node)->children[new_node->num_children]=((art_node48*)node)->children[((art_node48*)node)->offsets[i]];
						((art_node16*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}
		break;
	case NODE256:
		if(instruction == 1){

			new_node = (art_node*) alloc_node(NODE256);
			for(int i=0; i < node->num_children+node->num_remotion-1; i++){
				if(((art_node256*)node)->keys[((art_node256*)node)->offsets[i]] == ((art_node256*)node)->keys[((art_node256*)node)->offsets[i+1]])
					i++;
				else{
					((art_node256*)new_node)->keys[new_node->num_children]=((art_node256*)node)->keys[((art_node256*)node)->offsets[i]];
					((art_node256*)new_node)->children[new_node->num_children]=((art_node256*)node)->children[((art_node256*)node)->offsets[i]];
					((art_node256*)new_node)->offsets[new_node->num_children]=new_node->num_children;
					new_node->num_children++;
				}
			}
		}else{
			if(node->num_children > 37){
				new_node = (art_node*) alloc_node(NODE256);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node256*)node)->keys[((art_node256*)node)->offsets[i]] == ((art_node256*)node)->keys[((art_node256*)node)->offsets[i+1]])
						i++;
					else{
						((art_node256*)new_node)->keys[new_node->num_children]=((art_node256*)node)->keys[((art_node256*)node)->offsets[i]];
						((art_node256*)new_node)->children[new_node->num_children]=((art_node256*)node)->children[((art_node256*)node)->offsets[i]];
						((art_node256*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}else{
				new_node = (art_node*) alloc_node(NODE48);
				for(int i=0; i < node->num_children+node->num_remotion-1; i++){
					if(((art_node256*)node)->keys[((art_node256*)node)->offsets[i]] == ((art_node256*)node)->keys[((art_node256*)node)->offsets[i+1]])
						i++;
					else{
						((art_node48*)new_node)->keys[new_node->num_children]=((art_node256*)node)->keys[((art_node256*)node)->offsets[i]];
						((art_node48*)new_node)->children[new_node->num_children]=((art_node256*)node)->children[((art_node256*)node)->offsets[i]];
						((art_node48*)new_node)->offsets[new_node->num_children]=new_node->num_children;
						new_node->num_children++;
					}
				}
			}
		}
		break;
	default:return NULL;
	}

	copy_header(new_node, node);
	return new_node;
}


static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {

	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	if(memory_page(child) != page){
		persist();
	}
	while(n->n.substitute)
		n=n->n.substitute;
    if(n->n.num_children+n->n.num_remotion < 256){
    	n->keys[n->n.num_children] = c;
    	n->children[n->n.num_children] = (art_node*)child;
    	n->offsets[n->n.num_children]=n->n.num_children;
    	if(n->n.num_children > 0){
    		for(int i=0; i < n->n.num_children; i++){
    			if(n->keys[n->offsets[i]] < n->keys[n->offsets[n->n.num_children]]){
    				unsigned char offset=n->offsets[i];
    				n->offsets[i]=n->offsets[n->n.num_children];
    				n->offsets[n->n.num_children]=offset;
    				break;
    			}
    		}
    	}
    	n->n.num_children++;
    	insert_list((art_node*)n);
    }else{
    	art_node* new_node=clean_node((art_node*)n, 1);
    	insert_list(new_node);
    	n->n.substitute=new_node;
    	add_child256((art_node256*)new_node, ref, c, child);
    }
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	if(memory_page(child) != page){
		persist();
	}
	while(n->n.substitute)
		n=n->n.substitute;
    if (n->n.num_children+n->n.num_remotion < 48) {
    	n->keys[n->n.num_children] = c;
    	n->children[n->n.num_children] = (art_node*)child;
    	n->offsets[n->n.num_children]=n->n.num_children;
    	if(n->n.num_children > 0){
    		for(int i=0; i < n->n.num_children; i++){
    			if(n->keys[n->offsets[i]] < n->keys[n->offsets[n->n.num_children]]){
    				unsigned char offset=n->offsets[i];
    				n->offsets[i]=n->offsets[n->n.num_children];
    				n->offsets[n->n.num_children]=offset;
    				break;
    			}
    		}
    	}
        n->n.num_children++;
        insert_list((art_node*)n);
    } else {
        art_node* new_node=clean_node((art_node*)n, 1);
        insert_list(new_node);
        n->n.substitute=new_node;
        if(n->n.num_children < 48)
        	add_child48((art_node48*)new_node, ref, c, child);
        else
        	add_child256((art_node256*)new_node, ref, c, child);
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	if(memory_page(child) != page){
		persist();
	}
	while(n->n.substitute)
		n=n->n.substitute;
    if (n->n.num_children+n->n.num_remotion < 16) {
    	n->keys[n->n.num_children] = c;
    	n->children[n->n.num_children] = (art_node*)child;
    	n->offsets[n->n.num_children]=n->n.num_children;
    	if(n->n.num_children > 0){
    		for(int i=0; i < n->n.num_children; i++){
    			if(n->keys[n->offsets[i]] < n->keys[n->offsets[n->n.num_children]]){
    				unsigned char offset=n->offsets[i];
    				n->offsets[i]=n->offsets[n->n.num_children];
    				n->offsets[n->n.num_children]=offset;
    				break;
    			}
    		}
    	}
        n->n.num_children++;
        insert_list((art_node*)n);
    } else {
    	 art_node* new_node=clean_node((art_node*)n, 1);
    	 insert_list(new_node);
    	 n->n.substitute=new_node;
    	 if(n->n.num_children < 16)
    		 add_child16((art_node16*)new_node, ref, c, child);
    	 else
    		 add_child48((art_node48*)new_node, ref, c, child);
    }
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	if(memory_page(child) != page){
		persist();
	}
	while(n->n.substitute)
		n=n->n.substitute;
    if (n->n.num_children+n->n.num_remotion < 4) {
    	n->keys[n->n.num_children] = c;
    	n->children[n->n.num_children] = (art_node*)child;
    	n->offsets[n->n.num_children]=n->n.num_children;
    	if(n->n.num_children > 0){
    		for(int i=0; i < n->n.num_children; i++){
    			if(n->keys[n->offsets[i]] < n->keys[n->offsets[n->n.num_children]]){
    				unsigned char offset=n->offsets[i];
    				n->offsets[i]=n->offsets[n->n.num_children];
    				n->offsets[n->n.num_children]=offset;
    				break;
    			}
    		}
    	}
        n->n.num_children++;
        insert_list((art_node*)n);
    } else {
    	 art_node* new_node=clean_node((art_node*)n, 1);
    	 insert_list(new_node);
    	 n->n.substitute=new_node;
    	 if(n->n.num_children < 4)
    		 add_child4((art_node4*)new_node, ref, c, child);
    	 else
    		 add_child16((art_node16*)new_node, ref, c, child);
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

	    while(n->stable)n=n->substitute;

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
	        l->substitute=new_node;
	        insert_list((void*)l);
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
	        n->substitute=new_node;
	        insert_list((void*)n);

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
    void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	while(n->n.substitute)
		n=n->n.substitute;
	if(n->n.num_children+n->n.num_remotion < 4){

		int idx=l - n->children;

		grave* g=vmem_malloc(vmem,sizeof(grave));

		if(memory_page((void*)g) != page){
			persist();
		}
		g->node=(void*)n->children[idx];

		n->keys[n->n.num_children-1]=n->keys[idx];
		n->children[n->n.num_children-1]=(void*)g;
		n->offsets[n->n.num_children]=n->n.num_children;
		n->n.num_remotion++;
		n->n.num_remotion++;
		n->n.num_children--;
		insert_list((art_node*)n);
	}else{
		art_node* new_node=clean_node((art_node*)n, 0);
		insert_list(new_node);
		remove_child4((art_node4*)new_node, ref,l);

	}
}


static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	while(n->n.substitute)
		n=n->n.substitute;
	if(n->n.num_children+n->n.num_remotion < 16 && n->n.num_children > 3){

		int idx=l - n->children;

		grave* g=vmem_malloc(vmem,sizeof(grave));
		if(memory_page((void*)g) != page){
			persist();
		}
		g->node=(void*)n->children[idx];

		n->keys[n->n.num_children-1]=n->keys[idx];
		n->children[n->n.num_children-1]=(void*)g;
		n->offsets[n->n.num_children]=n->n.num_children;
		n->n.num_remotion++;
		n->n.num_remotion++;
		n->n.num_children--;
		insert_list((art_node*)n);
	}else{
		art_node* new_node=clean_node((art_node*)n, 0);
		insert_list(new_node);
		n->n.substitute=new_node;
		if(n->n.num_children > 3)
			remove_child16((art_node16*)new_node,ref,l);
		else
			remove_child4((art_node4*)new_node, ref,l);

	}
}


static void remove_child48(art_node48 *n, art_node **ref, art_node **l) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	while(n->n.substitute)
		n=n->n.substitute;
	if(n->n.num_children+n->n.num_remotion < 48 && n->n.num_children > 12){

		int idx=l - n->children;

		grave* g=vmem_malloc(vmem,sizeof(grave));
		if(memory_page((void*)g) != page){
			persist();
		}
		g->node=(void*)n->children[idx];

		n->keys[n->n.num_children-1]=n->keys[idx];
		n->children[n->n.num_children-1]=(void*)g;
		n->offsets[n->n.num_children]=n->n.num_children;
		n->n.num_remotion++;
		n->n.num_remotion++;
		n->n.num_children--;
		insert_list((art_node*)n);
	}else{
		art_node* new_node=clean_node((art_node*)n, 0);
		insert_list(new_node);
		n->n.substitute=new_node;
		if(n->n.num_children > 12)
			remove_child48((art_node48*)new_node,ref,l);
		else
			remove_child16((art_node16*)new_node, ref,l);

	}
}

static void remove_child256(art_node256 *n, art_node **ref, art_node **l) {
	if(search_list((void*)n) == -1 && n->children[n->n.stable])
	{
		printf("unstable node");
		return;
	}
	if(n->n.parent==NULL)
		n->n.parent=(void**)ref;
	while(n->n.substitute)
		n=n->n.substitute;
    if(n->n.num_children+n->n.num_remotion < 256 && n->n.num_children > 37){

    	int idx=l - n->children;

    	grave* g=vmem_malloc(vmem,sizeof(grave));
    	if(memory_page((void*)g) != page){
    		persist();
    	}
    	g->node=(void*)n->children[idx];

    	n->keys[n->n.num_children-1]=n->keys[idx];
    	n->children[n->n.num_children-1]=(void*)g;
    	n->offsets[n->n.num_children]=n->n.num_children;
    	n->n.num_remotion++;
    	n->n.num_remotion++;
    	n->n.num_children--;
    	insert_list((art_node*)n);
    }else{
    	art_node* new_node=clean_node((art_node*)n, 0);
    	insert_list(new_node);
    	n->n.substitute=new_node;
    	if(n->n.num_children > 37)
    		remove_child256((art_node256*)new_node,ref,l);
    	else
    		remove_child48((art_node48*)new_node, ref,l);
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

    // Handle hitting a leaf node
    /*if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }*/

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
        free(l);
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
