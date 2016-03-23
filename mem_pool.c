/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    if(pool_store != NULL){
        return ALLOC_CALLED_AGAIN; //could this report incorrect status for dealloc errors
    }
    pool_store = (pool_mgr_pt *)calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
    if(pool_store == NULL){
        return ALLOC_FAIL;
    }
    pool_store_size = 0;
    pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
    return ALLOC_OK;
}

alloc_status mem_free() {

    if(pool_store != NULL){
        for (int i = 0; i < pool_store_size; i++) {
            if(pool_store[i] != NULL){
                if(mem_pool_close((pool_pt)(pool_store[i])) != ALLOC_OK){
                    return ALLOC_FAIL;
                }
            }
        }
        free(pool_store);
        pool_store_size = 0;
        pool_store_capacity = 0;
        pool_store = NULL;
        return ALLOC_OK;
    }else{
        return ALLOC_CALLED_AGAIN;
    }

}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    if(pool_store != NULL){
        if(_mem_resize_pool_store() == ALLOC_OK){
            pool_mgr_pt  new_mem_pool_manager = malloc(sizeof(pool_mgr_t));
            if(new_mem_pool_manager != NULL){
                new_mem_pool_manager->pool.mem = malloc(size);
                if(NULL != new_mem_pool_manager->pool.mem){
                    new_mem_pool_manager->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
                    if(NULL != new_mem_pool_manager->node_heap) {
                        new_mem_pool_manager->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
                        if(NULL != new_mem_pool_manager->gap_ix){

                            new_mem_pool_manager->node_heap[0].used = 1;
                            new_mem_pool_manager->node_heap[0].allocated = 0;
                            new_mem_pool_manager->node_heap[0].alloc_record.size = size;
                            new_mem_pool_manager->node_heap[0].alloc_record.mem = new_mem_pool_manager->pool.mem;
                            new_mem_pool_manager->pool.alloc_size = 0;
                            new_mem_pool_manager->pool.total_size = size;
                            new_mem_pool_manager->pool.policy = policy;
                            new_mem_pool_manager->pool.num_gaps = 0;
                            new_mem_pool_manager->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
                            new_mem_pool_manager->used_nodes = 1;
                            new_mem_pool_manager->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
                            pool_store[pool_store_size] = new_mem_pool_manager;
                            pool_store_size ++;
                            _mem_add_to_gap_ix(new_mem_pool_manager, size, &new_mem_pool_manager->node_heap[0]);

                            return (pool_pt)new_mem_pool_manager;

                        }else{
                            free(new_mem_pool_manager->node_heap);
                            free(new_mem_pool_manager->pool.mem);
                            free(new_mem_pool_manager);
                            return NULL;
                        }
                    }else{
                        free(new_mem_pool_manager->pool.mem);
                        free(new_mem_pool_manager);
                        return NULL;
                    }
                }else{
                    //Allocating pool failed free manager
                    free(new_mem_pool_manager);
                    return NULL;
                }
            }else{
                return NULL;
            }

        }else{
            return NULL;
        }


    } else{
        return NULL;
    }
    // make sure there the pool store is allocated
    // expand the pool store, if necessary
    // allocate a new mem pool mgr
    // check success, on error return null
    // allocate a new memory pool
    // check success, on error deallocate mgr and return null
    // allocate a new node heap
    // check success, on error deallocate mgr/pool and return null
    // allocate a new gap index
    // check success, on error deallocate mgr/pool/heap and return null
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    //   initialize top node of gap index
    //   initialize pool mgr
    //   link pool mgr to pool store
    // return the address of the mgr, cast to (pool_pt)

    return NULL;
}

alloc_status mem_pool_close(pool_pt pool) {
    pool_mgr_pt local_pool_mgr_pt = (pool_mgr_pt)pool;

    if(NULL == pool){
        return ALLOC_NOT_FREED;
    }
    if(pool->num_gaps == 1 && pool->num_allocs == 0){
        free(local_pool_mgr_pt->node_heap);
        free(local_pool_mgr_pt->pool.mem);
        free(local_pool_mgr_pt->gap_ix);
        for (int i = 0; i < pool_store_size; i++) {
            if (pool_store[i] == local_pool_mgr_pt) {
                pool_store[i] = NULL;
            }
        }
        free(local_pool_mgr_pt);
        return ALLOC_OK;
    }else{
        return ALLOC_NOT_FREED;
    }
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if this pool is allocated
    // check if pool has only one gap
    // check if it has zero allocations
    // free memory pool
    // free node heap
    // free gap index
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    // free mgr

}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    pool_mgr_pt local_pool_mgr_pt = (pool_mgr_pt)pool;
    if(local_pool_mgr_pt->pool.num_gaps == 0){
        return NULL;
    }
    if(_mem_resize_node_heap(local_pool_mgr_pt) != ALLOC_OK){
        exit(1);
    }
    if(local_pool_mgr_pt->used_nodes > local_pool_mgr_pt->total_nodes){
        exit(1);
    }
    node_pt new_node = NULL;
    int indexOfFirst = -1;
    int sizeOfClosest = (int)size+10;
    if(local_pool_mgr_pt->pool.policy == FIRST_FIT){
        for (int i = 0; i < local_pool_mgr_pt->total_nodes; i++) {
            if(local_pool_mgr_pt->node_heap[i].used == 1 &&local_pool_mgr_pt->node_heap[i].allocated == 0 && local_pool_mgr_pt->node_heap[i].alloc_record.size >= size){
                new_node = &(local_pool_mgr_pt->node_heap[i]);
                indexOfFirst = i;
                sizeOfClosest = (int)local_pool_mgr_pt->node_heap[i].alloc_record.size;
                break;
            }
        }
        if(indexOfFirst == -1){
            return NULL;
        }
        local_pool_mgr_pt->pool.num_allocs++;
        local_pool_mgr_pt->pool.alloc_size+=size;
    }else{
        int indexOfClosest = -1;
        sizeOfClosest = (int)size+10;
        for (int i = 0; i < local_pool_mgr_pt->gap_ix_capacity; i++) {
            if(size-local_pool_mgr_pt->gap_ix[i].size < sizeOfClosest && size-local_pool_mgr_pt->gap_ix[i].size >= 0){
                sizeOfClosest = (int)local_pool_mgr_pt->gap_ix[i].size;
                indexOfClosest = i;
            }
        }
        if(indexOfClosest == -1){
            return NULL;
        }
        new_node = local_pool_mgr_pt->gap_ix[indexOfClosest].node;

    }
    local_pool_mgr_pt->pool.num_allocs++;
    local_pool_mgr_pt->pool.alloc_size+=size;
    new_node->allocated = 1;
    new_node->alloc_record.size = size;
    new_node->next = NULL;
    new_node->alloc_record.mem = malloc(size);
    if(new_node == NULL){
        return NULL;
    }
    size_t newGap = (sizeOfClosest - size);
    if(_mem_remove_from_gap_ix(local_pool_mgr_pt,size,new_node) != ALLOC_OK){
        return NULL;
    }
    node_pt new_gap = NULL;
    if(newGap > 0){
        int found = 0;
        for (int i=0; i < local_pool_mgr_pt->total_nodes; i++){
            //   make sure one was found
            if (local_pool_mgr_pt->node_heap[i].used == 0){
                //   initialize it to a gap node
                found = 1;
                new_gap = &(local_pool_mgr_pt->node_heap[i]);
                new_gap->allocated = 0;
                new_gap->used = 1;
                local_pool_mgr_pt->used_nodes++;
                new_gap->alloc_record.mem = new_node->alloc_record.mem + size;
                new_gap->alloc_record.size = newGap;
                break;
            }
        }
        if(found == 0){
            return NULL;
        }
        new_gap->next = new_node->next;
        if(new_node->next != NULL){
            new_node->next->prev = new_gap;
        }else{
            new_gap->next = NULL;
        }
        new_node->next = new_gap;
        new_gap->prev = new_node;
        if(_mem_add_to_gap_ix(local_pool_mgr_pt, newGap, new_gap) != ALLOC_OK){
            return NULL;
        }
        return (alloc_pt)new_node;

    }

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if any gaps, return null if none
    // expand heap node, if necessary, quit on error
    // check used nodes fewer than total nodes, quit on error
    // get a node for allocation:
    // if FIRST_FIT, then find the first sufficient node in the node heap
    // if BEST_FIT, then find the first sufficient node in the gap index
    // check if node found
    // update metadata (num_allocs, alloc_size)
    // calculate the size of the remaining gap, if any
    // remove node from gap index

    // convert gap_node to an allocation node of given size
    // adjust node heap:
    //   if remaining gap, need a new node
    //   find an unused one in the node heap
    //   make sure one was found
    //   initialize it to a gap node
    //   update metadata (used_nodes)
    //   update linked list (new node right after the node for allocation)
    //   add to gap index
    //   check if successful
    // return allocation record by casting the node to (alloc_pt)

    return NULL;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {

    pool_mgr_pt local_pool_mgr = (pool_mgr_pt)pool;
    node_pt local_node = (node_pt)alloc;
    int found = -1;
    for (int i = 0; i < local_pool_mgr->total_nodes; i++) {
        if(&local_pool_mgr->node_heap[i] ==  local_node){
            found = 1;
            break;
        }
    }
    if(found != -1){
        local_node->allocated = 0;
        local_pool_mgr->pool.alloc_size -= alloc->size;
        local_pool_mgr->pool.num_allocs--;
        if(local_node->next != NULL && local_node->next->allocated ==0){
            if(_mem_remove_from_gap_ix(local_pool_mgr, local_node->next->alloc_record.size, local_node->next) != ALLOC_OK){
                return ALLOC_FAIL;
            }
            local_node->next->used = 0;
            local_pool_mgr->used_nodes--;
            local_node->alloc_record.size += local_node->next->alloc_record.size;
            node_pt  delete_me = local_node->next;
            if(local_node->next->next){
                local_node->next = local_node->next->next;
                local_node->next->next->prev = local_node;
            }else{
                local_node->next = NULL;
            }
            delete_me->next = NULL;
            delete_me->prev = NULL;
        }
        if(local_node->prev != NULL && local_node->prev->allocated == 0){
            if(_mem_remove_from_gap_ix(local_pool_mgr, local_node->alloc_record.size, local_node->prev) != ALLOC_OK){
                return ALLOC_FAIL;
            }
            local_node->prev->alloc_record.size += local_node->alloc_record.size;
            local_pool_mgr->used_nodes--;
            local_node->used = 0;
            node_pt previous = local_node->prev;
            if(local_node->next){
                local_node->next->prev = local_node->prev;
                local_node->prev->next = local_node->next;
            }else{
                local_node->prev->next = NULL;
            }
            local_node->prev = NULL;
            local_node->next = NULL;
            local_node = previous;
        }
    }else{
        return ALLOC_FAIL;
    }
    if(_mem_add_to_gap_ix(local_pool_mgr, local_node->alloc_record.size, local_node) != ALLOC_OK){
        return ALLOC_FAIL;
    }
    return ALLOC_OK;

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // get node from alloc by casting the pointer to (node_pt)
    // find the node in the node heap
    // this is node-to-delete
    // make sure it's found
    // convert to gap node
    // update metadata (num_allocs, alloc_size)
    // if the next node in the list is also a gap, merge into node-to-delete
    //   remove the next node from gap index
    //   check success
    //   add the size to the node-to-delete
    //   update node as unused
    //   update metadata (used nodes)
    //   update linked list:
    /*
                    if (next->next) {
                        next->next->prev = node_to_del;
                        node_to_del->next = next->next;
                    } else {
                        node_to_del->next = NULL;
                    }
                    next->next = NULL;
                    next->prev = NULL;
     */

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    //   remove the previous node from gap index
    //   check success
    //   add the size of node-to-delete to the previous
    //   update node-to-delete as unused
    //   update metadata (used_nodes)
    //   update linked list
    /*
                    if (node_to_del->next) {
                        prev->next = node_to_del->next;
                        node_to_del->next->prev = prev;
                    } else {
                        prev->next = NULL;
                    }
                    node_to_del->next = NULL;
                    node_to_del->prev = NULL;
     */
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

    return ALLOC_FAIL;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    pool_mgr_pt  local_pool_mgr = (pool_mgr_pt) pool;
    pool_segment_pt  seg_list = calloc(local_pool_mgr->used_nodes, sizeof(pool_segment_t));
    if(seg_list == NULL){
        return;
    }
    pool_segment_pt seg = seg_list;
    node_pt local_node = local_pool_mgr->node_heap;
    for(int i = 0; i < local_pool_mgr->used_nodes; i++){
        seg->allocated = local_node->allocated;
        seg->size = local_node->alloc_record.size;
        local_node = local_node->next;
        seg++;

    }
    *num_segments = local_pool_mgr->used_nodes;
    *segments = seg_list;
    // get the mgr from the pool
    // allocate the segments array with size == used_nodes
    // check successful
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/


static alloc_status _mem_resize_pool_store() {

    if (((float)pool_store_size / (float)pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR){
        pool_store = (pool_mgr_pt *)realloc(pool_store, (sizeof(pool_mgr_pt)*(pool_store_capacity * MEM_EXPAND_FACTOR)));
        if(pool_store == NULL){
            return ALLOC_FAIL;
        }
        //assert(pool_store);//double check that pool_store isnt NULL after realloc

        pool_store_capacity = pool_store_capacity * MEM_EXPAND_FACTOR;
        return ALLOC_OK;

    }

    return ALLOC_OK;

}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above
    if(((float)pool_mgr->pool.num_gaps/pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR){
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(node_t) * (pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR));
        if(pool_mgr->node_heap == NULL){
            return ALLOC_FAIL;
        }
        pool_mgr->total_nodes = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;
    }
    return ALLOC_OK;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above
    if (((float) pool_mgr->pool.num_gaps/pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR){
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, sizeof(gap_t) * (pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR));
        if(pool_mgr->gap_ix == NULL){
            return ALLOC_FAIL;
        }
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;
    }
    return ALLOC_OK;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {
    if(_mem_resize_gap_ix(pool_mgr) != ALLOC_OK){
        return ALLOC_FAIL;
    }
    pool_mgr->pool.num_gaps++;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
    if(_mem_sort_gap_ix(pool_mgr) != ALLOC_OK){
        return ALLOC_FAIL;
    }
    return ALLOC_OK;

    // expand the gap index, if necessary (call the function)
    // add the entry at the end
    // update metadata (num_gaps)
    // sort the gap index (call the function)
    // check success

}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    int index = -1;
    for (int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        if(pool_mgr->gap_ix[i].node == node) {
            index = i;
            break;
        }
    }
    if(index == -1){
        return ALLOC_FAIL;
    }
    for (int j = index; j < pool_mgr->pool.num_gaps; j++) {
        pool_mgr->gap_ix[j].node = pool_mgr->gap_ix[j + 1].node;
        pool_mgr->gap_ix[j].size = pool_mgr->gap_ix[j + 1].size;
    }
    pool_mgr->pool.num_gaps--;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    // find the position of the node in the gap index
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    // update metadata (num_gaps)
    // zero out the element at position num_gaps!

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    if(pool_mgr->pool.num_gaps < 2){
        return ALLOC_OK;
    }
    for(int i = pool_mgr->pool.num_gaps - 1; i > 0; i--){
        if (pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i - 1].size
            || (pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i - 1].size
                && pool_mgr->gap_ix[i].node->alloc_record.mem < pool_mgr->gap_ix[i-1].node->alloc_record.mem)) {
            gap_t temp = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
            pool_mgr->gap_ix[i - 1] = temp;
        }
    }
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)

    return ALLOC_OK;
}


