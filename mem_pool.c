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

    if(!pool_store){
        return ALLOC_CALLED_AGAIN;
    }
        for (int i = 0; i < pool_store_size; i++) {
            if(pool_store[i] != NULL){
//                if(mem_pool_close((pool_pt)(pool_store[i])) != ALLOC_OK){
//                    return ALLOC_FAIL;
//                }
                mem_pool_close((pool_pt) pool_store[i]);
            }
        }
        free(pool_store);
        pool_store_size = 0;
        pool_store_capacity = 0;
        pool_store = NULL;
        return ALLOC_OK;

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
////



alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    pool_mgr_pt manager = (pool_mgr_pt) pool;
    if(manager->gap_ix[0].node == NULL){
        return NULL;
    }
    if (((float) manager -> used_nodes / manager -> total_nodes) > MEM_NODE_HEAP_FILL_FACTOR) {
        alloc_status resize = _mem_resize_node_heap(manager);
        if (resize != ALLOC_OK) {
            return NULL;
        }
    }
    if (manager -> used_nodes >= manager -> total_nodes) {
        return NULL;
    }
    node_pt new_node;
    int i = 0;
    if(manager -> pool.policy == FIRST_FIT) {
        node_pt this_node = manager -> node_heap;
        while ((manager -> node_heap[i].allocated != 0) || (manager -> node_heap[i].alloc_record.size < size) && i < manager -> total_nodes) {
            ++i;
        }
        if ( i == manager -> total_nodes) {
            return NULL;
        }
        new_node = &manager -> node_heap[i];
    }
    else if (manager -> pool.policy == BEST_FIT) {
        if (manager ->pool.num_gaps > 0) {
            while (i < manager -> pool.num_gaps && manager -> gap_ix[i+1].size >= size) {
                if (manager -> gap_ix[i].size == size ) {
                    break;
                }
                ++i;
            }
        } else {
            return NULL;
        }
        new_node = manager -> gap_ix[i].node;
    }
    manager -> pool.num_allocs++;
    manager -> pool.alloc_size += size;
    int size_of_gap = 0;
    if(new_node -> alloc_record.size - size > 0) {
        size_of_gap = new_node -> alloc_record.size - size;
    }
    if(_mem_remove_from_gap_ix(manager,size,new_node) != ALLOC_OK){
        return NULL;
    }
    new_node -> allocated = 1;
    new_node -> used = 1;
    new_node -> alloc_record.size = size;
    if (size_of_gap > 0) {
        int j = 0;
        while (manager -> node_heap[j].used != 0) {
            ++j;
        }
        node_pt new_gap_created = &manager -> node_heap[j];
        if (new_gap_created != NULL) {
            new_gap_created -> used = 1;
            new_gap_created -> allocated = 0;
            new_gap_created -> alloc_record.size = size_of_gap;
        }
        manager -> used_nodes++;
        new_gap_created -> next = new_node -> next;
        if(new_node -> next != NULL) {
            new_node -> next -> prev = new_gap_created;
        }
        new_node -> next = new_gap_created;
        new_gap_created -> prev = new_node;
        _mem_add_to_gap_ix(manager, size_of_gap, new_gap_created);
    }
    return (alloc_pt) new_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    pool_mgr_pt manager = (pool_mgr_pt) pool;
    node_pt delete_node;
    node_pt node = (node_pt) alloc;
    for(int i = 0; i < manager -> total_nodes; ++i) {
        if (node == &manager -> node_heap[i]) {
            delete_node = &manager -> node_heap[i];
            break;
        }
    }
    if(delete_node == NULL) {
        return ALLOC_NOT_FREED;
    }
    delete_node -> allocated = 0;
    manager -> pool.num_allocs--;
    manager -> pool.alloc_size -= delete_node -> alloc_record.size;
    node_pt node_to_merge = NULL;
    if (delete_node -> next != NULL && delete_node -> next -> used == 1 && delete_node -> next -> allocated == 0 ) {
        node_to_merge = delete_node->next;
        _mem_remove_from_gap_ix(manager, node_to_merge->alloc_record.size, node_to_merge);
        delete_node->alloc_record.size = delete_node->alloc_record.size + delete_node->next->alloc_record.size;
        node_to_merge->used = 0;
        manager->used_nodes--;
        if (node_to_merge->next) {
            node_to_merge->next->prev = delete_node;
            delete_node->next = node_to_merge->next;
        } else {
            delete_node->next = NULL;
        }
        node_to_merge->next = NULL;
        node_to_merge->prev = NULL;
        node_to_merge->alloc_record.mem = NULL;
        node_to_merge->alloc_record.size = 0;
    };
    _mem_add_to_gap_ix(manager, delete_node ->alloc_record.size, delete_node);
    node_pt previous_node;
    if(delete_node -> prev != NULL && delete_node -> prev -> allocated == 0 && delete_node -> prev -> used == 1) {
        previous_node = delete_node->prev;
        _mem_remove_from_gap_ix(manager, previous_node->alloc_record.size, previous_node);
        _mem_remove_from_gap_ix(manager, delete_node->alloc_record.size, delete_node);
        previous_node->alloc_record.size = delete_node->alloc_record.size + delete_node->prev->alloc_record.size;
        delete_node->alloc_record.mem = NULL;
        delete_node->alloc_record.size = 0;
        delete_node->used = 0;
        delete_node->allocated = 0;
        manager->used_nodes--;
        if (delete_node->next) {
            previous_node->next = delete_node->next;
            delete_node->next->prev = previous_node;
        } else {
            previous_node->next = NULL;
        }
        delete_node->next = NULL;
        delete_node->prev = NULL;
        _mem_add_to_gap_ix(manager, previous_node->alloc_record.size, previous_node);
    };
    return ALLOC_OK;
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
//static alloc_status _mem_resize_pool_store() {
//    //Check if the pool_store needs to be resized
//    //  "necessary" to resize when size/cap > 0.75
//    if (((float)pool_store_size / (float)pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR){
//
//        unsigned int expandFactor = pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR;
//        pool_store = (pool_mgr_pt *)realloc(pool_store, (sizeof(pool_mgr_pt) * expandFactor));
//        //Verify the realloc worked
//        if(pool_store == NULL){
//            return ALLOC_FAIL;
//        }
//        //double check that pool_store isnt NULL after realloc
//        assert(pool_store);
//        //Update capacity variable
//        pool_store_capacity = expandFactor;
//
//        return ALLOC_OK;
//    }
//
//    return ALLOC_OK;
//}




//static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr_ptr) {
//
//    //Check if the node_heap needs to be resized
//    //  "necessary" to resize when size/cap > 0.75
//    if(((float)pool_mgr_ptr->used_nodes / (float)pool_mgr_ptr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR){
//
//        //Reallocate more nodes to the node_heap, by the size of the
//        //Perform the realloc straight to the node_heap pointer
//        pool_mgr_ptr->node_heap = (node_pt)realloc(pool_mgr_ptr->node_heap,
//                                                   MEM_NODE_HEAP_EXPAND_FACTOR * pool_mgr_ptr->total_nodes * sizeof(node_t));
//        //Check and see if the realloc failed
//        if (NULL == pool_mgr_ptr->node_heap){
//
//            return ALLOC_FAIL;
//        }
//        //Make sure to update the number of nodes!  This is a prop of the pool_mgr_t
//        pool_mgr_ptr->total_nodes *= MEM_NODE_HEAP_EXPAND_FACTOR;
//
//        return ALLOC_OK;
//    }
//
//    return ALLOC_OK;
//}



//static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
//
//    float expandFactor = (float)MEM_GAP_IX_EXPAND_FACTOR * (float)pool_mgr->gap_ix_capacity;
//    //Does gap_ix need to be resized?
//    if((((float)pool_mgr->pool.num_gaps)/(pool_mgr->gap_ix_capacity)) > MEM_GAP_IX_FILL_FACTOR){
//        //resize if needed
//        //Perform realloc to increase size of gap_ix
//        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, expandFactor * sizeof(gap_t));
//        //update metadata
//        //  Expand the gap_ix_capacity by the same factor as the realloc
//        pool_mgr->gap_ix_capacity *= MEM_GAP_IX_EXPAND_FACTOR;
//        //Check and make sure it worked
//        if(NULL == pool_mgr->gap_ix){
//
//            return ALLOC_FAIL;
//        }
//        return ALLOC_OK;
//    }
//    return ALLOC_OK;
//}

//static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
//                                       size_t size,
//                                       node_pt node) {
//
//    // expand the gap index, if necessary (call the function)
//    _mem_resize_gap_ix(pool_mgr);
//    // add the entry at the end
//    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
//    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
//    // update metadata (num_gaps)
//    pool_mgr->pool.num_gaps ++;
//    // sort the gap index (call the function)
//    alloc_status sortCheck = _mem_sort_gap_ix(pool_mgr);
//    // check success
//    if(sortCheck == ALLOC_OK){
//        return ALLOC_OK;
//    }
//    return ALLOC_FAIL;
//}



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
    if(((float)pool_mgr->used_nodes/pool_mgr->total_nodes) > MEM_GAP_IX_FILL_FACTOR){
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
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
    pool_mgr->pool.num_gaps++;
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
    if(index != -1) {
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
    }

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    if(pool_mgr->pool.num_gaps < 2){
        return ALLOC_OK;
    }
    int count = pool_mgr->pool.num_gaps - 1;
    gap_t gap1 = pool_mgr->gap_ix[count];
    gap_t gap2 = pool_mgr->gap_ix[count-1];
    for(int i = count; i > 0; i--){
        if (gap1.size < gap2.size
            || (gap1.size == gap2.size
                && gap1.node->alloc_record.mem < gap2.node->alloc_record.mem)) {
            gap_t temp = gap1;
            gap1 = gap2;
            gap2 = temp;
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




