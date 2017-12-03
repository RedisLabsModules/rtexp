/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "librtexp.h"

#include "trie/triemap.h"
#include "util/heap.h"
#include "util/millisecond_time.h"
#include "util/rmalloc.h"

#include <time.h>

#define RTX_LATANCY_NS 50

/***************************
 *   Datastructure Utils
 ***************************/
RTXElementNode* newRTXElementNode(char* key, size_t len, mstime_t timestamp_ms, int version) {
  RTXElementNode* node = malloc(sizeof(RTXElementNode));
  node->key = rm_strndup(key, len);
  node->len = len;
  node->exp.time = timestamp_ms;
  node->exp.version = version;
  return node;
}

void freeRTXElementNode(RTXElementNode* node) {
  rm_free(node->key);
  rm_free(node);
}

size_t expiration_count(RTXStore* store){
  if (store)
    return heap_count(store->sorted_keys);    
  return 0;
}

void RTXStore_Free(RTXStore* store) {
  TrieMap_Free(store->element_node_map, NULL);
  while (heap_count(store->sorted_keys) != 0) {
    RTXElementNode* node = heap_poll(store->sorted_keys);
    freeRTXElementNode(node);
  }  
  heap_free(store->sorted_keys);
  rm_free(store);
}

/*
 * Update expiration, increase the version num, keep the name
 */
void* _trie_node_updater(void* oldval, void* newval) {
  RTXExpiration *n = newval, *o = oldval;
  if (o) {
    n->version = o->version+1;
    rm_free(o);
  }
  
  return newval;  
}

int _cmp_node(const void* node_a, const void* node_b, const void* udata) {
  const RTXElementNode *a=node_a, *b=node_b;
  return b->exp.time - a->exp.time;
}

/*
 * @return True if node has a valid version
 */
int _is_valid_node(RTXStore* store, RTXElementNode* node) {
  if (node == NULL || node == TRIEMAP_NOTFOUND || node->key == NULL) {
    return 0;
  }
  char* key = node->key;
  RTXExpiration* stored_node = TrieMap_Find(store->element_node_map, key, strlen(key));
  if (stored_node != NULL && stored_node != TRIEMAP_NOTFOUND)
    return (node->exp.version == stored_node->version);
  else
    return 0;
}

/*
 * @return next valid element node, NULL if DS empty
 */
RTXElementNode* _peek_next(RTXStore* store) {
  while (heap_count(store->sorted_keys) != 0) {
    RTXElementNode* node = heap_peek(store->sorted_keys);
    // printf("peeked and saw:%s\n", node->key);
    if (_is_valid_node(store, node)) {
      return node;
    } else {
      RTXElementNode* polled_node = heap_poll(store->sorted_keys);
      freeRTXElementNode(polled_node);
    }
  }
  return NULL;
}

RTXStore* newRTXStore(void) {
  RTXStore* store = malloc(sizeof(RTXStore));
  store->sorted_keys = heap_new(_cmp_node, NULL);
  store->element_node_map = NewTrieMap();
  return store;
}

/************************************
 *   General DS handling functions
 ************************************/

/*
 * Insert expiration for a new key or update an existing one
 * @return RTXS_OK on success, RTXS_ERR on error
 */
int set_element_exp(RTXStore* store, char* key, size_t len, mstime_t ttl_ms) {
  mstime_t timestamp_ms = current_time_ms() + ttl_ms;
  RTXExpiration* exp = malloc(sizeof(*exp)); 
  //printf("settting timestamp to be %llu\n", timestamp_ms);
  exp->time = timestamp_ms;
  exp->version = 0; 
  
  int trie_result = TrieMap_Add(store->element_node_map, key, len, exp, _trie_node_updater);
  
  // EXP's version was updated by the trie updater callback
  RTXElementNode *node = newRTXElementNode(key, len, exp->time, exp->version);

  int heap_result = heap_offer(&store->sorted_keys, node);
  if (heap_result != 0) {  // we failed inserting into the heap, back out of everything
    // TODO: for now let's (wrongly) assume we will not get here and just return with error, but
    //       this needs writing
    return RTXS_ERR;
  }
  return RTXS_OK;
}

/*
 * Get the expiration value for the given key
 * @return datetime of expiration (in milliseconds) on success, -1 on error
 */
mstime_t get_element_exp(RTXStore* store, char* key) {
  RTXExpiration* exp = TrieMap_Find(store->element_node_map, key, strlen(key));
  if (exp != NULL && exp != TRIEMAP_NOTFOUND) {
    return exp->time;
  }
  return -1;
}

/*
 * Remove expiration from the data store for the given key
 * @return RTXS_OK
 */
int del_element_exp(RTXStore* store, char* key) {
  TrieMap* t = store->element_node_map;
  TrieMap_Delete(t, key, strlen(key), NULL);
  return RTXS_OK;
}

/*
 * @return the closest element expiration datetime (in milliseconds), or -1 if DS is empty
 */
mstime_t next_at(RTXStore* store) {
  RTXElementNode* node = _peek_next(store);
  if (node == NULL) {  // empty_DS
    return -1;
  } else {
    return node->exp.time;
  }
}

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
char* pop_next(RTXStore* store) {
  RTXElementNode* node = _peek_next(store);
  if (node != NULL) {  // a non empty DS
    node = heap_poll(store->sorted_keys);
    char* key = rm_strdup(node->key);
    del_element_exp(store, node->key);
    freeRTXElementNode(node);
    return key;
  }
  return NULL;
}

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the key of the element with closest expiration datetime
 */
char* pop_wait(RTXStore* store) {
  mstime_t sleep_target_ms = next_at(store);
  mstime_t time_to_wait = sleep_target_ms - current_time_ms();
  if (time_to_wait > 0) {
    struct timespec ttw, rem;
    ttw.tv_sec = 0;
    ttw.tv_nsec = time_to_wait * 1000 - RTX_LATANCY_NS;
    nanosleep(&ttw, &rem);  // TODO: for now we'll assume this is allways fully successfull
  }
  return pop_next(store);
}
