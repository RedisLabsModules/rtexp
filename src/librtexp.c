/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "librtexp.h"

#include "trie/triemap.h"
#include "util/heap.h"
#include "util/millisecond_time.h"

#include <time.h>

#define RTX_LATANCY_NS 50

/***************************
 *   Datastructure Utils
 ***************************/
RTXElementNode* newRTXElementNode(char* key, mstime_t timestamp_ms, int version) {
  RTXElementNode* node = malloc(sizeof(RTXElementNode));
  // TODO: RedisModule_Strndup;
  // TODO: Add key length
  node->key = key;
  node->exp.time = timestamp_ms;
  node->exp.version = version;
  return node;
}

void RTXStore_Free(RTXStore* store) {
  TrieMap_Free(store->element_node_map, NULL);
  heap_free(store->sorted_keys);
  free(store);
}

/*
 * Update expiration, increase the version num, keep the name
 */
void* _trie_node_updater(void* oldval, void* newval) {
  RTXExpiration *n = newval, *o = oldval;
  if (o) {
    n->version = o->version+1;
    free(o);
  }
  
  return newval;  
}

int _cmp_node(const RTXElementNode* node_a, const RTXElementNode* node_b, const void* udata) {
  return node_b->exp.time - node_a->exp.time;
}

/*
 * @return True if node has a valid version
 */
int _is_valid_node(RTXStore* store, RTXElementNode* node) {
  if (node == NULL || node == TRIEMAP_NOTFOUND || node->key == NULL) {
    return 0;
  }
  char* key = node->key;
  TrieMap* t = store->element_node_map;
  RTXExpiration* stored_node = TrieMap_Find(t, key, strlen(key));
  if (stored_node != NULL && stored_node != TRIEMAP_NOTFOUND)
    return (node->exp.version == stored_node->version);
  else
    return 0;
}

/*
 * @return next valid element node, NULL if DS empty
 */
RTXElementNode* _peek_next(RTXStore* store) {
  heap_t* h = store->sorted_keys;
  while (heap_count(h) != 0) {
    RTXElementNode* node = heap_peek(h);
    // printf("peeked and saw:%s\n", node->key);
    if (_is_valid_node(store, node)) {
      return node;
    } else {
      heap_poll(h);
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
int set_element_exp(RTXStore* store, char* key, mstime_t ttl_ms) {
  TrieMap* t = store->element_node_map;
  mstime_t timestamp_ms = current_time_ms() + ttl_ms;
  RTXExpiration* exp = malloc(sizeof(*exp)); 
  exp->time = timestamp_ms;
  exp->version = 0; 
  
  int trie_result = TrieMap_Add(t, key, strlen(key), exp, _trie_node_updater);
  
  // EXP's version was updated by the updater callback
  RTXElementNode *node = newRTXElementNode(key, ttl_ms, exp->version);

  heap_t* h = store->sorted_keys;
  int heap_result = heap_offer(&h, node);
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
  TrieMap* t = store->element_node_map;
  RTXElementNode* element_node = TrieMap_Find(t, key, strlen(key));
  if (element_node != NULL && element_node != TRIEMAP_NOTFOUND) {
    return element_node->exp.time;
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
char* pop_wait(RTXStore* store) {
  RTXElementNode* node = _peek_next(store);
  if (node != NULL) {  // a non empty DS
    node = heap_poll(store->sorted_keys);
    char* key = strdup(node->key);
    del_element_exp(store, node->key);
    return key;
  }
  return NULL;
}

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the key of the element with closest expiration datetime
 */
char* wait_and_pull(RTXStore* store) {
  mstime_t sleep_target_ms = next_at(store);
  mstime_t time_to_wait = sleep_target_ms - current_time_ms();
  if (time_to_wait > 0) {
    struct timespec ttw, rem;
    ttw.tv_sec = 0;
    ttw.tv_nsec = time_to_wait * 1000 - RTX_LATANCY_NS;
    nanosleep(&ttw, &rem);  // TODO: for now we'll assume this is allways fully successfull
  }
  return pull_next(store);
}
