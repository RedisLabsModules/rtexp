/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "rtexp_store.h"

#include "trie/triemap.h"
#include "util/heap.h"
#include "util/millisecond_time.h"

#include <time.h>

#define RTX_LATANCY_NS 50

/***************************
 *   Datastructure Utils
 ***************************/

/*
 * Update expiration, increase the version num, keep the name
 */
void* _trie_node_updater(RTXElementNode* oldval, RTXElementNode* newval) {
  return newRTXElementNode(oldval->key, newval->expiration, ++(oldval->version));
}

/*
 * <0 node_a goes before node_b
 * 0  node_a is equivalent to node_b
 * >0 node_a goes after node_b
 */
int _cmp_node(const RTXElementNode* node_a, const RTXElementNode* node_b, const void* udata) {
  return node_a->expiration - node_b->expiration;
}

/*
 * @return True if node has a valid version
 */
int _is_valid_node(RTXStore* store, RTXElementNode* node) {
  if (node == NULL) {
    return 0;
  }
  TrieMap* t = store->element_node_map;
  char* key = node->key;
  RTXElementNode* stored_node = TrieMap_Find(t, key, strlen(key));
  return (node->version == stored_node->version);
}

/*
 * @return next valid element node, NULL if DS empty
 */
RTXElementNode* _peek_next(RTXStore* store) {
  heap_t* h = store->sorted_keys;
  while (heap_count(h) != 0) {
    RTXElementNode* node = heap_peek(h);
    if (_is_valid_node(store, node)) {
      return node;
    } else {
      heap_poll(h);
    }
  }
  return NULL;
}

/***************************
 *     CONSTRUCTORS
 ***************************/
RTXElementNode* newRTXElementNode(char* key, mtime_t timestamp_ms, int version) {
  RTXElementNode node = {.key = key, .expiration = timestamp_ms, .version = version};
  return &node;
}

RTXStore* newRTXStore(void) {
  RTXStore store = {.sorted_keys = heap_new(_cmp_node, NULL), .element_node_map = NewTrieMap()};
  return &store;
}

/************************************
 *   General DS handling functions
 ************************************/

/*
 * Insert expiration for a new key or update an existing one
 * @return RTXS_OK on success, RTXS_ERR on error
 */
int set_element_exp(RTXStore* store, char* key, mtime_t ttl_ms) {
  TrieMap* t = store->element_node_map;
  mtime_t timestamp_ms = current_time_ms() + ttl_ms;
  RTXElementNode* new_node = newRTXElementNode(key, timestamp_ms, 0);
  int trie_result = TrieMap_Add(t, key, strlen(key), new_node, _trie_node_updater);

  if (trie_result == 0) {  // we replaced an existing element
    RTXElementNode* stored_node = TrieMap_Find(t, key, strlen(key));
    new_node->version = stored_node->version;
  }

  heap_t* h = store->sorted_keys;
  int heap_result = heap_offer(&h, new_node);
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
mtime_t get_element_exp(RTXStore* store, char* key) {
  TrieMap* t = store->element_node_map;
  RTXElementNode* element_node = TrieMap_Find(t, key, strlen(key));
  if (element_node != NULL && element_node != TRIEMAP_NOTFOUND) {
    return element_node->expiration;
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
mtime_t next_at(RTXStore* store) {
  RTXElementNode* node = _peek_next(store);
  if (node == NULL) {  // empty_DS
    return -1;
  } else {
    return node->expiration;
  }
}

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
// TODO: there might be a deletion issue here (where is the key string stored?)
char* pull_next(RTXStore* store) {
  RTXElementNode* node = _peek_next(store);
  if (node != NULL) {  // a non empty DS
    node = heap_poll(store->sorted_keys);
    del_element_exp(store, node->key);
    return node->key;
  }
  return NULL;
}

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the key of the element with closest expiration datetime
 */
char* wait_and_pull(RTXStore* store) {
  mtime_t sleep_target_ms = next_at(store);
  mtime_t time_to_wait = sleep_target_ms - current_time_ms();
  if (time_to_wait > 0) {
    struct timespec ttw, rem;
    ttw.tv_sec = 0;
    ttw.tv_nsec = time_to_wait * 1000 - RTX_LATANCY_NS;
    nanosleep(&ttw, &rem);  // TODO: for now we'll assume this is allways fully successfull
  }
  return pull_next(store);
}
