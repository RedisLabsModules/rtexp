#ifndef RTX_STORE_H
#define RTX_STORE_H

#include "trie/triemap.h"
#include "util/heap.h"
#include "util/millisecond_time.h"

#define RTXS_OK 0
#define RTXS_ERR 1

/***************************
 *        STRUCTS
 ***************************/

typedef struct {
  mstime_t time;
  int version;
} RTXExpiration;

typedef struct rtxs_node {
  char* key;
  size_t len;
  RTXExpiration exp;
} RTXElementNode;

typedef struct rtxs_store {
  heap_t* sorted_keys;        // <key, exp_version, timestamp> (sorted by [exp_timestamp])
  TrieMap* element_node_map;  // [key] -> <exp_version, exp_timestamp>
} RTXStore;

/***************************
 *     CONSTRUCTOR/ DESTRUCTOR
 ***************************/
RTXStore* newRTXStore(void);

void RTXStore_Free(RTXStore* store);

/************************************
 *   General DS handling functions
 ************************************/

/*
 * @return the number of uniqe expiration entries in the store
 */
size_t expiration_count(RTXStore* store);

/*
 * Insert expiration for a new key or update an existing one
 * @return RTXS_OK on success, RTXS_ERR on error
 */
int set_element_exp(RTXStore* store, char* key, size_t len, mstime_t ttl_ms);

/*
 * Get the expiration value for the given key
 * @return datetime of expiration (in milliseconds) on success, -1 on error
 */
mstime_t get_element_exp(RTXStore* store, char* key);

/*
 * Remove expiration from the data store for the given key
 * @return RTXS_OK
 */
int del_element_exp(RTXStore* store, char* key);

/*
 * @return the closest element expiration datetime (in milliseconds), or -1 if DS is empty
 */
mstime_t next_at(RTXStore* store);

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
RTXElementNode* pop_next(RTXStore* store);

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the node of the element with closest expiration datetime
 */
RTXElementNode* pop_wait(RTXStore* store);

/*
 * Gracefully free nodes
 */
void freeRTXElementNode(RTXElementNode* node);
#endif
