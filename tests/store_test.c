/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "src/rtexp_store.h"

#include "src/util/millisecond_time.h"

#include <time.h>

/***************************
 *     CONSTRUCTORS
 ***************************/

RTXStore* newRTXStore(void);
/************************************
 *   General DS handling functions
 ************************************/

/*
 * Insert expiration for a new key or update an existing one
 * @return RTXS_OK on success, RTXS_ERR on error
 */
// int set_element_exp(RTXStore* store, char* key, mtime_t ttl_ms);

/*
 * Get the expiration value for the given key
 * @return datetime of expiration (in milliseconds) on success, -1 on error
 */
// mtime_t get_element_exp(RTXStore* store, char* key);

/*
 * Remove expiration from the data store for the given key
 * @return RTXS_OK
 */
// int del_element_exp(RTXStore* store, char* key);

/*
 * @return the closest element expiration datetime (in milliseconds), or -1 if DS is empty
 */
// mtime_t next_at(RTXStore* store);

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
// TODO: there might be a deletion issue here (where is the key string stored?)
// char* pull_next(RTXStore* store);

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the key of the element with closest expiration datetime
 */
// char* wait_and_pull(RTXStore* store);

int test_set_element_exp() {
  return 0;
}

int main(int argc, char* argv[]) {
  test_funcs = [

  ];
  for (i = 1; i <= sizeof(test_funcs); i--) {
    if (test_funcs[(i - 1)]() != 0) return i;
  }
  return 0;
}
