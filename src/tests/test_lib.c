/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "../librtexp.h"

#include "../util/millisecond_time.h"

#include <time.h>
#include <unistd.h>

// RTXStore* newRTXStore(void);

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

#define SUCCESS 0
#define FAIL 1

int test_set_get_element_exp() {
  mtime_t ttl_ms = 10000;
  mtime_t expected = current_time_ms() + ttl_ms;
  char* key = "test_key_1";
  RTXStore* store = newRTXStore();
  if (set_element_exp(store, key, ttl_ms) == RTXS_ERR) return FAIL;
  mtime_t saved_ms = get_element_exp(store, key);
  if (saved_ms != expected) {
    printf("ERROR: expected %llu but found %llu\n", ttl_ms, saved_ms);
    return FAIL;
  } else
    return SUCCESS;
}

int main(int argc, char* argv[]) {
  mtime_t start_time = current_time_ms();
  int num_of_failed_tests = 0;
  if (test_set_get_element_exp() == FAIL) {
    ++num_of_failed_tests;
    printf("Failed on set-get\n");
  }

  double total_time_ms = current_time_ms() - start_time;
  if (num_of_failed_tests) {
    printf("Failed on %d tests\n", num_of_failed_tests);
    return FAIL;
  } else {

    printf("OK (done in %.2f sec)\n", total_time_ms / 1000);
    return SUCCESS;
  }
}
