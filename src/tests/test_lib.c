/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "../librtexp.h"

#include "../util/millisecond_time.h"

#include <time.h>
#include <unistd.h>

#define SUCCESS 0
#define FAIL 1

// RTXStore* newRTXStore(void);
// void RTXStore_Free(RTXStore* store);
int constructor_distructore_test() {
  RTXStore* store = newRTXStore();
  RTXStore_Free(store);
  return SUCCESS;
}

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
int test_set_get_element_exp() {
  int retval = FAIL;
  mtime_t ttl_ms = 10000;
  mtime_t expected = current_time_ms() + ttl_ms;
  char* key = "set_get_test_key";
  RTXStore* store = newRTXStore();
  if (set_element_exp(store, key, ttl_ms) == RTXS_ERR) return FAIL;
  mtime_t saved_ms = get_element_exp(store, key);
  if (saved_ms != expected) {
    printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
    retval = FAIL;
  } else
    retval = SUCCESS;

  RTXStore_Free(store);
  return retval;
}

/*
 * Remove expiration from the data store for the given key
 * @return RTXS_OK
 */
// int del_element_exp(RTXStore* store, char* key);
int test_del_element_exp() {
  int retval = FAIL;
  mtime_t ttl_ms = 10000;
  mtime_t expected = -1;
  char* key = "del_test_key";
  RTXStore* store = newRTXStore();
  if (set_element_exp(store, key, ttl_ms) == RTXS_ERR) return FAIL;
  if (del_element_exp(store, key) == RTXS_ERR) return FAIL;
  mtime_t saved_ms = get_element_exp(store, key);
  if (saved_ms != expected) {
    printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
    retval = FAIL;
  } else
    retval = SUCCESS;

  RTXStore_Free(store);
  return retval;
}

/*
 * @return the closest element expiration datetime (in milliseconds), or -1 if DS is empty
 */
// mtime_t next_at(RTXStore* store);
int test_next_at() {
  int retval = FAIL;
  RTXStore* store = newRTXStore();

  mtime_t ttl_ms1 = 10000;
  char* key1 = "next_at_test_key_1";

  mtime_t ttl_ms2 = 2000;
  char* key2 = "next_at_test_key_2";

  mtime_t ttl_ms3 = 3000;
  char* key3 = "next_at_test_key_3";

  mtime_t ttl_ms4 = 400000;
  char* key4 = "next_at_test_key_4";

  if ((set_element_exp(store, key1, ttl_ms1) != RTXS_ERR) &&
      (set_element_exp(store, key2, ttl_ms2) != RTXS_ERR) &&
      (set_element_exp(store, key3, ttl_ms3) != RTXS_ERR) &&
      (set_element_exp(store, key4, ttl_ms4) != RTXS_ERR) &&
      (del_element_exp(store, key2) != RTXS_ERR)) {

    mtime_t expected = current_time_ms() + ttl_ms3;
    mtime_t saved_ms = next_at(store);
    if (saved_ms != expected) {
      printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
      retval = FAIL;
    } else
      retval = SUCCESS;
  }

  RTXStore_Free(store);
  return retval;
}

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
// char* pull_next(RTXStore* store);
int test_pull_next() {
  int retval = FAIL;
  RTXStore* store = newRTXStore();

  mtime_t ttl_ms1 = 10000;
  char* key1 = "pull_next_test_key_1";

  mtime_t ttl_ms2 = 2000;
  char* key2 = "pull_next_test_key_2";

  mtime_t ttl_ms3 = 3000;
  char* key3 = "pull_next_test_key_3";

  if ((set_element_exp(store, key1, ttl_ms1) != RTXS_ERR) &&
      (set_element_exp(store, key2, ttl_ms2) != RTXS_ERR) &&
      (del_element_exp(store, key2) != RTXS_ERR) &&
      (set_element_exp(store, key3, ttl_ms3) != RTXS_ERR)) {

    char* expected = key3;
    char* actual = pull_next(store);
    if (strcmp(expected, actual)) {
      printf("ERROR: expected \'%s\' but found \'%s\'\n", expected, actual);
      retval = FAIL;
    } else {
      // make sure we actually delete the thing
      mtime_t expected_ms = -1;
      mtime_t saved_ms = get_element_exp(store, expected);
      if (expected_ms != saved_ms) {
        printf("ERROR: expected %llu but found %llu\n", expected_ms, saved_ms);
        retval = FAIL;
      } else
        retval = SUCCESS;
    }
  }

  RTXStore_Free(store);
  return retval;
}

/*
 * Wait Remove the element with the closest expiration datetime from the data store and return it's
 * key
 * @return the key of the element with closest expiration datetime
 */
// char* wait_and_pull(RTXStore* store);
int test_wait_and_pull() {
  int retval = FAIL;
  RTXStore* store = newRTXStore();

  mtime_t ttl_ms1 = 10000;
  char* key1 = "pull_next_test_key_1";

  mtime_t ttl_ms2 = 2000;
  char* key2 = "pull_next_test_key_2";

  mtime_t ttl_ms3 = 3000;
  char* key3 = "pull_next_test_key_3";

  if ((set_element_exp(store, key1, ttl_ms1) != RTXS_ERR) &&
      (set_element_exp(store, key2, ttl_ms2) != RTXS_ERR) &&
      (del_element_exp(store, key2) != RTXS_ERR) &&
      (set_element_exp(store, key3, ttl_ms3) != RTXS_ERR)) {

    mtime_t expected_ms = ttl_ms3;
    char* expected_key = key3;
    mtime_t start_time = current_time_ms();
    char* pulled_key = wait_and_pull(store);
    mtime_t actual_ms = current_time_ms() - start_time;
    if (expected_ms == actual_ms) {
      printf("ERROR: expected %llu but found %llu\n", expected_ms, actual_ms);
      retval = FAIL;
    } else {
      // make sure we actually delete the thing
      mtime_t expected_ms = -1;
      mtime_t saved_ms = get_element_exp(store, expected_key);
      if (expected_ms != saved_ms) {
        printf("ERROR: expected %llu but found %llu\n", expected_ms, saved_ms);
        retval = FAIL;
      } else
        retval = SUCCESS;
    }
  }

  RTXStore_Free(store);
  return retval;
}

int main(int argc, char* argv[]) {
  mtime_t start_time = current_time_ms();
  int num_of_failed_tests = 0;
  int num_of_passed_tests = 0;

  if (test_set_get_element_exp() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on set-get\n");
  } else {
    printf("PASSED set-get test\n");
    ++num_of_passed_tests;
  }

  if (test_del_element_exp() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on del\n");
  } else {
    printf("PASSED del test\n");
    ++num_of_passed_tests;
  }

  if (test_pull_next() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on pull_next\n");
  } else {
    printf("PASSED pull_next test\n");
    ++num_of_passed_tests;
  }

  if (test_next_at() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on next_at\n");
  } else {
    printf("PASSED next_at test\n");
    ++num_of_passed_tests;
  }

  if (test_wait_and_pull() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on wait_and_pull\n");
  } else {
    printf("PASSED wait_and_pull\n");
    ++num_of_passed_tests;
  }

  double total_time_ms = current_time_ms() - start_time;
  printf("\n-------------\n");
  if (num_of_failed_tests) {
    printf("Failed (%d tests failed and %d passed in %.2f sec)\n", num_of_failed_tests,
           num_of_passed_tests, total_time_ms / 1000);
    return FAIL;
  } else {
    printf("OK (%d tests passed in %.2f sec)\n", num_of_passed_tests, total_time_ms / 1000);
    return SUCCESS;
  }
}
