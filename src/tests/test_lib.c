/* This is a stand-alone implementation of a real-time expiration data store.
 * It is basically a min heap <key, exp_version> sorted by expiration,
 * with a map of [key] -> <exp_version, exp> on the side
 */
#include "../lawn.h"

#include "../util/millisecond_time.h"

#include <time.h>
#include <unistd.h>

#define SUCCESS 0
#define FAIL 1

// Lawn* newLawn(void);
// void freeLawn(Lawn* store);
int constructor_distructore_test() {
  Lawn* store = newLawn();
  freeLawn(store);
  return SUCCESS;
}

/*
 * Insert expiration for a new key or update an existing one
 * @return RTXS_OK on success, LAWN_ERR on error
 */
// int set_element_ttl(Lawn* store, char* key, mstime_t ttl_ms);
int test_set_element_ttl() {
  int retval = FAIL;
  mstime_t ttl_ms = 10000;
  mstime_t expected = current_time_ms() + ttl_ms;
  char* key = "set_get_test_key";
  Lawn* store = newLawn();
  if (set_element_ttl(store, key, strlen(key), ttl_ms) == LAWN_ERR) return FAIL;
  retval = SUCCESS;

  freeLawn(store);
  return retval;
}


/*
 * Get the expiration value for the given key
 * @return datetime of expiration (in milliseconds) on success, -1 on error
 */
// mstime_t get_element_exp(Lawn* store, char* key);
int test_set_get_element_exp() {
  int retval = FAIL;
  mstime_t ttl_ms = 10000;
  mstime_t expected = current_time_ms() + ttl_ms;
  char* key = "set_get_test_key";
  Lawn* store = newLawn();
  if (set_element_ttl(store, key, strlen(key), ttl_ms) == LAWN_ERR) return FAIL;
  mstime_t saved_ms = get_element_exp(store, key);
  if (saved_ms != expected) {
    printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
    retval = FAIL;
  } else
    retval = SUCCESS;

  freeLawn(store);
  return retval;
}

/*
 * Remove expiration from the data store for the given key
 * @return RTXS_OK
 */
// int del_element_exp(Lawn* store, char* key);
int test_del_element_exp() {
  int retval = FAIL;
  mstime_t ttl_ms = 10000;
  mstime_t expected = -1;
  char* key = "del_test_key";
  Lawn* store = newLawn();
  if (set_element_ttl(store, key, strlen(key), ttl_ms) == LAWN_ERR) return FAIL;
  if (del_element_exp(store, key) == LAWN_ERR) return FAIL;
  mstime_t saved_ms = get_element_exp(store, key);
  if (saved_ms != expected) {
    printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
    retval = FAIL;
  } else
    retval = SUCCESS;

  freeLawn(store);
  return retval;
}

/*
 * @return the closest element expiration datetime (in milliseconds), or -1 if DS is empty
 */
// mstime_t next_at(Lawn* store);
int test_next_at() {
  int retval = FAIL;
  Lawn* store = newLawn();

  mstime_t ttl_ms1 = 10000;
  char* key1 = "next_at_test_key_1";

  mstime_t ttl_ms2 = 2000;
  char* key2 = "next_at_test_key_2";

  mstime_t ttl_ms3 = 3000;
  char* key3 = "next_at_test_key_3";

  mstime_t ttl_ms4 = 400000;
  char* key4 = "next_at_test_key_4";

  if ((set_element_ttl(store, key1, strlen(key1), ttl_ms1) != LAWN_ERR) &&
      (set_element_ttl(store, key2, strlen(key2), ttl_ms2) != LAWN_ERR) &&
      (set_element_ttl(store, key3, strlen(key3), ttl_ms3) != LAWN_ERR) &&
      (del_element_exp(store, key2) != LAWN_ERR) &&
      (set_element_ttl(store, key4, strlen(key4), ttl_ms4) != LAWN_ERR)) {

    mstime_t expected = current_time_ms() + ttl_ms3;
    mstime_t saved_ms = next_at(store);
    if (saved_ms != expected) {
      printf("ERROR: expected %llu but found %llu\n", expected, saved_ms);
      retval = FAIL;
    } else
      retval = SUCCESS;
  }

  freeLawn(store);
  return retval;
}

/*
 * Remove the element with the closest expiration datetime from the data store and return it's key
 * @return the key of the element with closest expiration datetime
 */
// char* pop_next(Lawn* store);
int test_pop_next() {
  int retval = FAIL;
  Lawn* store = newLawn();

  mstime_t ttl_ms1 = 10000;
  char* key1 = "pop_next_test_key_1";

  mstime_t ttl_ms2 = 2000;
  char* key2 = "pop_next_test_key_2";

  mstime_t ttl_ms3 = 3000;
  char* key3 = "pop_next_test_key_3";

  if ((set_element_ttl(store, key1, strlen(key1), ttl_ms1) != LAWN_ERR) &&
      (set_element_ttl(store, key2, strlen(key2), ttl_ms2) != LAWN_ERR) &&
      (del_element_exp(store, key2) != LAWN_ERR) &&
      (set_element_ttl(store, key3, strlen(key3), ttl_ms3) != LAWN_ERR)) {

    char* expected = key3;
    ElementQueueNode* actual_node = pop_next(store);
    char* actual = actual_node->element; 
    if (strcmp(expected, actual)) {
      printf("ERROR: expected key \'%s\' but found \'%s\'\n", expected, actual);
      retval = FAIL;
    } else {
      // make sure we actually delete the thing
      mstime_t expected_ms = -1;
      mstime_t saved_ms = get_element_exp(store, expected);
      if (expected_ms != saved_ms) {
        printf("ERROR: expected ttl %llu but found %llu\n", expected_ms, saved_ms);
        retval = FAIL;
      } else
        retval = SUCCESS;
    }
    freeNode(actual_node);
  }
  freeLawn(store);
  return retval;
}


int main(int argc, char* argv[]) {
  mstime_t start_time = current_time_ms();
  int num_of_failed_tests = 0;
  int num_of_passed_tests = 0;

  if (constructor_distructore_test() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on constructor-distructore\n");
  } else {
    printf("PASSED constructor-distructore test\n");
    ++num_of_passed_tests;
  }

  if (test_set_element_ttl() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on set\n");
  } else {
    printf("PASSED set test\n");
    ++num_of_passed_tests;
  }

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

  if (test_pop_next() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on pop_next\n");
  } else {
    printf("PASSED pop_next test\n");
    ++num_of_passed_tests;
  }

  if (test_next_at() == FAIL) {
    ++num_of_failed_tests;
    printf("FAILED on next_at\n");
  } else {
    printf("PASSED next_at test\n");
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
