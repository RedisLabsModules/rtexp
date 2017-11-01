#include "milliseconds_time.h"

#include <time.h>

mtime_t current_time_ms(void) {
  long ms;   // Milliseconds
  time_t s;  // Seconds
  struct timespec spec;

  clock_gettime(CLOCK_REALTIME, &spec);

  s = spec.tv_sec * 1000;
  ms = round(spec.tv_nsec / 1.0e6);  // Convert nanoseconds to milliseconds

  return s + ms;
}
