
// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored
#ifndef RTEXP_MODULE_VERSION

#define RTEXP_VERSION_MAJOR 0
#define RTEXP_VERSION_MINOR 1
#define RTEXP_VERSION_PATCH 0

#define RTEXP_MODULE_VERSION \
  (RTEXP_VERSION_MAJOR * 10000 + RTEXP_VERSION_MINOR * 100 + RTEXP_VERSION_PATCH)

#endif
