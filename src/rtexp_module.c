#include "librtexp.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "util/millisecond_time.h"

/********************
 *    C Utils
 ********************/

char *string_append(char *a, const char *b) {
  char *retstr = RedisModule_Alloc(strlen(a) + strlen(b));
  strcpy(retstr, a);
  strcat(retstr, b);
  // printf("printing: %s", retstr);
  RedisModule_Free(a);
  return retstr;
}

/********************
 *    Redis Type
 ********************/

/********************
 *    DS Binding
 ********************/

int insert_with_expiration(RTXStore *store, char *object_key, char *element, mtime_t ttl_ms) {
  // TODO: set redis element here
  return set_element_exp(store, object_key, ttl_ms);
}

int update_ttl(RTXStore *store, char *object_key, mtime_t ttl_ms) {
  return set_element_exp(store, object_key, ttl_ms);
}

int remove_expiration(RTXStore *store, char *object_key) {
  return del_element_exp(store, object_key);
}

mtime_t get_ttl(RTXStore *store, char *object_key) {
  mtime_t timestamp_ms = get_element_exp(store, object_key);
  if (timestamp_ms != -1) {
    mtime_t now = current_time_ms();
    return timestamp_ms - now;
  }
  return -1;
}

/************************
 *    Module Commands
 ************************/

// 1. REXPIRE {key} {milliseconds}
int ExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

// 2. REXPIREAT {key} {timestamp_ms}
int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

// 3. RTTL {key}
int TTLCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

// 4. RUNEXPIRE {key}
int UnexpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

// 5. RSETEX {key} {value} {ttl}
int SetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

// 6. REXECEX {key} {ttl} {....} - execute command, save result to key, and set expiration
int ExecuteAndExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  // Register the module itself
  if (RedisModule_Init(ctx, "REALEX", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // register commands - using the shortened utility registration macro
  RMUtil_RegisterReadCmd(ctx, "REXPIRE", ExpireCommand);
  RMUtil_RegisterReadCmd(ctx, "REXPIREAT", ExpireAtCommand);
  RMUtil_RegisterReadCmd(ctx, "RTTL", TTLCommand);
  RMUtil_RegisterReadCmd(ctx, "RUNEXPIRE", UnexpireCommand);
  RMUtil_RegisterReadCmd(ctx, "RSETEX", SetexCommand);
  RMUtil_RegisterReadCmd(ctx, "REXECEX", ExecuteAndExpireCommand);

  return REDISMODULE_OK;
}
