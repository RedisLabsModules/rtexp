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
static RedisModuleType *RTXStoreType;

RTXStore *validateStoreKey(RedisModuleCtx *ctx, RedisModuleKey *key) {
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != RTXStoreType) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    RedisModule_CloseKey(key);
    return NULL;
  }
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    RTXStore *rtxStore = newRTXStore();
    RedisModule_ModuleTypeSetValue(key, RTXStoreType, rtxStore);
    return rtxStore;
  } else
    return RedisModule_ModuleTypeGetValue(key);
}

/********************
 *    DS Binding
 ********************/

int insert_with_expiration(RTXStore *store, char *element_key, char *element, mstime_t ttl_ms) {
  // TODO: set redis element here
  return set_element_exp(store, element_key, ttl_ms);
}

int update_ttl(RTXStore *store, char *element_key, mstime_t ttl_ms) {
  return set_element_exp(store, element_key, ttl_ms);
}

int remove_expiration(RTXStore *store, char *element_key) {
  return del_element_exp(store, element_key);
}

mstime_t get_ttl(RTXStore *store, char *element_key) {
  mstime_t timestamp_ms = get_element_exp(store, element_key);
  if (timestamp_ms != -1) {
    mstime_t now = current_time_ms();
    return timestamp_ms - now;
  }
  return -1;
}

/************************
 *    Module Utils
 ************************/

// extract store and element key from the input params of the redis module command
int getStoreAndKey(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, void *store,
                   const void *element_key) {
  if (argc < 2) return REDISMODULE_ERR;

  RedisModuleString *rtxstore_name = argv[1];
  RedisModuleString *element_key_str = argv[2];

  // get key store_name
  RedisModuleKey *store_key =
      RedisModule_OpenKey(ctx, rtxstore_name, REDISMODULE_READ | REDISMODULE_WRITE);
  store = validateStoreKey(ctx, store_key);

  size_t element_key_len;
  element_key = RedisModule_StringPtrLen(element_key_str, &element_key_len);
  return REDISMODULE_OK;
}

/************************
 *    Module Commands
 ************************/

// 1. REXPIRE {key} {ttl_ms}
int ExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  if ((argc != 3) || (getStoreAndKey(ctx, argv, argc, &store, &element_key) != REDISMODULE_OK))
    RedisModule_WrongArity(ctx);

  RedisModuleString *ttl_ms_str = argv[3];
  mstime_t ttl_ms;

  if (RedisModule_StringToLongLong(ttl_ms_str, &ttl_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Time stamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }

  mstime_t stored_ttl = update_ttl(store, element_key, ttl_ms);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

// 2. REXPIREAT {key} {timestamp_ms}
int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  if ((argc != 3) || (getStoreAndKey(ctx, argv, argc, &store, &element_key) != REDISMODULE_OK))
    RedisModule_WrongArity(ctx);

  RedisModuleString *timestamp_ms_str = argv[3];
  mstime_t timestamp_ms;

  if (RedisModule_StringToLongLong(timestamp_ms_str, &timestamp_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Time stamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }
  mstime_t ttl_ms = timestamp_ms - current_time_ms();

  mstime_t stored_ttl = update_ttl(store, element_key, ttl_ms);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

// 3. RTTL {key}
int TTLCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  if (getStoreAndKey(ctx, argv, argc, &store, &element_key) != REDISMODULE_OK)
    RedisModule_WrongArity(ctx);

  mstime_t stored_ttl = get_ttl(store, element_key);
  RedisModule_ReplyWithLongLong(ctx, stored_ttl);
  if (stored_ttl == -1)
    return REDISMODULE_ERR;
  else
    return REDISMODULE_OK;
}

// 4. RUNEXPIRE {key}
int UnexpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  if (getStoreAndKey(ctx, argv, argc, &store, &element_key) != REDISMODULE_OK)
    RedisModule_WrongArity(ctx);

  remove_expiration(store, element_key);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
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
