#include "librtexp.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/periodic.h"
#include "util/millisecond_time.h"

#define REDIS_MODULE_TARGET
#include "util/rmalloc.h"

#define RTEXP_BUFFER_MS 5
#define RTEXP_MIN_INTERVAL_NS 250 // =0.25 microsecond (10^-6) scale. existing Expire is on milliseconds (10^-3) scale
#define RTEXP_MAX_INTERVAL_NS 10000000 // = 10 millisecond (0.01 second) scale

static RTXStore *rtxStore;
static struct RMUtilTimer *interval_timer;

typedef long long nstime_t;
/************************
 *    Module Utils
 ************************/

int redisSetPExpiration(RedisModuleCtx *ctx, RedisModuleString *key_str, mstime_t ttl_ms) {
  int res; 
  if (ttl_ms){ 
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "PEXPIRE", "sl", key_str, ttl_ms + RTEXP_BUFFER_MS); 
    long long rep_int = RedisModule_CallReplyInteger(rep);
    res = (rep_int == 0) ? REDISMODULE_ERR: REDISMODULE_OK;
    RedisModule_FreeCallReply(rep);
  } else {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, key_str, REDISMODULE_READ | REDISMODULE_WRITE);
    res = RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    RedisModule_CloseKey(key);
  }
  return res;
}

nstime_t to_ns(mstime_t ms) {
  return ms * 1000000;
}

void setNextTimerInterval(mstime_t interval_ms){
  nstime_t interval_ns = to_ns(interval_ms);
  nstime_t new_interval_ns = (interval_ns < RTEXP_MAX_INTERVAL_NS)? interval_ms : RTEXP_MAX_INTERVAL_NS;
  new_interval_ns = new_interval_ns / 2; // Safety buffer - asimptotically get closet to the timeout
  
  RMUtilTimer_SetInterval(interval_timer, 
    (struct timespec){
    .tv_sec = 0,
    .tv_nsec = new_interval_ns});
}

void timerCb(RedisModuleCtx *ctx, void *p) {
  RedisModule_ThreadSafeContextLock(ctx);

  mstime_t now = rm_current_time_ms();
  nstime_t now_ns = to_ns(now);

  mstime_t next = next_at(rtxStore);
  while (next > 0 && to_ns(next) < (now_ns+RTEXP_MIN_INTERVAL_NS)) {
    RTXElementNode* node = pop_next(rtxStore);    
    if (node != NULL) {
      RedisModule_Call(ctx, "UNLINK", "c", node->key);
      freeRTXElementNode(node);
    }
    next = next_at(rtxStore);
  }
  if (next < 0)
    setNextTimerInterval(RTEXP_MAX_INTERVAL_NS);
  else
    setNextTimerInterval(next);
  RedisModule_ThreadSafeContextUnlock(ctx);
}

/********************
 *    DS Binding
 ********************/

int set_ttl(RTXStore *store, char *element_key, size_t len, mstime_t ttl_ms) {
  setNextTimerInterval(ttl_ms);
  return set_element_exp(store, element_key, len, ttl_ms);
}

int remove_expiration(RTXStore *store, char *element_key) {
  return del_element_exp(store, element_key);
}

mstime_t get_ttl(RTXStore *store, char *element_key) {
  mstime_t timestamp_ms = get_element_exp(store, element_key);
  if (timestamp_ms != -1) {
    mstime_t now = rm_current_time_ms();
    return timestamp_ms - now;
  }
  return -2; // to conform with redis' PTTL
}

/************************
 *    Module Commands
 ************************/

// 1. REXPIRE {key} {ttl_ms}
int ExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3)
    RedisModule_WrongArity(ctx);

  if (!rtxStore) {
    RedisModule_ReplyWithError(ctx, "Store was not initialized");
    return REDISMODULE_ERR;
  }

  size_t element_key_len;
  const char * element_key = RedisModule_StringPtrLen(argv[1], &element_key_len);

  RedisModuleString *ttl_ms_str = argv[2];
  mstime_t ttl_ms;
  if (RedisModule_StringToLongLong(ttl_ms_str, &ttl_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Timestamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }

  if (redisSetPExpiration(ctx, argv[1], ttl_ms) == REDISMODULE_ERR){
    RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_ERR;
  }

  // THE ACTUAL EXPIRATION 
  if (set_ttl(rtxStore, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_ERR;
  }
}

// 2. REXPIREAT {key} {timestamp_ms}
int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3)
    RedisModule_WrongArity(ctx);

  if (!rtxStore) {
    RedisModule_ReplyWithError(ctx, "Store was not initialized");
    return REDISMODULE_ERR;
  }

  size_t element_key_len;
  const char * element_key = RedisModule_StringPtrLen(argv[1], &element_key_len);

  RedisModuleString *timestamp_ms_str = argv[2];
  mstime_t timestamp_ms;

  if (RedisModule_StringToLongLong(timestamp_ms_str, &timestamp_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Timestamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }
  mstime_t ttl_ms = timestamp_ms - rm_current_time_ms();
  if (ttl_ms <= 0){
    RedisModule_ReplyWithError(ctx, "Expiration time must be in the future");
    return REDISMODULE_ERR;
  }

  if (redisSetPExpiration(ctx, argv[1], ttl_ms) == REDISMODULE_ERR){
    RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_ERR;
  }

  if (set_ttl(rtxStore, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithError(ctx, "Unexpeted Error Occured");
    return REDISMODULE_ERR;
  }
}

// 3. RTTL {key}
int TTLCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2)
    RedisModule_WrongArity(ctx);
  
  if (!rtxStore) {
    RedisModule_ReplyWithError(ctx, "Store was not initialized");
    return REDISMODULE_ERR;
  }

  size_t element_key_len;
  const char * element_key = RedisModule_StringPtrLen(argv[1], &element_key_len);

  mstime_t stored_ttl = get_ttl(rtxStore, element_key);
  RedisModule_ReplyWithLongLong(ctx, stored_ttl);
  if (stored_ttl == -1)
    return REDISMODULE_ERR;
  else
    return REDISMODULE_OK;
}

// 4. RUNEXPIRE {key}
int UnexpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2)
    RedisModule_WrongArity(ctx);
  
  if (!rtxStore) {
    RedisModule_ReplyWithError(ctx, "Store was not initialized");
    return REDISMODULE_ERR;
  }

  size_t element_key_len;
  const char * element_key = RedisModule_StringPtrLen(argv[1], &element_key_len);

  if (redisSetPExpiration(ctx, argv[1], NULL) == REDISMODULE_ERR){
    RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_ERR;
  }

  remove_expiration(rtxStore, element_key);
  RedisModule_ReplyWithLongLong(ctx, 1);
  return REDISMODULE_OK;
}

// 5. RSETEX {key} {value} {ttl}
int SetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4)
    RedisModule_WrongArity(ctx);
  
  if (!rtxStore) {
    RedisModule_ReplyWithError(ctx, "Store was not initialized");
    return REDISMODULE_ERR;
  }

  size_t element_key_len;
  const char * element_key = RedisModule_StringPtrLen(argv[1], &element_key_len);

  // RedisModuleString *element = argv[2];
  RedisModuleString *ttl_ms_str = argv[3];
  mstime_t ttl_ms;

  if (RedisModule_StringToLongLong(ttl_ms_str, &ttl_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Timestamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }  
  if (ttl_ms <= 0) {
    RedisModule_ReplyWithError(ctx, "Expiration time must be in the future");
    return REDISMODULE_ERR;
  }
  
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModule_StringSet(key, argv[2]);
  RedisModule_CloseKey(key);
  
  if (redisSetPExpiration(ctx, argv[1], ttl_ms) == REDISMODULE_ERR){
    RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_ERR;
  }

  if (set_ttl(rtxStore, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithError(ctx, "Unexpeted Error Occured");
    return REDISMODULE_ERR;
  }
}

// 6. REXECEX {key} {ttl} {....} - execute command, save result to key, and set expiration
int ExecuteAndExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) RedisModule_WrongArity(ctx);
  //
  // RedisModuleString *rtxstore_name = argv[1];
  // RedisModuleString *element_key_str = argv[2];
  // RedisModuleString *ttl_ms_str = argv[3];
  //
  // RedisModuleString *cmdname_str = argv[4];
  // size_t cmdname_len;
  // const char *cmdname = RedisModule_StringPtrLen(cmdname_str, &cmdname_len);
  //
  // RedisModuleCallReply *call_reply = RedisModule_Call(
  //     ctx, cmdname, const char *fmt,
  //     ...);  // TODO: how do I handle all these defferent formats for different commands here?
  // RedisModuleString *value = RedisModule_CreateStringFromCallReply();
  // RedisModuleString **outv = [ rtxstore_name, element_key_str, value, ttl_ms_str ];
  // return SetexCommand(ctx, outv, 4);
  return REDISMODULE_OK;
}

int CreateRTEXP() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  
  rtxStore = newRTXStore();
  interval_timer = RMUtil_NewPeriodicTimer( 
      timerCb, NULL, &rtxStore,
      (struct timespec){
          .tv_sec = 0,
          .tv_nsec = RTEXP_MIN_INTERVAL_NS});  

  return REDISMODULE_OK;
}

// Init Module
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  // Register the module itself
  if (RedisModule_Init(ctx, "RTEXP", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  RedisModule_AutoMemory(ctx);

  // Init internals
  CreateRTEXP();

  // register commands - using the shortened utility registration macro
  RMUtil_RegisterWriteCmd(ctx, "REXPIRE", ExpireCommand);
  RMUtil_RegisterWriteCmd(ctx, "REXPIREAT", ExpireAtCommand);
  RMUtil_RegisterWriteCmd(ctx, "RTTL", TTLCommand);
  RMUtil_RegisterWriteCmd(ctx, "RUNEXPIRE", UnexpireCommand);
  RMUtil_RegisterWriteCmd(ctx, "RSETEX", SetexCommand);
  // RMUtil_RegisterWriteCmd(ctx, "REXECEX", ExecuteAndExpireCommand);

  return REDISMODULE_OK;
}
