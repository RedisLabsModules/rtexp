#include "librtexp.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/periodic.h"
#include "util/millisecond_time.h"

#define REDIS_MODULE_TARGET
#include "util/rmalloc.h"

#define RTEXP_BUFFER_MS 5

static RTXStore *rtxStore;

/********************
 *    Redis Type
 ********************/
static RedisModuleType *RTXStoreType;

// RTXStore *validateStoreKey(RedisModuleCtx *ctx, RedisModuleKey *key) {
//   int type = RedisModule_KeyType(key);
//   if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != RTXStoreType) {
//     RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
//     RedisModule_CloseKey(key);
//     return NULL;
//   }
//   if (type == REDISMODULE_KEYTYPE_EMPTY) {
//     RTXStore *rtxStore = newRTXStore();
//     RedisModule_ModuleTypeSetValue(key, RTXStoreType, rtxStore);
//     return rtxStore;
//   } else
//     return RedisModule_ModuleTypeGetValue(key);
// }

// RTXStore *getStore(RedisModuleCtx *ctx, RedisModuleString *rtxstore_name) {
  

//   if (rtxstore_name){
//     merge_stores(rtxstore_name)
//   }
//   else if (RTEXP_STORE_NAME_GLOBAL == NULL) {
//       // TODO: generate random name
//   }
  
//   RedisModuleKey *store_key = RedisModule_OpenKey(ctx, rtxstore_name, REDISMODULE_READ | REDISMODULE_WRITE);
//   int type = RedisModule_KeyType(key);
//   if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != RTXStoreType) {
//     RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
//     RedisModule_CloseKey(key);
//     return NULL;
//   }
//   if (type == REDISMODULE_KEYTYPE_EMPTY) {
//     RTXStore *rtxStore = newRTXStore();
//     RedisModule_ModuleTypeSetValue(key, RTXStoreType, rtxStore);
//     return rtxStore;
//   } else
//     return RedisModule_ModuleTypeGetValue(key);
// }

// RTEXP_STORE_NAME_GLOBAL
// void RTXStoreTypeFree(void *store)
// {
//   RTXStore_Free(store);
// }

// void RTXStoreTypeRdbSave(RedisModuleIO *rdb, void *value)
// {
//     Dehydrator *dehy = value;
//     RedisModule_SaveString(rdb, dehy->name);
//     RedisModule_SaveUnsigned(rdb, kh_size(dehy->timeout_queues));
//     // for each timeout_queue in timeout_queues
//     khiter_t k;
//     for (k = kh_begin(dehy->timeout_queues); k != kh_end(dehy->timeout_queues); ++k)
//     {
//         if (!kh_exist(dehy->timeout_queues, k)) continue;
//         ElementList* list = kh_value(dehy->timeout_queues, k);
//         int ttl = kh_key(dehy->timeout_queues, k);
//         RedisModule_SaveUnsigned(rdb, ttl);
//         RedisModule_SaveUnsigned(rdb, list->len);
//         ElementListNode* node = list->head;
//         int done_with_queue = 0;
//         while (!done_with_queue)
//         {
//             if ((node != NULL))
//             {
//                 RedisModule_SaveUnsigned(rdb, node->expiration);
//                 RedisModule_SaveString(rdb, node->element_id);
//                 RedisModule_SaveString(rdb, node->element);
//                 node = node->next;
//             }
//             else
//             {
//                 done_with_queue = 1;
//             }
//         }
//     }
// }

// void *RTXStoreTypeRdbLoad(RedisModuleIO *rdb, int encver)
// {
//     if (encver != 0) { return NULL; }
//     khiter_t k;
//     RedisModuleString* name = RedisModule_LoadString(rdb);
//     Dehydrator *dehy = _createDehydrator(name);
//     //create an ElementListNode
//     uint64_t queue_num = RedisModule_LoadUnsigned(rdb);
//     while(queue_num--)
//     {
//         ElementList* timeout_queue = _createNewList();
//         uint64_t ttl = RedisModule_LoadUnsigned(rdb);

//         uint64_t node_num = RedisModule_LoadUnsigned(rdb);
//         while(node_num--)
//         {
//             uint64_t expiration = RedisModule_LoadUnsigned(rdb);
//             RedisModuleString* element_id = RedisModule_LoadString(rdb);
//             RedisModuleString* element = RedisModule_LoadString(rdb);

//             ElementListNode* node  = _createNewNode(element, element_id, ttl, expiration);
//             _listPush(timeout_queue, node);

//             // mark element dehytion location in element_nodes
//             int retval;
//             k = kh_put(32, dehy->element_nodes, RedisModule_StringPtrLen(element_id, NULL), &retval);
//             kh_value(dehy->element_nodes, k) = node;
//         }

//         int retval;
//         k = kh_put(16, dehy->timeout_queues, ttl, &retval);
//         kh_value(dehy->timeout_queues, k) = timeout_queue;
//     }

//     return dehy;
// }


void RTXStoreTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{

    RTXStore *store = value;
    mstime_t now = rm_current_time_ms();
    while (expiration_count(store) != 0) {
      char *expired_key = pop_next(store);
      mstime_t next_timeout  = next_at(store);
      char *next_key = pop_next(store);
      if (next_timeout > now ) {
        RedisModule_EmitAOF(aof,"RTEXP.EXPIREAT", "cl", next_key, next_timeout);
      }
      rm_free(next_key);
    }
}

/************************
 *    Module Utils
 ************************/

// extract store and element key from the input params of the redis module command
int getStoreAndKey(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, void *store,
                   const void *element_key, size_t* element_key_len) {
  if (argc < 2) return REDISMODULE_ERR;

  // RedisModuleString *rtxstore_name = argv[1];
  RedisModuleString *element_key_str = argv[1];

  // get key store_name
  // RedisModuleKey *store_key =
      // RedisModule_OpenKey(ctx, rtxstore_name, REDISMODULE_READ | REDISMODULE_WRITE);
  store = rtxStore; //validateStoreKey(ctx, store_key);

  element_key = RedisModule_StringPtrLen(element_key_str, element_key_len);
  return REDISMODULE_OK;
}

int redisSetWithExpiration(RedisModuleCtx *ctx, RedisModuleString *element_key,
                           RedisModuleString *element, mstime_t ttl_ms) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, element_key, REDISMODULE_WRITE);
  if (element != NULL) RedisModule_StringSet(key, element);
  RedisModule_SetExpire(key, ttl_ms + RTEXP_BUFFER_MS);
  RedisModule_CloseKey(key);
  return REDISMODULE_OK;
}

void timerCb(RedisModuleCtx *ctx, void *p) {
  RTXStore *store = p;
  mstime_t now = rm_current_time_ms();
  if (next_at(store) < now) {
    char *expired_key = pop_next(store);
    RedisModule_Call(ctx, "UNLINK", "c", expired_key);
    rm_free(expired_key);
  }
}

/********************
 *    DS Binding
 ********************/

int set_ttl(RTXStore *store, char *element_key, size_t len, mstime_t ttl_ms) {
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
  return -1;
}

/************************
 *    Module Commands
 ************************/

// TODO: add DEL function to store

// 1. REXPIRE {key} {ttl_ms}
int ExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  size_t element_key_len;
  if ((argc != 3) || (getStoreAndKey(ctx, argv, argc, &store, &element_key, &element_key_len) != REDISMODULE_OK))
    RedisModule_WrongArity(ctx);

  RedisModuleString *ttl_ms_str = argv[3];
  mstime_t ttl_ms;
  if (RedisModule_StringToLongLong(ttl_ms_str, &ttl_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Time stamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }

  // THE ACTUAL EXPIRATION 
  if (set_ttl(store, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithError(ctx, "Unexpeted Error Occured");
    return REDISMODULE_ERR;
  }
}

// 2. REXPIREAT {key} {timestamp_ms}
int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  size_t element_key_len;
  if ((argc != 3) || (getStoreAndKey(ctx, argv, argc, &store, &element_key, &element_key_len) != REDISMODULE_OK))
    RedisModule_WrongArity(ctx);

  RedisModuleString *timestamp_ms_str = argv[3];
  mstime_t timestamp_ms;

  if (RedisModule_StringToLongLong(timestamp_ms_str, &timestamp_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Time stamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }
  mstime_t ttl_ms = timestamp_ms - rm_current_time_ms();
  // TODO: verify timestamp not in the past and return specific error

  if (set_ttl(store, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithError(ctx, "Unexpeted Error Occured");
    return REDISMODULE_ERR;
  }
}

// 3. RTTL {key}
int TTLCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  size_t element_key_len;
  if (getStoreAndKey(ctx, argv, argc, &store, &element_key, &element_key_len) != REDISMODULE_OK)
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
  size_t element_key_len;
  if (getStoreAndKey(ctx, argv, argc, &store, &element_key, &element_key_len) != REDISMODULE_OK)
    RedisModule_WrongArity(ctx);

  redisSetWithExpiration(ctx, argv[2], NULL, REDISMODULE_NO_EXPIRE);
  remove_expiration(store, element_key);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

// 5. RSETEX {key} {value} {ttl}
int SetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *element_key;
  RTXStore *store;
  size_t element_key_len;
  if ((argc != 4) || (getStoreAndKey(ctx, argv, argc, &store, &element_key, &element_key_len) != REDISMODULE_OK))
    RedisModule_WrongArity(ctx);

  RedisModuleString *element = argv[3];
  RedisModuleString *ttl_ms_str = argv[4];
  mstime_t ttl_ms;

  if (RedisModule_StringToLongLong(ttl_ms_str, &ttl_ms) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Time stamp must be parsable to type Long Long");
    return REDISMODULE_ERR;
  }

  redisSetWithExpiration(ctx, argv[2], element, ttl_ms + RTEXP_BUFFER_MS);
  if (set_ttl(store, element_key, element_key_len, ttl_ms) == REDISMODULE_OK) {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
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

  // RedisModule_ReplicateVerbatim(ctx); // TODO: is this allowed?

  rtxStore = newRTXStore();

  // assert(ctx->timer == NULL);  // TODO: how should I add a timer to the context?
  // there is one in redisearch GarbageCollectorCtx
  struct RMUtilTimer *tm = RMUtil_NewPeriodicTimer( 
      timerCb, NULL, &rtxStore,
      (struct timespec){
          .tv_sec = 0,
          .tv_nsec =
              10000000});  // TODO: existing Expire is on milliseconds scale, what shold this be?

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// Init Module
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  // Register the module itself
  if (RedisModule_Init(ctx, "RTEXP", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // RedisModuleTypeMethods tm = {
  //   .version = REDISMODULE_TYPE_METHOD_VERSION,
  //   .rdb_load = RTXStoreTypeRdbLoad,
  //   .rdb_save = RTXStoreTypeRdbSave,
  //   .aof_rewrite = RTXStoreTypeAofRewrite,
  //   .free = RTXStoreTypeFree,
  // };

  RedisModule_AutoMemory(ctx);
  CreateRTEXP();

  // register commands - using the shortened utility registration macro
  // TODO: not all of these are read commands
  // RMUtil_RegisterReadCmd(ctx, "START", CreateRTEXPCommand);  // "write deny-oom"
  // RMUtil_RegisterReadCmd(ctx, "REXPIRE", ExpireCommand);
  // RMUtil_RegisterReadCmd(ctx, "REXPIREAT", ExpireAtCommand);
  // RMUtil_RegisterReadCmd(ctx, "RTTL", TTLCommand);
  // RMUtil_RegisterReadCmd(ctx, "RUNEXPIRE", UnexpireCommand);
  // RMUtil_RegisterReadCmd(ctx, "RSETEX", SetexCommand);
  // RMUtil_RegisterReadCmd(ctx, "REXECEX", ExecuteAndExpireCommand);

  return REDISMODULE_OK;
}
