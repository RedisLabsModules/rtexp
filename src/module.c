#include "redismodule.h"
#include "rmutil/util.h"
#include "khash.h"
#include "rmutil/strings.h"



//#########################################################
//#
//#                    C Utilities
//#
//#########################################################

char* string_append(char* a, const char* b)
{
    char* retstr = RedisModule_Alloc(strlen(a)+strlen(b));
    strcpy(retstr, a);
    strcat(retstr, b);
    // printf("printing: %s", retstr);
    RedisModule_Free(a);
    return retstr;
}


long long current_time_ms (void)
{
    long            ms; // Milliseconds
    time_t          s;  // Seconds
    struct timespec spec;

    clock_gettime(CLOCK_REALTIME, &spec);

    s  = spec.tv_sec*1000;
    ms = round(spec.tv_nsec / 1.0e6); // Convert nanoseconds to milliseconds

    return s+ms;
}


//#########################################################
//#
//#                     Redis Type
//#
//#########################################################


//#########################################################
//#
//#                     Module Commands
//#
//#########################################################

// 1. REXPIRE {key} {milliseconds}
int ExpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "");
    return REDISMODULE_OK;
}

// 2. REXPIREAT {key} {timestamp_ms}
int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "");
    return REDISMODULE_OK;
}

// 3. RTTL {key}
int TTLCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "");
    return REDISMODULE_OK;
}

// 4. RUNEXPIRE {key}
int UnexpireCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "");
    return REDISMODULE_OK;
}

// 5. RSETEX {key} {value} {ttl}
int SetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "");
    return REDISMODULE_OK;
}

// 6. REXECEX {key} {ttl} {....} ?????
// int ExpireAtCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
// {
//     RedisModule_ReplyWithSimpleString(ctx, "");
//     return REDISMODULE_OK;
// }





int RedisModule_OnLoad(RedisModuleCtx *ctx)
{
    // Register the module itself
    if (RedisModule_Init(ctx, "REALEX", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }


    // register commands - using the shortened utility registration macro
    RMUtil_RegisterReadCmd(ctx, "REXPIRE",   ExpireCommand);
    RMUtil_RegisterReadCmd(ctx, "REXPIREAT", ExpireAtCommand);
    RMUtil_RegisterReadCmd(ctx, "RTTL",      TTLCommand);
    RMUtil_RegisterReadCmd(ctx, "RUNEXPIRE", UnexpireCommand);
    RMUtil_RegisterReadCmd(ctx, "RSETEX",    SetexCommand);
    //??? RMUtil_RegisterReadCmd(ctx, "REXECEX",     ExecuteCommand);

    return REDISMODULE_OK;
}