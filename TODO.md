# TBD

- [ ] notification mechanism (note whenever somthing is expired)
- [ ] fix random crash issue with heap.c
- [ ] write documentation for lib
- [ ] write documentation for redis module - ELABORATE
- [ ] bind lib to redis module commands
    - [ ] REXECEX
    - [X] REXPIRE
    - [X] REXPIREAT
    - [X] RTTL
    - [X] RUNEXPIRE
    - [X] RSETEX
    - [X] Make sure to bind commands correctly in terms of read/write
- [X] clean Makefiles
- [X] expire using UNLINK
- [X] write lib tests
- [X] add -DREDIS_MODULE_TARGET for that redis module
- [X] use key, key_len pairs everywhere
- [X] auto expiration
    - [X] maybe sleep until closest and interrupt on any expiration update
    - [X] add background task to poll Store
- [X] add some decent seatbelts:
    - [X] Check for existance of key before setting expiration (to resemble redis main)
    - [X] redisSetWithExpiration (using PEXPIRE)
- [X] write module tests

# deferred tasks -  these may no be needed, time will tell
- [ ] add rtexp as redis type
- [ ] add DEL function to store
- [ ] random store key
    - [ ] define a global with the store key
    - [ ] on rdb load look at the global and use it as key
    - [ ] if on rdb load we already have an open key MERGE stores
- [ ] add some cleanup on module termination
- [ ] RTTL {key1} {key2} ...
