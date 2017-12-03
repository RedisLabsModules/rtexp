# Redis Labs Realtime-Expiration Redis Module
## Overview:
The perpose of this module is to simplify, improve and extend the expiration mechanism of redis elements for real-time applications. 
 
## Commands:
The API follows the vanilla Redis expiration API, denoting Real-Time with R as the prefix.

This module includes the following commands (See full documentation [here](docs/Commands.md)):
1. `REXPIRE {key} {ttl_ms}` - Set TTL for a given key
2. `REXPIREAT {key} {timestamp_ms}` - Set specific expiration date-time for a given key
3. `RTTL {key}` - See the remeining time (in millisecons) until the key is auto expired
4. `RUNEXPIRE {key}` - Remove auto expiration from the given key
5. `RSETEX {key} {value} {ttl_ms}` - Set key to a given value and mark it for auto expiration.
6. `REXECEX {CMD} {key} {ttl_ms} {....}` - Run CMD, set key to contain the result, and mark that key for auto expiration.

The module commands provide no guarantees of duplication with normal expiration mechanisms.
