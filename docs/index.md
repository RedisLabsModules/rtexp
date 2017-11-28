<!-- <center>![logo.png](logo.png)</center> -->

# RTExp - Redis real time expiration module

RTExp is a real time key expiration module for Redis, developed by [Tamar Labs](http://www.tamarlabs.com) for [Redis Labs](http://redislabs.com). 

!!! note "Quick Links:"
    * [Source Code at GitHub](https://github.com/RedisLabsModules/rtexp).


!!! tip "Supported Platforms"
    RTExp is developed and tested on Linux <!--and Mac OS, on x86_64 CPUs.-->

    <!-- i386 CPUs should work fine for small data-sets, but are not tested and not recommended. Atom CPUs are not supported currently.  -->

## Commands
The API follows the Redis expiration API, denoting Real-Time with R as the prefix.
This module includes the following commands:
1. REXPIRE {key} {ttl_ms}
2. REXPIREAT {key} {timestamp_ms}
3. RTTL {key}
4. RUNEXPIRE {key}
5. RSETEX {key} {value} {ttl_ms}

