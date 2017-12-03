# RTExp Full Command Documentation

As a rule of thumb all R* commands are aligned with their corrisponding vanilla Redis counterpart. 


## REXPIRE

### Format:

```
REXPIRE {key} {ttl_ms}
```

### Description:

Set up a realtime auto-expiration timer for key `key`, `ttl_ms` milliseconds from now. 

### Parameters:

* **key**: The key under which the item to expire is to be found.
* **ttl_ms**: The number of milliseconds to wait before expiring the given key.

### Complexity

Avarge: O(1)
Worst: O(log n)

### Returns

0 & OK on success, 1 & error otherwise.


## REXPIREAT

### Format

```
REXPIREAT {key} {timestamp_ms}
```

### Description

Set up a realtime auto-expiration timer for key `key` that will expire when the wall clock will be `timestamp_ms` in milliseconds.

### Parameters

* **key**: The key under which the item to expire is to be found.
* **timestamp_ms**: The timestamp in milliseconds in which to expire the given key.

### Complexity

Avarge: O(1)
Worst: O(log n)

### Returns

0 & OK on success, 1 & error otherwise.


## RTTL

### Format

```
RTTL {key}
```

### Description

Return the time left before key `key` will be expired, in milliseconds.

### Parameters

* **key**: The key under which the item to expire is to be found.

### Complexity

O(1)

### Returns

Long Long representing the remaining time before expiration on success, error otherwise.


## RUNEXPIRE

### Format

```
RUNEXPIRE {key}
```

### Description

Unset the realtime auto-expiration timer for key `key`.

### Parameters

* **key**: The key under which the item to expire is to be found.

### Complexity

O(1) 

### Returns

0 & OK on success, 1 & error otherwise.




## RSETEX

### Format

```
RSETEX {key} {value} {ttl_ms}
```

### Description

Set a value for key `key` and mark for realtime auto-expiration in `ttl_ms` milliseconds from now. 

### Parameters:

* **key**: The key under which the item to expire is to be found.
* **value**: The value to be inserted to the given key.
* **ttl_ms**: The number of milliseconds to wait before expiring the given key.

### Complexity

Avarge: O(1)
Worst: O(log n)

### Returns

0 & OK on success, 1 & error otherwise.


## REXECEX 


### Format

```
REXECEX {command name} {key} {ttl_ms} {commnad params} 
```

### Description

Run `command` with params `command params` for key `key` and mark for `key` for realtime auto-expiration in `ttl_ms` milliseconds from now. 
The following command will be invoked prior to setting the expiration:
```
<command> <key> <command params>
```

### Parameters:

* **command name**: The command name to run for `key`. 
* **key**: The key to set expiration for.
* **ttl_ms**: The number of milliseconds to wait before expiring the given key.
* **command params**: The Parameters for the command `command`. 

### Complexity

Avarge: O(1) + runtime complexity of `command`
Worst: O(log n) + runtime complexity of `command`

### Returns

`command` response on success, error otherwise.
In case of failiure to set expiration error will be returned even if `command` was successful.
