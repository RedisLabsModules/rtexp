# Design Overview for the RTExp module

The design of the system was governed by the need to minimize lag for large scale inputs. Namly by combining high efficiency low runtime complexity data structures to produce an time efficient, space agnostic algorithm.

## Trie backed Heap
The current design of this module backbone is as follows:
1. A Key marked to expire at a specific datetime is compiled into a timer Struct containig all *key*, *expiration datetime* and *expiration version* (see below).
2. Timer structs are stored in a Trie by key, if the Trie already containes *key*, the newer (last requested) *timestamp* is stored and the *expiration version* is inremented by one (starting by default from 0).
3. An entry for that timer containig the *key* and *expiration version* is inserted to a Heap, sorted by *expiration datetime*.
4. Every system tick (as set by it's granularity) we peek into the top of the Heap, if the node is set to expire, we validate the *expiration version* against the data stored in the Trie - a valid *version* will be the same as stored in the Trie, and the key will be expired. A stored value in the Heap that contains an invalid *version* or points to a non-exsisting *key* can be disreguarded.  

This Algorithm Perfers complexity on the auto-expiration side in favor of insertion time, resulting in a responsive system with low client latancy.