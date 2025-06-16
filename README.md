# dsync
synchronization tool

prototype:

* if dest does not exist, create it
* implement Threshold to decide if file should be copied (default to 0.5) meaning that if the file is more than 50% different, it will be copied
* chunk comparison
