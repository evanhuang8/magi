package lock

import (
	"magi/pool"
)

// GetLocks attempts to acquire the lock on the job for
// a majority of redis instances in the pool
func GetLocks(pool *pool.RedisPool, id string) {

}

// ReleaseLock releases the lock on the job id
func ReleaseLock(pool *pool.RedisPool) {

}
