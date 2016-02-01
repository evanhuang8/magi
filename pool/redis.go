package pool

import (
	"math"

	"github.com/garyburd/redigo/redis"
)

// LockPrefix is the prefix for redis lock keys
var LockPrefix = "magirl:"

// RedisPool is a struct representing a group of connections to redis
type RedisPool struct {
	hosts    []map[string]interface{}
	conns    []*redis.Conn
	majority int
}

// NewRedisPool creates a redis connection pool using hosts information
func NewRedisPool(hosts *[]map[string]interface{}) (*RedisPool, error) {
	n := len(*hosts)
	pool := &RedisPool{
		hosts:    *hosts,
		majority: int(math.Ceil(float64(n) / 2)),
	}
	conns := make([]*redis.Conn, n, n)
	var connection redis.Conn
	var err error
	for i, host := range *hosts {
		connection, err = redis.Dial("tcp", host["address"].(string))
		if err != nil {
			return nil, err
		}
		conns[i] = &connection
	}
	pool.conns = conns
	return pool, nil
}

// GetLock attempts to get the lock from a single redis instance
func GetLock(conn *redis.Conn, id string) {

}
