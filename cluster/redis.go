package cluster

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// LockPrefix is the prefix for redis lock keys
var LockPrefix = "magirl:"

// GetKey constructs a redis key by applying prefix
func GetKey(key string) string {
	return LockPrefix + key
}

// RedisCluster is a struct representing a group of connections pools to the target redis instances
type RedisCluster struct {
	hosts []map[string]interface{}
	pools []*redis.Pool
}

// NewRedisCluster creates a redis connection pool using hosts information
func NewRedisCluster(hosts *[]map[string]interface{}) *RedisCluster {
	cluster := &RedisCluster{
		hosts: *hosts,
	}
	n := len(*hosts)
	pools := make([]*redis.Pool, n, n)
	for i, host := range *hosts {
		pool := &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", host["address"].(string))
				if err != nil {
					return nil, err
				}
				if _, exists := host["auth"]; exists {
					if _, err := conn.Do("AUTH", host["auth"].(string)); err != nil {
						conn.Close()
						return nil, err
					}
				}
				if _, exists := host["db"]; exists {
					if _, err := conn.Do("SELECT", host["db"].(string)); err != nil {
						conn.Close()
						return nil, err
					}
				}
				return conn, nil
			},
		}
		pools[i] = pool
	}
	cluster.pools = pools
	return cluster
}

// GetQuorum returns the correct qorum necessary for acquiring the lock
func (cluster *RedisCluster) GetQuorum() int {
	return len(cluster.pools)/2 + 1
}

// GetPools returns a reference to all redis connection pools
func (cluster *RedisCluster) GetPools() *[]*redis.Pool {
	return &cluster.pools
}
