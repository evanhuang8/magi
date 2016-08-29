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
	pools  []*redis.Pool
	config *RedisClusterConfig
}

// RedisClusterConfig is the config struct for creating a redis locking cluster
type RedisClusterConfig struct {
	Hosts []map[string]interface{}
}

// NewRedisCluster creates a redis connection pool using hosts information
func NewRedisCluster(config *RedisClusterConfig) *RedisCluster {
	cluster := &RedisCluster{
		config: config,
	}
	n := len(config.Hosts)
	pools := make([]*redis.Pool, n, n)
	for i, host := range config.Hosts {
		func(host map[string]interface{}) {
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
		}(host)
	}
	cluster.pools = pools
	return cluster
}

// Close closes the connection pools to the redis instances
func (cluster *RedisCluster) Close() error {
	for _, pool := range cluster.pools {
		err := pool.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetQuorum returns the correct qorum necessary for acquiring the lock
func (cluster *RedisCluster) GetQuorum() int {
	return len(cluster.pools)/2 + 1
}

// GetPools returns a reference to all redis connection pools
func (cluster *RedisCluster) GetPools() *[]*redis.Pool {
	return &cluster.pools
}
