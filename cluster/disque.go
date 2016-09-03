package cluster

import (
	"time"

	"github.com/goware/disque"
)

// DisqueClusterLBMode is the type for load balancing mode
type DisqueClusterLBMode int

const (
	// DisqueClusterLBModeRoundRobin is the round robin lb mode
	DisqueClusterLBModeRoundRobin = 1 << iota
)

// DisqueCluster is a struct representing a disque cluster with multiple instances
type DisqueCluster struct {
	config *DisqueClusterConfig

	pools     []*disque.Pool
	poolIndex int

	lbMode  DisqueClusterLBMode
	lbFixed bool
}

// DisqueClusterConfig is the config struct for creating a disque cluster
type DisqueClusterConfig struct {
	Hosts  []map[string]interface{}
	LBMode DisqueClusterLBMode
}

// DisqueOpConfig is the config struct for any disque operations
type DisqueOpConfig struct {
	Timeout    time.Duration
	Replicate  int
	Delay      time.Duration
	RetryAfter time.Duration
	TTL        time.Duration
	MaxLen     int
}

// Config generates a config representation for the underlying disque lib
func (c *DisqueOpConfig) Config() disque.Config {
	return disque.Config{
		Timeout:    c.Timeout,
		Replicate:  c.Replicate,
		Delay:      c.Delay,
		RetryAfter: c.RetryAfter,
		TTL:        c.TTL,
		MaxLen:     c.MaxLen,
	}
}

// NewDisqueCluster creates disque connection pools to the cluster using hosts information
func NewDisqueCluster(config *DisqueClusterConfig) (*DisqueCluster, error) {
	var lbMode DisqueClusterLBMode
	if config.LBMode > 0 {
		lbMode = config.LBMode
	} else {
		lbMode = DisqueClusterLBModeRoundRobin
	}
	cluster := &DisqueCluster{
		config: config,
		lbMode: lbMode,
	}
	n := len(config.Hosts)
	pools := make([]*disque.Pool, n, n)
	for i, host := range config.Hosts {
		pool, err := disque.New(host["address"].(string))
		if err != nil {
			return nil, err
		}
		pools[i] = pool
	}
	cluster.pools = pools
	cluster.poolIndex = 0
	return cluster, nil
}

// Close closes the disque connection pools to the disque cluster
func (cluster *DisqueCluster) Close() error {
	for _, pool := range cluster.pools {
		err := pool.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Add adds a job to the disque cluster
func (cluster *DisqueCluster) Add(queueName string, data string, config *DisqueOpConfig) (*disque.Job, error) {
	pool := cluster.getPool()
	if config != nil {
		pool = pool.With(config.Config())
	}
	job, err := pool.Add(data, queueName)
	return job, err
}

// Get finds a job in the disque cluster by its id
func (cluster *DisqueCluster) Get(id string) (*disque.Job, error) {
	pool := cluster.getPool()
	job, err := pool.Fetch(id)
	return job, err
}

// Ack tries to ack a job as done in the disque cluster
func (cluster *DisqueCluster) Ack(id string) error {
	pool := cluster.getPool()
	job := &disque.Job{
		ID: id,
	}
	err := pool.Ack(job)
	return err
}

// Nack tries to nack a job so that job is put back into the queue
func (cluster *DisqueCluster) Nack(id string) error {
	pool := cluster.getPool()
	job := &disque.Job{
		ID: id,
	}
	err := pool.Nack(job)
	return err
}

// Fetch receives job from the disque cluster for processing
func (cluster *DisqueCluster) Fetch(queueName string, config *DisqueOpConfig) (*disque.Job, error) {
	pool := cluster.getPool()
	if config != nil {
		pool = pool.With(config.Config())
	}
	pool = pool.Timeout(2 * time.Second)
	job, err := pool.Get(queueName)
	return job, err
}

// Pool chaining functions

// Chain sets the index of pool to use for subsequent operations
func (cluster *DisqueCluster) Chain() {
	cluster.poolIndex = cluster.nextPoolIndex()
	cluster.lbFixed = true
}

// Unchain unsets the index of the pool
func (cluster *DisqueCluster) Unchain() {
	cluster.lbFixed = false
}

func (cluster *DisqueCluster) nextPoolIndex() int {
	n := len(cluster.pools)
	i := cluster.poolIndex
	if !cluster.lbFixed {
		if cluster.lbMode == DisqueClusterLBModeRoundRobin {
			i++
			if i >= n {
				i = 0
			}
		}
	}
	return i
}

func (cluster *DisqueCluster) getPool() *disque.Pool {
	i := cluster.nextPoolIndex()
	cluster.poolIndex = i
	return cluster.pools[i]
}
