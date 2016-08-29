package cluster

import (
	"math/rand"

	"github.com/goware/disque"
)

// DisqueCluster is a struct representing a disque cluster with multiple instances
type DisqueCluster struct {
	pools  []*disque.Pool
	config *DisqueClusterConfig
}

// DisqueClusterConfig is the config struct for creating a disque cluster
type DisqueClusterConfig struct {
	Hosts []map[string]interface{}
}

// DisqueOpConfig is the config struct for any disque operations
type DisqueOpConfig struct {
	disque.Config
}

// NewDisqueCluster creates disque connection pools to the cluster using hosts information
func NewDisqueCluster(config *DisqueClusterConfig) (*DisqueCluster, error) {
	n := len(config.Hosts)
	cluster := &DisqueCluster{
		config: config,
	}
	pools := make([]*disque.Pool, n, n)
	for i, host := range config.Hosts {
		pool, err := disque.New(host["address"].(string))
		if err != nil {
			return nil, err
		}
		pools[i] = pool
	}
	cluster.pools = pools
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
		pool = pool.With(config.Config)
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
		pool = pool.With(config.Config)
	}
	job, err := pool.Get(queueName)
	return job, err
}

// Private functions

func (cluster *DisqueCluster) getPool() *disque.Pool {
	n := len(cluster.pools)
	i := rand.Intn(n)
	return cluster.pools[i]
}
