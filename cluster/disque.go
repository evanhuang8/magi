package cluster

import (
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.com/zencoder/disque-go/disque"
)

// DisqueCluster is a struct representing a disque cluster with multiple instances
type DisqueCluster struct {
	hosts     []map[string]interface{}
	pools     []*disque.DisquePool
	replicate int
}

// NewDisqueCluster creates disque connection pools to the cluster using hosts information
func NewDisqueCluster(hosts *[]map[string]interface{}) *DisqueCluster {
	n := len(*hosts)
	cluster := &DisqueCluster{
		hosts:     *hosts,
		replicate: len(*hosts),
	}
	pools := make([]*disque.DisquePool, n, n)
	var pool *disque.DisquePool
	for i, host := range *hosts {
		pool = disque.NewDisquePool(
			[]string{
				host["address"].(string),
			},
			1000,           // cycle
			5,              // initial capacity
			20,             // max capacity
			15*time.Minute, // idle timeout
		)
		pools[i] = pool
	}
	cluster.pools = pools
	return cluster
}

// Close closes the disque connection pools to the disque cluster
func (cluster *DisqueCluster) Close() {
	for _, pool := range cluster.pools {
		pool.Close()
	}
}

// Add adds a job to the disque cluster
func (cluster *DisqueCluster) Add(queueName string, data string, timeout time.Duration, options *map[string]string) (id string, err error) {
	pool := cluster.getPool()
	conn, err := pool.Get(context.Background())
	if err != nil {
		return
	}
	defer pool.Put(conn)
	if _, val := (*options)["RETRY"]; val {
		(*options)["RETRY"] = "5"
	}
	id, err = conn.PushWithOptions(queueName, data, timeout, *options)
	return
}

// Get finds details of a job in the disque cluster
func (cluster *DisqueCluster) Get(id string) (details *disque.JobDetails, err error) {
	pool := cluster.getPool()
	conn, err := pool.Get(context.Background())
	if err != nil {
		return
	}
	defer pool.Put(conn)
	details, err = conn.GetJobDetails(id)
	return
}

// Ack tries to ack a job as done in the disque cluster
func (cluster *DisqueCluster) Ack(id string) (bool, error) {
	pool := cluster.getPool()
	conn, err := pool.Get(context.Background())
	if err != nil {
		return false, err
	}
	defer pool.Put(conn)
	err = conn.Ack(id)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Fetch receives job(s) from the disque cluster for processing
func (cluster *DisqueCluster) Fetch(queueName string, count int, timeout time.Duration) ([]string, error) {
	pool := cluster.getPool()
	conn, err := pool.Get(context.Background())
	if err != nil {
		return nil, err
	}
	defer pool.Put(conn)
	jobs, err := conn.FetchMultiple(queueName, count, timeout)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, count)
	for _, _job := range jobs {
		ids = append(ids, _job.JobId)
	}
	return ids, nil
}

// Private functions

func (cluster *DisqueCluster) getPool() *disque.DisquePool {
	n := len(cluster.pools)
	i := rand.Intn(n)
	return cluster.pools[i]
}
