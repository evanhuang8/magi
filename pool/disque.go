package pool

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/zencoder/disque-go/disque"
)

// DisquePool is a struct representing a disque connection pool
type DisquePool struct {
	hosts     []map[string]interface{}
	conns     []*disque.Disque
	replicate int
}

// NewDisquePool creates a disque connection pool using hosts information
func NewDisquePool(hosts *[]map[string]interface{}) (*DisquePool, error) {
	n := len(*hosts)
	pool := &DisquePool{
		hosts:     *hosts,
		replicate: len(*hosts),
	}
	conns := make([]*disque.Disque, n, n)
	var connection *disque.Disque
	var err error
	for i, host := range *hosts {
		connection = disque.NewDisque(
			[]string{
				host["address"].(string),
			},
			1000,
		)
		err = connection.Initialize()
		if err != nil {
			return nil, err
		}
		conns[i] = connection
		fmt.Println("Connected to Disque at:", host["address"].(string))
	}
	pool.conns = conns
	return pool, nil
}

// Close closes the disque connection on the disque pool
func (pool *DisquePool) Close() {
	for _, connection := range pool.conns {
		connection.Close()
	}
}

// Add adds a job to the disque cluster
func (pool *DisquePool) Add(queueName string, data string, timeout time.Duration, options *map[string]string) (id string, err error) {
	conn := pool.getConnection(false)
	if _, val := (*options)["RETRY"]; val {
		(*options)["RETRY"] = "5"
	}
	id, err = conn.PushWithOptions(queueName, data, timeout, *options)
	return
}

// Get finds details of a job in the disque cluster
func (pool *DisquePool) Get(id string) (details *disque.JobDetails, err error) {
	conn := pool.getConnection(false)
	details, err = conn.GetJobDetails(id)
	return
}

// Fetch receives job(s) from the disque cluster for processing
func (pool *DisquePool) Fetch(queueName string, count int, timeout time.Duration) ([]string, error) {
	conn := pool.getConnection(true)
	defer pool.releaseConnection(conn)
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

func (pool *DisquePool) getConnection(blocking bool) *disque.Disque {
	n := len(pool.conns)
	i := rand.Intn(n)
	return pool.conns[i]
}

func (pool *DisquePool) releaseConnection(conn *disque.Disque) {
	return
}
