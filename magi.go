package magi

import (
	"fmt"
	"magi/cluster"
	"magi/job"
	"magi/lock"
	"time"
)

// MagiAPIVersion is the current API version
var MagiAPIVersion = "0.1"

// BlockingTimeout is the timeout used for blocking operations
var BlockingTimeout = "5s"

// Magi represents the top level queue application
type Magi struct {
	APIVersion string
	dqCluster  *cluster.DisqueCluster
	rCluster   *cluster.RedisCluster
	processors map[string]*Processor
}

// Producer creates a Magi instance that acts as a producer
func Producer(config *cluster.DisqueClusterConfig) (*Magi, error) {
	dqCluster, err := cluster.NewDisqueCluster(config)
	if err != nil {
		return nil, err
	}
	producer := &Magi{
		APIVersion: MagiAPIVersion,
		dqCluster:  dqCluster,
	}
	return producer, nil
}

// Consumer creates a Magi instance that acts as a consumer
func Consumer(dqConfig *cluster.DisqueClusterConfig, rConfig *cluster.RedisClusterConfig) (*Magi, error) {
	dqCluster, err := cluster.NewDisqueCluster(dqConfig)
	if err != nil {
		return nil, err
	}
	rCluster := cluster.NewRedisCluster(rConfig)
	consumer := &Magi{
		APIVersion: MagiAPIVersion,
		dqCluster:  dqCluster,
		rCluster:   rCluster,
	}
	return consumer, nil
}

// Close terminates all connections from the Magi instance
func (m *Magi) Close() error {
	if m.dqCluster != nil {
		err := m.dqCluster.Close()
		if err != nil {
			return err
		}
	}
	if m.rCluster != nil {
		err := m.rCluster.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * Producer methods
 */

// AddJob adds a job to the queue
func (m *Magi) AddJob(queueName string, body string, ETA time.Time) (*job.Job, error) {
	_job, err := job.Add(m.dqCluster, queueName, body, ETA, nil)
	return _job, err
}

// GetJob tries to get the details about a job
func (m *Magi) GetJob(id string) (*job.Job, error) {
	details, err := m.dqCluster.Get(id)
	if err != nil {
		return nil, err
	}
	_job, err := job.FromDetails(details)
	return _job, err
}

/**
 * Consumer methods
 */

// Processor is an interface that all job processor should implement
type Processor interface {
	Process(*job.Job) (interface{}, error)
	ShouldAutoRenew() bool
}

// Process starts the job processing procedure
func (m *Magi) Process(queueName string) {
	for {
		job, err := m.dqCluster.Fetch(queueName, nil)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			go m.process(queueName, job.ID)
		}
	}
}

func (m *Magi) process(queueName string, id string) {
	var _lock *lock.Lock
	// Catch panics
	defer func() {
		if err := recover(); err != nil {
			if err == lock.ErrLockLost {
				// Lock is lost, release remaining lock segments
				_lock.Release()
			} else {
				panic(err)
			}
		}
	}()
	// Check if the processor is available
	processor, exists := m.processors[queueName]
	if !exists {
		return
	}
	// Get job details
	_job, err := m.GetJob(id)
	if err != nil {
		return
	}
	// Acquire lock
	_lock = lock.CreateLock(m.rCluster, id)
	result, err := _lock.Get((*processor).ShouldAutoRenew())
	// If lock cannot be acquired, return and do not acknowledge
	if err != nil {
		return
	}
	if !result {
		return
	}
	// Process the job
	(*processor).Process(_job)
	// Ack the job
	err = m.dqCluster.Ack(id)
	if err != nil {
		return
	}
	if !result {
		return
	}
	// Release the lock
	result, err = _lock.Release()
	if err != nil {
		return
	}
	if !result {
		return
	}
	return
}

// AckJob sends an ACK for the job to the disque cluster
func AckJob(job *job.Job) (bool, error) {
	return true, nil
}
