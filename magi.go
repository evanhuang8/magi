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
func Producer(dqHosts *[]map[string]interface{}) (*Magi, error) {
	dqCluster := cluster.NewDisqueCluster(dqHosts)
	producer := &Magi{
		APIVersion: MagiAPIVersion,
		dqCluster:  dqCluster,
	}
	return producer, nil
}

// Consumer creates a Magi instance that acts as a consumer
func Consumer(dqHosts *[]map[string]interface{}, rHosts *[]map[string]interface{}) *Magi {
	dqCluster := cluster.NewDisqueCluster(dqHosts)
	rCluster := cluster.NewRedisCluster(rHosts)
	consumer := &Magi{
		APIVersion: MagiAPIVersion,
		dqCluster:  dqCluster,
		rCluster:   rCluster,
	}
	return consumer
}

/**
 * Producer methods
 */

// AddJob adds a job to the queue
func (magi *Magi) AddJob(queueName string, body string, ETA time.Time, options *map[string]string) (*job.Job, error) {
	_job, err := job.Add(magi.dqCluster, queueName, body, ETA, options)
	return _job, err
}

// GetJob tries to get the details about a job
func (magi *Magi) GetJob(id string) (*job.Job, error) {
	details, err := magi.dqCluster.Get(id)
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
func (magi *Magi) Process(queueName string, concurrency int) {
	var ids []string
	var id string
	var err error
	timeout, _ := time.ParseDuration(BlockingTimeout)
	for {
		ids, err = magi.dqCluster.Fetch(queueName, concurrency, timeout)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			for _, id = range ids {
				go magi.process(queueName, id)
			}
		}
	}
}

func (magi *Magi) process(queueName string, id string) {
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
	processor, exists := magi.processors[queueName]
	if !exists {
		return
	}
	// Get job details
	_job, err := magi.GetJob(id)
	if err != nil {
		return
	}
	// Acquire lock
	_lock = lock.CreateLock(magi.rCluster, id)
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
	result, err = magi.dqCluster.Ack(id)
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
