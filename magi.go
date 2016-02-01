package magi

import (
	"fmt"
	"magi/job"
	"magi/pool"
	"time"
)

// MagiAPIVersion is the current API version
var MagiAPIVersion = "0.1"

// BlockingTimeout is the timeout used for blocking operations
var BlockingTimeout = "5s"

// Magi represents the top level queue application
type Magi struct {
	APIVersion string
	pool       *pool.DisquePool
}

// Producer creates a Magi instance that acts as a producer
func Producer(dqHosts *[]map[string]interface{}) (*Magi, error) {
	dqPool, err := pool.NewDisquePool(dqHosts)
	if err != nil {
		return nil, err
	}
	producer := &Magi{
		APIVersion: MagiAPIVersion,
		pool:       dqPool,
	}
	return producer, nil
}

// Consumer creates a Magi instance that acts as a consumer
func Consumer(dqHosts *[]map[string]interface{}, rHosts *[]map[string]interface{}) *Magi {
	consumer := &Magi{
		APIVersion: MagiAPIVersion,
	}
	return consumer
}

/**
 * Producer methods
 */

// AddJob adds a job to the queue
func (magi *Magi) AddJob(queueName string, body string, ETA time.Time, options *map[string]string) (*job.Job, error) {
	_job, err := job.Add(magi.pool, queueName, body, ETA, options)
	return _job, err
}

// GetJob tries to get the details about a job
func (magi *Magi) GetJob(id string) (*job.Job, error) {
	details, err := magi.pool.Get(id)
	if err != nil {
		return nil, err
	}
	_job, err := job.FromDetails(details)
	return _job, err
}

/**
 * Consumer methods
 */

// Process starts the job processing procedure
func (magi *Magi) Process(queueName string, concurrency int) {
	var ids []string
	var id string
	var err error
	timeout, _ := time.ParseDuration(BlockingTimeout)
	for {
		ids, err = magi.pool.Fetch(queueName, concurrency, timeout)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			for _, id = range ids {
				go magi.process(id)
			}
		}
	}
}

func (magi *Magi) process(id string) {

}
