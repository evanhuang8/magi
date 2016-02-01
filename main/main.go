package main

import (
	"fmt"
	"magi"
	"time"
)

func main() {
	dqHosts := make([]map[string]interface{}, 3, 3)
	dqHosts[0] = map[string]interface{}{
		"address": "127.0.0.1:7711",
	}
	dqHosts[1] = map[string]interface{}{
		"address": "127.0.0.1:7712",
	}
	dqHosts[2] = map[string]interface{}{
		"address": "127.0.0.1:7713",
	}
	producer, err := magi.Producer(&dqHosts)
	if err != nil {
		panic(err)
	}
	fmt.Println("Magi API v" + producer.APIVersion)
	// Add a job
	delay, _ := time.ParseDuration("10s")
	ETA := time.Now().Add(delay)
	job, err := producer.AddJob("test", "job1", ETA, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Job added:", job.ID)
	// Get a job
	_job, err := producer.GetJob(job.ID)
	if err != nil {
		panic(err)
	}
	fmt.Println(_job)
}
