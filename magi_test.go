package magi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testQueue = "magi_test"

var disqueHosts = []map[string]interface{}{
	map[string]interface{}{
		"address": "127.0.0.1:7711",
	},
	map[string]interface{}{
		"address": "127.0.0.1:7712",
	},
	map[string]interface{}{
		"address": "127.0.0.1:7713",
	},
}

func TestProducer(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	producer, err := Producer(&disqueHosts)
	assert.Empty(err)
	assert.NotEmpty(producer)
	// Add job
	delay, _ := time.ParseDuration("10s")
	eta := time.Now().Add(delay)
	job, err := producer.AddJob(testQueue, "job1", eta, nil)
	assert.Empty(err)
	assert.NotEmpty(job)
	assert.NotEmpty(job.ID)
	assert.Equal(job.Body, "job1")
	// Get job
	_job, err := producer.GetJob(job.ID)
	assert.Empty(err)
	assert.NotEmpty(_job)
	assert.Equal(job.Body, _job.Body)
}
