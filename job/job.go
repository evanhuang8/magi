package job

import (
	"encoding/json"
	"time"

	"github.com/evanhuang8/magi/cluster"
	"github.com/goware/disque"
)

// JobTimeout is the default job timeout
var JobTimeout = "2s"

// Job represents a job
type Job struct {
	ID        string
	QueueName string
	Body      string
	ETA       time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	Raw       *disque.Job
}

func (job *Job) String() string {
	output, _ := json.MarshalIndent(&job, "", "  ")
	return string(output)
}

// Data represents the Magi wrapper for the job's data
type Data struct {
	Body      string
	ETA       time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Add adds a job to queue
func Add(c *cluster.DisqueCluster, queueName string, body string, ETA time.Time, config *cluster.DisqueOpConfig) (*Job, error) {
	job := &Job{
		QueueName: queueName,
		ETA:       ETA,
	}
	if config == nil {
		config = &cluster.DisqueOpConfig{}
	}
	// Calculate the delay
	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now
	delay := ETA.Sub(now)
	if delay.Seconds() > 0 {
		config.Delay = delay
	}
	data, _ := json.Marshal(
		&Data{
			Body:      body,
			ETA:       ETA,
			CreatedAt: now,
			UpdatedAt: now,
		},
	)
	_job, err := c.Add(queueName, string(data), config)
	if err != nil {
		return nil, err
	}
	job.ID = _job.ID
	job.Body = body
	return job, nil
}

// FromDetails creates a Job instance using details data
func FromDetails(details *disque.Job) (*Job, error) {
	var data Data
	err := json.Unmarshal([]byte(details.Data), &data)
	if err != nil {
		return nil, err
	}
	job := &Job{
		ID:        details.ID,
		QueueName: details.Queue,
		Body:      data.Body,
		ETA:       data.ETA,
		CreatedAt: data.CreatedAt,
		UpdatedAt: data.UpdatedAt,
		Raw:       details,
	}
	return job, nil
}
