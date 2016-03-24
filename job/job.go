package job

import (
	"encoding/json"
	"magi/cluster"
	"strconv"
	"time"

	"github.com/zencoder/disque-go/disque"
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
	Raw       *disque.JobDetails
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

// Add adds a job to queue via a DisquePool
func Add(cluster *cluster.DisqueCluster, queueName string, body string, ETA time.Time, options *map[string]string) (*Job, error) {
	job := &Job{
		QueueName: queueName,
		ETA:       ETA,
	}
	timeout, _ := time.ParseDuration(JobTimeout)
	// Calculate the delay
	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now
	delay := ETA.Sub(now).Seconds()
	_options := make(map[string]string)
	if delay > 0 {
		_options["DELAY"] = strconv.Itoa(int(delay))
	}
	data, _ := json.Marshal(
		&Data{
			Body:      body,
			ETA:       ETA,
			CreatedAt: now,
			UpdatedAt: now,
		},
	)
	id, err := cluster.Add(queueName, string(data), timeout, &_options)
	if err != nil {
		return nil, err
	}
	job.ID = id
	job.Body = body
	return job, nil
}

// FromDetails creates a Job instance using details data
func FromDetails(details *disque.JobDetails) (*Job, error) {
	var data Data
	err := json.Unmarshal([]byte(details.Message), &data)
	if err != nil {
		return nil, err
	}
	job := &Job{
		ID:        details.JobId,
		QueueName: details.QueueName,
		Body:      data.Body,
		ETA:       data.ETA,
		CreatedAt: data.CreatedAt,
		UpdatedAt: data.UpdatedAt,
		Raw:       details,
	}
	return job, nil
}
