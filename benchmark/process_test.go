package benchmark_test

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/evanhuang8/magi"
	"github.com/evanhuang8/magi/cluster"
	"github.com/evanhuang8/magi/job"
)

func FlushQueue() {
	cmd := exec.Command("../test/disque/flush.sh")
	err := cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return
}

func TestMain(m *testing.M) {
	flag.Parse()
	FlushQueue()
	code := m.Run()
	FlushQueue()
	os.Exit(code)
}

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

var dqConfig = &cluster.DisqueClusterConfig{
	Hosts: disqueHosts,
}

var disqueHostsSingle = []map[string]interface{}{
	map[string]interface{}{
		"address": "127.0.0.1:7711",
	},
}

var dqsConfig = &cluster.DisqueClusterConfig{
	Hosts: disqueHostsSingle,
}

var redisHosts = []map[string]interface{}{
	map[string]interface{}{
		"address": "127.0.0.1:7777",
	},
	map[string]interface{}{
		"address": "127.0.0.1:7778",
	},
	map[string]interface{}{
		"address": "127.0.0.1:7779",
	},
}

var rConfig = &cluster.RedisClusterConfig{
	Hosts: redisHosts,
}

var redisHostsSingle = []map[string]interface{}{
	map[string]interface{}{
		"address": "127.0.0.1:7777",
	},
}

var rsConfig = &cluster.RedisClusterConfig{
	Hosts: redisHostsSingle,
}

func RandomKey() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(errors.New("Fail to generate random bytes!"))
	}
	key := fmt.Sprintf("lockkey%X%X%X%X%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return key
}

type BenchProcessor struct {
	Bodies []string
	mutex  sync.Mutex

	CompleteChannel chan bool
	target          int
}

func (p *BenchProcessor) Process(job *job.Job) (interface{}, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Bodies = append(p.Bodies, job.Body+"dummy")
	if len(p.Bodies) >= p.target {
		p.CompleteChannel <- true
	}
	return true, nil
}

func (p *BenchProcessor) ShouldAutoRenew(job *job.Job) bool {
	return true
}

func NewBenchProcessor(n int) *BenchProcessor {
	p := &BenchProcessor{
		Bodies:          make([]string, 0, n),
		CompleteChannel: make(chan bool),
		target:          n,
	}
	return p
}

func TestProcessingSingleQueue(t *testing.T) {
	assert := assert.New(t)
	FlushQueue()
	// Instantiation
	consumer, err := magi.Consumer(dqsConfig, rConfig)
	assert.Empty(err)
	assert.NotEmpty(consumer)
	defer consumer.Close()
	queue := "jobq" + RandomKey()
	// Add jobs
	n := 10000
	bodies := make([]string, 0, n)
	eta := time.Now()
	conf := &cluster.DisqueOpConfig{
		Replicate: 1,
	}
	for i := 0; i < n; i++ {
		body := RandomKey()
		job, err := consumer.AddJob(queue, body, eta, conf)
		assert.Empty(err)
		assert.NotEmpty(job)
		assert.NotEmpty(job.ID)
		assert.Equal(job.Body, body)
		bodies = append(bodies, body)
	}
	// Setup the processor
	p := NewBenchProcessor(n)
	consumer.Register(queue, p)
	// Kick off processing
	start := time.Now()
	go consumer.Process(queue)
	time.Sleep(2 * time.Second)
	assert.True(consumer.IsProcessing())
	// Wait for it to be processed
	<-p.CompleteChannel
	end := time.Now()
	delta := end.Sub(start)
	assert.Equal(len(p.Bodies), n)
	for _, body := range bodies {
		isProcessed := false
		for _, _body := range p.Bodies {
			if body+"dummy" == _body {
				isProcessed = true
			}
		}
		assert.True(isProcessed)
	}
	fmt.Printf("%d jobs processed in %v\n", n, delta)
	fmt.Printf("Average processing time %v\n", delta/time.Duration(n))
	fmt.Printf("Average %f jobs per second\n", float64(n)/delta.Seconds())
}

func TestProcessingMultipleQueues(t *testing.T) {
	assert := assert.New(t)
	FlushQueue()
	// Instantiation
	consumer, err := magi.Consumer(dqConfig, rConfig)
	assert.Empty(err)
	assert.NotEmpty(consumer)
	defer consumer.Close()
	queue := "jobq" + RandomKey()
	// Add jobs
	n := 10000
	bodies := make([]string, 0, n)
	eta := time.Now()
	conf := &cluster.DisqueOpConfig{
		Replicate: 1,
	}
	for i := 0; i < n; i++ {
		body := RandomKey()
		job, err := consumer.AddJob(queue, body, eta, conf)
		assert.Empty(err)
		assert.NotEmpty(job)
		assert.NotEmpty(job.ID)
		assert.Equal(job.Body, body)
		bodies = append(bodies, body)
	}
	// Setup the processor
	p := NewBenchProcessor(n)
	consumer.Register(queue, p)
	// Kick off processing
	start := time.Now()
	go consumer.Process(queue)
	time.Sleep(2 * time.Second)
	assert.True(consumer.IsProcessing())
	// Wait for it to be processed
	<-p.CompleteChannel
	end := time.Now()
	delta := end.Sub(start)
	assert.Equal(len(p.Bodies), n)
	for _, body := range bodies {
		isProcessed := false
		for _, _body := range p.Bodies {
			if body+"dummy" == _body {
				isProcessed = true
			}
		}
		assert.True(isProcessed)
	}
	fmt.Printf("%d jobs processed in %v\n", n, delta)
	fmt.Printf("Average processing time %v\n", delta/time.Duration(n))
	fmt.Printf("Average %f jobs per second\n", float64(n)/delta.Seconds())
}

func TestProcessingMultipleConsumer(t *testing.T) {
	assert := assert.New(t)
	FlushQueue()
	// Instantiation
	producer, err := magi.Producer(dqConfig)
	assert.Empty(err)
	assert.NotEmpty(producer)
	defer producer.Close()
	queue := "jobq" + RandomKey()
	c := runtime.NumCPU()/2 + 1
	fmt.Printf("%d consumers\n", c)
	// Add jobs
	n := 10000
	bodies := make([]string, 0, n)
	eta := time.Now()
	conf := &cluster.DisqueOpConfig{
		Replicate: 3,
	}
	for i := 0; i < n; i++ {
		body := RandomKey()
		job, err := producer.AddJob(queue, body, eta, conf)
		assert.Empty(err)
		assert.NotEmpty(job)
		assert.NotEmpty(job.ID)
		assert.Equal(job.Body, body)
		bodies = append(bodies, body)
	}
	// Setup the processor
	p := NewBenchProcessor(n)
	// Instantiate multiple consumers
	start := time.Now()
	controllers := make([]chan bool, c, c)
	for i := 0; i < c; i++ {
		controllers[i] = make(chan bool)
	}
	for i := 0; i < c; i++ {
		go func(i int) {
			consumer, err := magi.Consumer(dqConfig, rConfig)
			assert.Empty(err)
			assert.NotEmpty(consumer)
			defer consumer.Close()
			// Kick off processing
			consumer.Register(queue, p)
			go consumer.Process(queue)
			// Wait for all the processing to finish
			<-controllers[i]
		}(i)
	}
	// Wait for it to be processed
	<-p.CompleteChannel
	for i := 0; i < c; i++ {
		controllers[i] <- true
	}
	end := time.Now()
	delta := end.Sub(start)
	assert.Equal(len(p.Bodies), n)
	for _, body := range bodies {
		isProcessed := false
		for _, _body := range p.Bodies {
			if body+"dummy" == _body {
				isProcessed = true
			}
		}
		assert.True(isProcessed)
	}
	fmt.Printf("%d jobs processed in %v\n", n, delta)
	fmt.Printf("Average processing time %v\n", delta/time.Duration(n))
	fmt.Printf("Average %f jobs per second\n", float64(n)/delta.Seconds())
}
