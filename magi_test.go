package magi

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"magi/cluster"
	"magi/lock"
)

func FlushQueue() error {
	cmd := exec.Command("./test/disque/flush.sh")
	err := cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return nil
}

func TestMain(m *testing.M) {
	flag.Parse()
	FlushQueue()
	code := m.Run()
	FlushQueue()
	os.Exit(code)
}

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

var dqConfig = &cluster.DisqueClusterConfig{
	Hosts: disqueHosts,
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

func RandomKey() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(errors.New("Fail to generate random bytes!"))
	}
	key := fmt.Sprintf("lockkey%X%X%X%X%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return key
}

func TestProducer(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	producer, err := Producer(dqConfig)
	assert.Empty(err)
	assert.NotEmpty(producer)
	defer producer.Close()
	// Add job
	delay, _ := time.ParseDuration("10s")
	eta := time.Now().Add(delay)
	job, err := producer.AddJob(testQueue, "job1", eta)
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

func TestLockAcquisition(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create lock
	key := RandomKey()
	l := lock.CreateLock(c, key)
	l.Duration = 3 * time.Second
	// Acquire lock
	success, err := l.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l.IsActive())
}

func TestLockMutualExclusion(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create locks
	key := RandomKey()
	l1 := lock.CreateLock(c, key)
	l2 := lock.CreateLock(c, key)
	l1.Duration = 3 * time.Second
	l2.Duration = 3 * time.Second
	// Acquire lock on l1
	success, err := l1.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l1.IsActive())
	// Acquire lock on l2, should fail
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.False(success)
	assert.False(l2.IsActive())
}

func TestLockIsolation(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create locks
	key1 := RandomKey()
	key2 := RandomKey()
	l1 := lock.CreateLock(c, key1)
	l2 := lock.CreateLock(c, key2)
	l1.Duration = 16 * time.Second
	l2.Duration = 16 * time.Second
	// Acquire lock on l1
	success, err := l1.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l1.IsActive())
	// Acquire lock on l2
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l2.IsActive())
}

func TestLockRelease(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create lock
	key := RandomKey()
	l1 := lock.CreateLock(c, key)
	l1.Duration = 16 * time.Second
	l2 := lock.CreateLock(c, key)
	// Acquire lock
	success, err := l1.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l1.IsActive())
	// Acquire lock on the same key, should fail
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.False(success)
	assert.False(l2.IsActive())
	// Release original lock
	success, err = l1.Release()
	assert.Empty(err)
	assert.True(success)
	assert.False(l1.IsActive())
	// Acquire lock, should succeed this time
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.True(success)
	assert.True(l2.IsActive())
}

func TestLockAutoExpire(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create lock
	key := RandomKey()
	l1 := lock.CreateLock(c, key)
	duration := 3 * time.Second
	l1.Duration = duration
	l2 := lock.CreateLock(c, key)
	// Acquire lock
	success, err := l1.Get(false)
	assert.Empty(err)
	assert.True(success)
	// Acquire lock on the same key, should fail
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.False(success)
	// Sleep past the expiration time
	time.Sleep(duration)
	// Acquire lock, should succeed this time
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.True(success)
}

func TestLockAutoRenew(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create lock
	key := RandomKey()
	l1 := lock.CreateLock(c, key)
	duration := 5 * time.Second
	l1.Duration = duration
	l2 := lock.CreateLock(c, key)
	// Acquire lock
	success, err := l1.Get(true)
	assert.Empty(err)
	assert.True(success)
	// Acquire lock on the same key, should fail
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.False(success)
	// Sleep past the expiration time
	time.Sleep(duration)
	// Acquire lock on the same key, should still fail
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.False(success)
	// Release original lock
	success, err = l1.Release()
	assert.Empty(err)
	assert.True(success)
	assert.False(l1.IsActive())
	// Acquire lock, should succeed this time
	success, err = l2.Get(false)
	assert.Empty(err)
	assert.True(success)
}

func TestLockContestDuo(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	c := cluster.NewRedisCluster(rConfig)
	assert.NotEmpty(c)
	defer c.Close()
	// Create 2 locks on the same key
	key := RandomKey()
	locks := []*lock.Lock{
		lock.CreateLock(c, key),
		lock.CreateLock(c, key),
	}
	// Unlease dogs of war
	result := make(chan int, 2)
	acquired := 0
	for _, l := range locks {
		func(l *lock.Lock) {
			go func() {
				success, err := l.Get(false)
				assert.Empty(err)
				if success {
					acquired++
				}
				result <- 1
			}()
		}(l)
	}
	// Wait for them to finish
	<-result
	<-result
	assert.Equal(acquired, 1)
}

func TestLockContestTrio(t *testing.T) {
	assert := assert.New(t)
	// Instantiation
	clusters := []*cluster.RedisCluster{
		cluster.NewRedisCluster(rConfig),
		cluster.NewRedisCluster(rConfig),
		cluster.NewRedisCluster(rConfig),
	}
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()
	// Create 3 locks on the same key
	key := RandomKey()
	locks := []*lock.Lock{}
	for _, c := range clusters {
		l := lock.CreateLock(c, key)
		locks = append(locks, l)
	}
	// Unlease dogs of war
	result := make(chan int, 3)
	acquired := 0
	for _, l := range locks {
		func(l *lock.Lock) {
			go func() {
				success, err := l.Get(false)
				assert.Empty(err)
				if success {
					acquired++
				}
				result <- 1
			}()
		}(l)
	}
	// Wait for them to finish
	for i := 0; i < 3; i++ {
		<-result
	}
	fmt.Println(acquired)
	assert.True(acquired <= 1)
}
