/*

The Redlock implementaion is heavily based on the redsync project by Mahmud Ridwan (https://github.com/hjr265/redsync)

Copyright (c) 2014, Mahmud Ridwan
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the {organization} nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package lock

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"magi/cluster"

	"github.com/garyburd/redigo/redis"
)

const (
	// DefaultDuration is the default duration of the lock
	DefaultDuration = 8 * time.Second
	// DefaultAttempts is the default attempts for acquiring lock
	DefaultAttempts = 16
	// DefaultDelay is the default delay between attempts
	DefaultDelay = 512 * time.Millisecond
	// DefaultFactor is the default drift factor
	DefaultFactor = 0.01
)

// Lock represents a distributed lock on a specific key
type Lock struct {
	Key       string                // redis key
	Duration  time.Duration         // duration for the lock
	Factor    float64               // drift factor
	Attempts  int                   // maximum attempts to acquire lock before failure
	Delay     time.Duration         // time between attempts
	Quorum    int                   // number of individual locks to take before considered success
	AutoRenew bool                  // whether to auto renew the lock if it expires
	Cluster   *cluster.RedisCluster // redis cluster

	value string // random string used for value of lock

	until time.Time // timestamp at which the lock expires

	ar        bool        // indicates whether the auto renew timer is on
	arControl chan string // auto renew control channel
	arResult  chan string // auto renew result channel

	lockMutex   sync.Mutex // internal mutex for getting lock
	updateMutex sync.Mutex // internal mutex for updating properties
}

// CreateLock creates a lock attempt on the job by job id
func CreateLock(cluster *cluster.RedisCluster, id string) *Lock {
	lock := &Lock{
		Key:      id,
		Duration: DefaultDuration,
		Attempts: DefaultAttempts,
		Delay:    DefaultDelay,
		Factor:   DefaultFactor,
		Quorum:   cluster.GetQuorum(),
		Cluster:  cluster,
	}
	return lock
}

var (
	// ErrLockFailedAfterMaxAttempts is the error for failing to acquire the lock after maximum attempts
	ErrLockFailedAfterMaxAttempts = errors.New("Lock Error: fail to acquire lock after maximum attempts!")
	// ErrLockExtensionFailed is the error for failing to extend the lock
	ErrLockExtensionFailed = errors.New("Lock Error: lock extension failed!")
	// ErrLockEmptyLock is the error for operating on a lock that is not acquired
	ErrLockEmptyLock = errors.New("Lock Error: attempting to operate on a lock that is not acquired!")
	// ErrLockExtendWhileAR is the error for trying to extend the lock manually while the auto renew process is running
	ErrLockExtendWhileAR = errors.New("Lock Error: attempting to extend the lock manually while auto renew is running!")
	// ErrLockLost is the error for lock lost during auto renewal
	ErrLockLost = errors.New("Lock Error: lock is lost during auto renewal!")
)

// Get attempts to acquire the lock on the key
func (lock *Lock) Get(ar bool) (bool, error) {
	var err error
	// Pick up internal lock
	lock.lockMutex.Lock()
	defer lock.lockMutex.Unlock()
	// Generate random value for the lock
	raw := make([]byte, 32)
	_, err = rand.Read(raw)
	if err != nil {
		return false, err
	}
	value := base64.StdEncoding.EncodeToString(raw)
	pools := lock.Cluster.GetPools()
	// Attempt to acquire the lock
	for i := 0; i < lock.Attempts; i++ {
		// Acquire lock on each node until quorum is achieved
		n := 0
		start := time.Now()
		for _, pool := range *pools {
			conn := pool.Get()
			duration := int(lock.Duration / time.Millisecond)
			reply, err := redis.String(conn.Do("SET", lock.Key, value, "NX", "PX", duration))
			conn.Close()
			if err != nil {
				continue
			}
			if reply != "OK" {
				continue
			}
			n++
			if n >= lock.Quorum {
				break
			}
		}
		// Check if a lock with time left is acquired in a quorum of redis hosts
		until := time.Now().Add(lock.Duration - time.Now().Sub(start) - time.Duration(int64(float64(lock.Duration)*lock.Factor)) + 2*time.Millisecond)
		// If not, release any acquired locks
		if n < lock.Quorum || time.Now().After(until) {
			pools := lock.Cluster.GetPools()
			for _, pool := range *pools {
				if pool == nil {
					continue
				}
				conn := pool.Get()
				_, err = releaseLock.Do(conn, lock.Key, value)
				if err != nil {
					continue
				}
			}
			return false, err
		}
		// Lock acquired, set proper values
		lock.value = value
		lock.until = until
		// Start the auto renew timer if necessary
		if ar {
			lock.StartAutoRenew()
		}
		// Otherwise, start auto renew timer
		return true, nil
	}
	// Failed to acquire lock after maximum attempts
	return false, ErrLockFailedAfterMaxAttempts
}

// Release releases the lock on key
func (lock *Lock) Release() (bool, error) {
	// Check if lock is indeed acquired
	if lock.value == "" {
		return false, ErrLockEmptyLock
	}
	// Take internal mutexes
	lock.lockMutex.Lock()
	defer lock.lockMutex.Unlock()
	lock.updateMutex.Lock()
	defer lock.updateMutex.Unlock()
	// Stop auto renew if necessary
	if lock.ar {
		lock.StopAutoRenew()
	}
	// Release locks
	n := 0
	pools := lock.Cluster.GetPools()
	for _, pool := range *pools {
		if pool == nil {
			continue
		}
		conn := pool.Get()
		status, err := releaseLock.Do(conn, lock.Key, lock.value)
		conn.Close()
		// Ignore error
		if err != nil {
			continue
		}
		// Key does not exist
		if status == 0 {
			continue
		}
		// Increment counter
		n++
	}
	if n < lock.Quorum {
		return false, nil
	}
	lock.value = ""
	return true, nil
}

// Extend extends the lock by the duration
func (lock *Lock) Extend(duration time.Duration) (bool, error) {
	if lock.ar {
		err := ErrLockExtendWhileAR
		return false, err
	}
	result, err := lock.extend(duration)
	return result, err
}

// Internal extend, does not check for auto renew status
func (lock *Lock) extend(duration time.Duration) (bool, error) {
	if lock.value == "" {
		return false, ErrLockEmptyLock
	}
	// Take internal lock
	lock.updateMutex.Lock()
	defer lock.updateMutex.Unlock()
	// Extend lock on each redis hosts
	var err error
	extension := int(duration / time.Millisecond)
	n := 0
	pools := lock.Cluster.GetPools()
	for _, pool := range *pools {
		if pool == nil {
			continue
		}
		conn := pool.Get()
		reply, err := extendLock.Do(conn, lock.Key, lock.value, extension)
		conn.Close()
		if err != nil {
			continue
		}
		if reply != "OK" {
			continue
		}
		n++
	}
	if n < lock.Quorum {
		return false, err
	}
	return true, nil
}

// StartAutoRenew starts the auto renew timer
func (lock *Lock) StartAutoRenew() error {
	if lock.value == "" {
		return ErrLockEmptyLock
	}
	// Take internal lock
	lock.updateMutex.Lock()
	defer lock.updateMutex.Unlock()
	// Start auto renewal
	lock.ar = true
	go lock.autoRenew()
	return nil
}

var (
	// LockARCommandStop is the command for stopping the auto renew timer
	LockARCommandStop = "STOP"
	// LockARSignalStopSuccess is the signal for a successful stop
	LockARSignalStopSuccess = "STOPSuccess"
)

// StopAutoRenew stops the auto renew timer
func (lock *Lock) StopAutoRenew() bool {
	lock.arControl <- LockARCommandStop
	signal := <-lock.arResult
	return signal == LockARSignalStopSuccess
}

// Auto renew timer
func (lock *Lock) autoRenew() {
	// Run timer until otherwise told
	for {
		// Check commands
		select {
		case command := <-lock.arControl:
			// Stop auto renew on command
			if command == LockARCommandStop {
				// Send signal
				lock.arResult <- LockARSignalStopSuccess
				return
			}
		default:
			// Sleep till next renewal
			sleepDuration := time.Duration(int64(float64(lock.Duration) * 0.5))
			time.Sleep(sleepDuration)
			// Extend lock
			result, err := lock.extend(lock.Duration)
			if err != nil {
				fmt.Println(err)
				panic(ErrLockLost)
			}
			if !result {
				panic(ErrLockLost)
			}
		}
	}
}

// Redis script for releasing lock
var releaseLockScript = `
  if redis.call("GET", KEY[1]) == ARGV[1] then
    return redis.call("DEL", KEY[1])
  else
    return 0
  end
`
var releaseLock = redis.NewScript(1, releaseLockScript)

// Redis script for extending lock
var extendLockScript = `
  if redis.call("GET", KEY[1]) == ARGV[1] then
    return redis.call("SET", KEYS[1], ARGV[1], "XX", "PX", ARGV[2])
  else
    return "ERR"
  end
`
var extendLock = redis.NewScript(1, extendLockScript)
