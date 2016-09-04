# Magi

[![Build Status](https://travis-ci.org/evanhuang8/magi.svg?branch=master)](https://travis-ci.org/evanhuang8/magi)
[![GoDoc](https://godoc.org/github.com/evanhuang8/magi?status.png)](https://godoc.org/github.com/evanhuang8/magi)

![Magi - Evangelion](https://raw.githubusercontent.com/evanhuang8/magi/images/images/magi.gif)

A scalable, distributed delay task queue building on top of [**disque**](https://github.com/antirez/disque) and [**redis**](http://redis.io/). You can read about the implementation proposal [here](./PROPOSAL.md).

### Usage

There are two primary ways to use `magi`, one is producer, the other is consumer. Technically, consumer can produce jobs as well, but creating two separate instances of `magi` is recommended.

### Producer

To create a producer, you should first create the configuration for your `disque` nodes:

```go
disqueHosts := []map[string]interface{}{
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
dqConfig := &cluster.DisqueClusterConfig{
	Hosts: disqueHosts,
}
```

then you can simply pass the config to the constructor:

```go
producer, err := Producer(dqConfig)
```

If you have a large `disque` cluster (with many nodes), you don't need to provide all the hosts here; only configure the producer to interact with 1-3 nodes, and spread your producers over many sub-clusters.

### Consumer

A consumer needs information of the `redis` hosts in addition to the `disque` information. Please note the `cluster` terminology here can be a bit confusing, since we are not establishing connections to a `RedisCluster` (see [here](http://redis.io/topics/cluster-spec)), but rather several instances of redis that have no knowledge of each other. Of course, you may use the actual `RedisCluster` for the individual instances here, but in this case we are using single redis instances as examples.

```go
redisHosts := []map[string]interface{}{
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
rConfig := &cluster.RedisClusterConfig{
	Hosts: redisHosts,
}
```

Similarily, just instantiate the consumer:

```go
consumer, err := Consumer(dqConfig, rConfig)
```

At this point, the consumer is not yet processing messages from the queue. To start processing, you must define your own `Processor` instance that implements the following interface:

```go
type Processor interface {
	Process(*job.Job) (interface{}, error)
	ShouldAutoRenew(*job.Job) bool
}
```

For the interface, `Process` will be called when a job is received from the queue. The other function, `ShouldAutoRenew`, will be called to determine whether the lock on the job should be renewed when it expires; in most cases the answer is yes, but this allows the processor to have more fine-tune control over the situation.

For example, a dummy processor can be:

```go
type DummyProcessor struct {
	Bodies []string
	mutex  sync.Mutex
}

func (p *DummyProcessor) Process(job *job.Job) (interface{}, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Bodies = append(p.Bodies, job.Body+"dummy")
	return true, nil
}

func (p *DummyProcessor) ShouldAutoRenew(job *job.Job) bool {
	return true
}
```

Once you have the processor defined, you can just instantiate one and register to process jobs for a specific queue with the consumer:

```go
queueName := "myqueue"
p := &DummyProcessor{}
consumer.Register(queueName, p)
```

Finally, you can instruct the consumer to kick off processing:

```go
go consumer.Process(queueName)
```

Since `consumer.Process` will run indefinitely, we are putting it in a goroutine. 

### Shutdown

Regardless of the usage, you should call `Close` on the magi instance to perform a graceful shutdown:

```go
consumer.Close()
```

For consumer especially, it will wait for the current job batch to finish, and then stop the processing.

## License

BSD License

Copyright (c) 2016, Evan Huang

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.