# Magi - Proposal

The goal of the Magi project is to create an easy-to-use, truly scalable and distributed queue and processor setup for delayed tasks.

This project came about when I encountered [**Disque**](https://github.com/antirez/disque), a project by the creator of [**Redis**](http://redis.io/). Per the description on the repo's page, the author implemented Disque to capture the essence of "Redis as a job queue" use case. Building on some share components as Redis, Disque promises to be distributed and scalable. Regarding jobs, it can guarantee at-least-once delivery of the messages. Given the features and promises, I want to implement a robust, scalable, distributed delayed task queue using Disque.

**Disclaimer**: this project is strictly experimental. As of the time of writing, Disque has just released its 1.0 RC1, so its first official release is very close, but it's still possible that things can change and cause havoc, so use with care.

## Scenarios

As mentioned in the title, Magi is designed to function as a distributed queue for delayed tasks/jobs. Specifically, it's designed for the scenarios where there are multiple producers and multiple consumers. Jobs produced should be executed at no earlier than some timestamp in the future. More often than not, jobs are expected to be executed right near the specified timestamp. Scenarios where jobs are simply expected to be executed immediately are trivial to implement using Magi.

## Deliverables

The Magi project is expected to deliver the following:

- A scalable, distributed system for processing delayed tasks
- Tests demonstrating the promises of the system are satisfied
- A tutorial for setting up such system in development as well as in production
- A tutorial for setting up such system using minimal setup and large scale setup
- Dockerfiles to for quick setups

## Promises of System

The system should satisfy the following promises:

**Distributed**

- Multiple copies of data
- No single point of failure
- Load is spread across nodes
- Automatic fail-over

**Scalability**

- Components that are potential bottlenecks are explicitly identified
- Scaling should be as trivial as adding another instance to the setup

**Robustness**

- Data is persistent at rest
- Data is not lost in failures unless the all nodes go down at once
- Network partition and/or other failures are always expected and handled
- Logs at each critical steps

**Queue**

- Jobs are executed no earlier than specified timestamp
- Each job is processed by one consumer only at the same time
- Each job can only be executed successfully once
- Each job is eventually processed exactly once if the action of consuming the job itself is not inherently faulty
- Failed jobs can be retried, retries can be defined by job producer
- Best effort FIFO processing according to delay timestamps
- Job can contain arbitrary forms of data
- Job results can be (optionally) persisted

**General**

- Provide language agnostic APIs so that end producers and consumers can be written in other languages
- Bundled monitoring APIs

## Design

**Data Storage**

As mentioned in the introduction, Magi builds on the foundation of Disque. Each Disque instance essentially holds the job data: each job is replicated across multiple Disque instances. Suppose there are `n` Disque instances, by default the replication factor is `n` as well, so that all instances hold a copy of the job before the job's creation is acknowledged; if `n` is big, and we want to opt for higher performance, we can set the replication factor to be lower (3, for example). Either way, we can be confident that the job data is not lost if one instance goes away.

**Timed Execution**

At the core of Magi is the delayed execution of jobs at expected timestamps. When adding jobs to Disque, we can set a delay value, which means that instance (and other instances the jobs replicated to) will not make the job available for the consumer until after the delay time elapses. It's worth noting that such delay time is calculated using the local clock of the server, so using `ntp` or other time sync mechanism to keep the clocks in sync is recommended.

Since the Disque instances do not push jobs to end consumers, the consumers need to have a mechanism to retrieve jobs that are due at a timely manner. The nice thing is that the `GETJOB` command from Disque can be set as blocking - that is, the consumer can listen on the queue until there is a job available. In addition, it will still be good to set a timeout (say, 2 seconds, or TBD) on the blocking command, and re-issue after the timeout. Given the nature of the `GET JOB`, it makes sense to separate the consumer and producer completely - at least they should use two different connections.

**Exactly Once Processing**

As mentioned before, Disque guarantees at-least-once delivery. Such feature is useful in ensuring the robustness of the job, but often times we only really want the job to be processed successfully once. When jobs may be delivered multiple times, however, how do we ensure no two consumers are processing the same job at the same time?

The solution is a lock on the job. When a consumer receives a job, it should attempt to take the lock on the job: if lock is acquired, then it will go ahead and process the job; otherwise, after several failed attempts, it should simply move on and try to acquire another job. The lock itself should be auto-expiring, and the consumer needs to extend the duration of the lock if the processing is taking longer than the default lock time.

Per the nature of Disque, the lock needs to be distributed, too. To address the problem, we can use the [Redlock algorithm](http://redis.io/topics/distlock) implementation using Redis. If implemented correctly, it should give us the locking mechanism we need. The nice thing about the Redlock is the multi-master nature: a consumer only needs to take lock from a majority of the Redis instances, so if one instance goes away the stack can continue to function.

**Job Results**

In some use cases, the results of the execution should be stored somewhere. In some queue implementations, this is done directly in the queue (for example, numerous job queue implemented on top of Redis). To keep thing separate, however, we will allow the user to set custom storage backends, and provide a uniform way of writing/retrieving such results. In other words, we will let the consumer deal with storing the result, since it's not a strong goal of the system.

## Topology

```
   +----+-------+-+-------+-----+
   |    |       ^ ^       |     |
   | +--v--+  +-+-+-+  +--v--+  |
   | |Redis|  |Redis|  |Redis|  |
   | +-+---+  +--+--+  +----++  |
   |   ^ ^-----+ ^ +------^ ^   |
   |   |       | | |        |   |
  ++---+---+ +-+-+-+--+ +---+---++
  |Consumer| |Consumer| |Consumer|
  +----+---+ +---+----+ +---+----+
       ^         ^          ^
       |         |          |
    +--+---+  +--+---+  +---+--+
    |Disque+-->Disque<--+Disque|
    +--+---+  +------+  +---+--+
       ^                    ^
       |                    |
  +----+---+            +---+----+
  |Producer|            |Producer|
  +--------+            +--------+
```

## Roadmap
