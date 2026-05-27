package core

import "sync"

type Partition struct {
	queue *Queue
	lock  sync.Mutex
}

func CreatePartition() *Partition {
	return &Partition{
		queue: createQueue(),
	}
}
