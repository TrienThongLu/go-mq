package core

import "sync"

type Topic struct {
	id             uint16
	queue          *Queue
	consumerGroups []*ConsumerGroup
	lock           sync.Mutex
}

func createTopic(id uint16) *Topic {
	return &Topic{
		id:             id,
		queue:          createQueue(),
		consumerGroups: make([]*ConsumerGroup, 0),
	}
}
