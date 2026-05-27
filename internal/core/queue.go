package core

import (
	"fmt"

	"github.com/TrienThongLu/goMQ/internal/config"
)

type Queue struct {
	arr  []byte
	head uint32
	tail uint32
}

func createQueue() *Queue {
	return &Queue{
		arr:  make([]byte, config.MsgLen*config.QueueSize),
		head: 0,
		tail: 0,
	}
}

func (q *Queue) push(data []byte) {
	q.arr[q.tail] = byte(len(data))
	copy(q.arr[q.tail+1:int(q.tail+1)+len(data)], data)
	q.tail = (q.tail + config.MsgLen) % (config.MsgLen * config.QueueSize)
}

func (q *Queue) pop() []byte {
	if q.head == q.tail {
		return nil
	}

	size := int(q.arr[q.head])
	data := q.arr[q.head+1 : q.head+1+uint32(size)]
	q.head = (q.head + config.MsgLen) % (config.MsgLen * config.QueueSize)

	return data
}

func (q *Queue) peek(offset uint32) []byte {
	if q.head == q.tail {
		return nil
	}

	pos := (q.head + (offset * config.MsgLen)) % (config.MsgLen * config.QueueSize)
	if q.head < q.tail {
		if !(pos >= q.head && pos < q.tail) {
			return nil
		}
	} else {
		if !(pos >= q.head || pos < q.tail) {
			return nil
		}
	}

	size := int(q.arr[pos])
	pos++
	data := q.arr[pos : pos+uint32(size)]

	return data
}

func (q *Queue) debug(topicId uint16) {
	fmt.Printf("Debug queue topic %d:\n", topicId)
	cur := q.head

	for {
		size := int(q.arr[cur])
		data := q.arr[cur+1 : cur+1+uint32(size)]
		fmt.Printf("%s", data)

		cur = (cur + config.MsgLen) % (config.MsgLen * config.QueueSize)
		if cur == q.tail {
			break
		}
	}
}

func (q *Queue) size() int {
	return int(q.tail-q.head+(config.MsgLen*config.QueueSize)) % (config.MsgLen * config.QueueSize) / config.MsgLen
}
