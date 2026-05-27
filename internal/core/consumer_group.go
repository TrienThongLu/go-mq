package core

import (
	"net"
	"sync"
)

type ConsumerGroup struct {
	id         uint16
	consumers  []*ConsumerConn
	partitions []*Partition
	lock       sync.Mutex
}

func createConsumerGroup(id uint16) *ConsumerGroup {
	return &ConsumerGroup{
		id: id,
	}
}

type ConsumerConn struct {
	status bool
	conn   net.Conn
}

func createConsumerConn(conn net.Conn) *ConsumerConn {
	return &ConsumerConn{
		status: true,
		conn:   conn,
	}
}
