package core

import (
	"net"
	"sync"
)

type ConsumerGroup struct {
	id             uint16
	offset         uint32
	consumers      []*ConsumerConn
	readyConsumers []*ConsumerConn
	lock           sync.Mutex
}

func createConsumerGroup(id uint16) *ConsumerGroup {
	return &ConsumerGroup{
		id:     id,
		offset: 0,
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
