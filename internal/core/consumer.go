package core

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/TrienThongLu/goMQ/internal/config"
)

type Consumer struct {
	port    uint16
	topicID uint16
	groupID uint16
}

func CreateConsumer(port uint16, topicID uint16, groupID uint16) Consumer {
	return Consumer{
		port:    port,
		topicID: topicID,
		groupID: groupID,
	}
}

func (c *Consumer) registerWithBroker() error {
	var err error

	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", config.BROKER_PORT))
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	message := Message{
		C_REG: &ConsumerRegisterMessage{
			port:    c.port,
			topicID: c.topicID,
			groupID: c.groupID,
		},
	}

	err = WriteMessageToStream(streamRW, message)
	if err != nil {
		panic(err)
	}

	parsedMsg, err := ReadMessageFromStream(streamRW)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Receive response from broker: %v\n", *parsedMsg.R_C_REG)
	return nil
}

func (c *Consumer) StartConsumerServer() error {
	var err error

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", c.port))
	if err != nil {
		return err
	}

	err = c.registerWithBroker()
	if err != nil {
		panic(err)
	}

	conn, _ := listener.Accept()
	defer conn.Close()
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		// Push-based
		// parsedMsg, err := ReadMessageFromStream(streamRW)
		// if err != nil {
		// 	return err
		// }
		// fmt.Printf("Receive message from consumer group: %s", parsedMsg.P_C_M)

		var resp byte = 1
		err = WriteMessageToStream(streamRW, Message{
			R_P_C_M: &resp,
		})
		if err != nil {
			return err
		}

		//Pull-based
		parsedMsg, err := ReadMessageFromStream(streamRW)
		if err != nil {
			return err
		}
		fmt.Printf("Receive message from consumer group: %s", parsedMsg.P_C_M)
		time.Sleep(8 * time.Second)
	}
}
