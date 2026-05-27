package core

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/TrienThongLu/goMQ/internal/config"
)

type Producer struct {
	port    uint16
	topicID uint16
}

func CreateProducer(port uint16, topicID uint16) Producer {
	return Producer{
		port:    port,
		topicID: topicID,
	}
}

func (p *Producer) registerWithBroker() error {
	var err error

	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", config.BROKER_PORT))
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	message := Message{
		P_REG: &ProducerRegisterMessage{
			port:    p.port,
			topicID: p.topicID,
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

	fmt.Printf("Receive response from broker: %v\n", *parsedMsg.R_P_REG)
	return nil
}

func (p *Producer) StartProducerServer() error {
	var err error

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.port))
	if err != nil {
		return err
	}

	err = p.registerWithBroker()
	if err != nil {
		panic(err)
	}

	conn, _ := listener.Accept()
	defer conn.Close()
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rd := bufio.NewReader(os.Stdin)

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}

			panic(err)
		}

		err = WriteMessageToStream(streamRW, Message{
			P_C_M: []byte(line),
		})
		if err != nil {
			return err
		}

		parsedMsg, err := ReadMessageFromStream(streamRW)
		if err != nil {
			return err
		}

		fmt.Printf("Receive message from broker: %d\n", *parsedMsg.R_P_C_M)
	}
}

func (p *Producer) StartAndSimulateProducerServer() error {
	var err error

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.port))
	if err != nil {
		return err
	}

	err = p.registerWithBroker()
	if err != nil {
		panic(err)
	}

	conn, _ := listener.Accept()
	defer conn.Close()
	streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		time.Sleep(5 * time.Second)
		line := fmt.Sprintf("Hello from producer %v at %v\n", p.port, time.Now().Unix())

		err = WriteMessageToStream(streamRW, Message{
			P_C_M: []byte(line),
		})
		if err != nil {
			return err
		}

		parsedMsg, err := ReadMessageFromStream(streamRW)
		if err != nil {
			return err
		}

		fmt.Printf("Receive message from broker: %d\n", *parsedMsg.R_P_C_M)
	}
}
