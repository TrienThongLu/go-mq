package core

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/TrienThongLu/goMQ/internal/config"
)

type Broker struct {
	topics []*Topic
}

func CreateBroker() *Broker {
	return &Broker{
		topics: make([]*Topic, 0),
	}
}

func (b *Broker) StartBrokerServer() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.BROKER_PORT))
	if err != nil {
		panic(err)
	}

	for {
		conn, _ := listener.Accept()
		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		parsedMsg, err := ReadMessageFromStream(streamRW)
		if err != nil {
			return err
		}

		resp, err := b.processBrokerMessage(parsedMsg)
		if err != nil {
			return err
		}

		err = WriteMessageToStream(streamRW, *resp)
		if err != nil {
			return err
		}

		err = conn.Close()
		if err != nil {
			return err
		}
	}
}

func (b *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		resp, err := b.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}

		return &Message{R_ECHO: &resp}, nil
	}

	if message.P_REG != nil {
		resp, err := b.processProducerRegisterMessage(*message.P_REG)
		if err != nil {
			return nil, err
		}

		return &Message{R_P_REG: resp}, nil
	}

	if message.C_REG != nil {
		resp, err := b.processConsumerRegisterMessage(*message.C_REG)
		if err != nil {
			return nil, err
		}

		return &Message{R_C_REG: resp}, nil
	}

	return nil, errors.New("Process broker message fail")
}

func (b *Broker) processEchoMessage(msg *string) (string, error) {
	return fmt.Sprintf("I have received: %s", *msg), nil
}

func (b *Broker) processProducerRegisterMessage(producerRegMsg ProducerRegisterMessage) (*byte, error) {
	var topic *Topic
	for _, tp := range b.topics {
		if tp.id == producerRegMsg.topicID {
			topic = tp
			break
		}
	}
	if topic == nil {
		topic = createTopic(producerRegMsg.topicID)
		b.topics = append(b.topics, topic)
	}

	go func() {
		conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", producerRegMsg.port))
		fmt.Printf("Connected to producer at port %v\n", producerRegMsg.port)

		streamRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		for {
			parsedMsg, err := ReadMessageFromStream(streamRW)
			if err != nil {
				panic(err)
			}

			if parsedMsg.P_C_M != nil {
				resp, err := b.processProducerPCM(parsedMsg.P_C_M, topic)
				if err != nil {
					panic(err)
				}

				err = WriteMessageToStream(streamRW, Message{
					R_P_C_M: &resp,
				})
				if err != nil {
					panic(err)
				}
			}

		}
	}()

	var resp byte = 0

	return &resp, nil
}

func (b *Broker) processProducerPCM(pcm []byte, topic *Topic) (byte, error) {
	if len(topic.consumerGroups) == 0 {
		topic.queue.push(pcm)
		topic.queue.debug(topic.id)
	} else {
		for _, cGroup := range topic.consumerGroups {
			minSize := config.QueueSize
			partitionIdx := -1
			for idx, partition := range cGroup.partitions {
				currentSize := partition.queue.size()
				if currentSize < minSize {
					minSize = currentSize
					partitionIdx = idx
				}
			}

			if partitionIdx != -1 {
				partition := cGroup.partitions[partitionIdx]
				partition.lock.Lock()

				for {
					msg := topic.queue.pop()
					if msg == nil {
						break
					}
					partition.queue.push(msg)
				}

				partition.queue.push(pcm)
				fmt.Printf("Put data '%s' to cgroup %d, partition %d\n", string(pcm), cGroup.id, partitionIdx)
				partition.lock.Unlock()
			}
		}
	}

	return 0, nil
}

func (b *Broker) processConsumerRegisterMessage(consumerRegMsg ConsumerRegisterMessage) (*byte, error) {
	var topic *Topic
	for _, tp := range b.topics {
		if tp.id == consumerRegMsg.topicID {
			topic = tp
			break
		}
	}
	if topic == nil {
		topic = createTopic(consumerRegMsg.topicID)
		b.topics = append(b.topics, topic)
	}

	var cGroup *ConsumerGroup
	for _, cg := range topic.consumerGroups {
		if cg.id == consumerRegMsg.groupID {
			cGroup = cg
			break
		}
	}
	if cGroup == nil {
		cGroup = createConsumerGroup(consumerRegMsg.topicID)
		topic.lock.Lock()
		topic.consumerGroups = append(topic.consumerGroups, cGroup)
		topic.lock.Unlock()
	}

	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", consumerRegMsg.port))
	fmt.Printf("Connected to consumer at port %v\n", consumerRegMsg.port)

	consumerConn := createConsumerConn(conn)
	cGroup.lock.Lock()
	cGroup.consumers = append(cGroup.consumers, consumerConn)

	if len(cGroup.consumers) > len(cGroup.partitions) {
		cGroup.partitions = append(cGroup.partitions, CreatePartition())
	}
	partition := cGroup.partitions[len(cGroup.partitions)-1]

	cGroup.lock.Unlock()
	go b.readConsumerReadyAndSend(consumerConn, partition)

	var resp byte = 0
	return &resp, nil
}

// Pull-based
func (b *Broker) readConsumerReadyAndSend(consumerConn *ConsumerConn, partition *Partition) {
	streamRW := bufio.NewReadWriter(bufio.NewReader(consumerConn.conn), bufio.NewWriter(consumerConn.conn))

	for {
		if !consumerConn.status {
			parsedMessage, err := ReadMessageFromStream(streamRW)
			if err != nil {
				panic(err)
			}

			if parsedMessage.R_P_C_M != nil {
				consumerConn.status = true
			} else {
				fmt.Printf("Parsed message not R_PCM: %v", parsedMessage)
				panic("Why not R_PCM???")

			}
		}

		partition.lock.Lock()
		pcm := partition.queue.pop()
		partition.lock.Unlock()

		if pcm == nil {
			continue
		}

		consumerConn.status = false
		err := WriteMessageToStream(streamRW, Message{
			P_C_M: pcm,
		})
		if err != nil {
			panic(err)
		}
	}
}
