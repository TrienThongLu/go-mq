package core

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"time"

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
		go b.stopAndPop(topic)
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
					R_P_C_M: resp,
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

func (b *Broker) processProducerPCM(pcm []byte, topic *Topic) (*byte, error) {
	topic.queue.push(pcm)
	topic.queue.debug(topic.id)

	var resp byte = 0

	return &resp, nil
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
		go b.stopAndPop(topic)
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
		// go b.startConsumerGroupConsumption(topic, cGroup)
	}

	conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", consumerRegMsg.port))
	fmt.Printf("Connected to consumer at port %v\n", consumerRegMsg.port)
	consumerConn := createConsumerConn(conn)
	cGroup.lock.Lock()
	cGroup.consumers = append(cGroup.consumers, consumerConn)
	cGroup.lock.Unlock()
	go b.readConsumerReadyAndSend(topic, cGroup, consumerConn)

	var resp byte = 0
	return &resp, nil
}

// Push-based
func (b *Broker) startConsumerGroupConsumption(topic *Topic, cGroup *ConsumerGroup) {
	for {
		cGroup.lock.Lock()
		offset := cGroup.offset
		pcm := topic.queue.peek(offset)
		if pcm == nil {
			cGroup.lock.Unlock()
			continue
		}

		for _, consumer := range cGroup.consumers {
			if consumer.status {
				streamRW := bufio.NewReadWriter(bufio.NewReader(consumer.conn), bufio.NewWriter(consumer.conn))

				consumer.status = false
				err := WriteMessageToStream(streamRW, Message{
					P_C_M: pcm,
				})
				if err != nil {
					panic(err)
				}

				parsedMsg, err := ReadMessageFromStream(streamRW)
				if err != nil {
					panic(err)
				}

				if parsedMsg.R_P_C_M != nil {
					consumer.status = true
				}

				cGroup.offset += 1
				break
			}
		}
		cGroup.lock.Unlock()
	}
}

// Pull-based
func (b *Broker) readConsumerReadyAndSend(topic *Topic, cGroup *ConsumerGroup, consumerConn *ConsumerConn) {
	streamRW := bufio.NewReadWriter(bufio.NewReader(consumerConn.conn), bufio.NewWriter(consumerConn.conn))

	for {
		parsedMessage, err := ReadMessageFromStream(streamRW)
		if err != nil {
			panic(err)
		}

		if parsedMessage.R_P_C_M == nil {
			fmt.Printf("Parsed message not R_PCM: %v", parsedMessage)
			panic("Why not R_PCM???")
		}

		cGroup.lock.Lock()
		offset := cGroup.offset
		var pcm []byte = nil
		for pcm == nil {
			pcm = topic.queue.peek(offset)
		}

		err = WriteMessageToStream(streamRW, Message{
			P_C_M: pcm,
		})
		if err != nil {
			panic(err)
		}

		cGroup.offset += 1
		cGroup.lock.Unlock()
	}
}

func (b *Broker) stopAndPop(topic *Topic) {
	for {
		time.Sleep(5 * time.Second)
		topic.lock.Lock()

		var minOffset uint32 = math.MaxUint32
		for _, cGroup := range topic.consumerGroups {
			minOffset = min(minOffset, cGroup.offset)
		}

		if minOffset == math.MaxUint32 {
			topic.lock.Unlock()
			continue
		}

		for _, cGroup := range topic.consumerGroups {
			cGroup.lock.Lock()
			cGroup.offset -= minOffset
		}

		for minOffset > 0 {
			topic.queue.pop()
			minOffset--
		}

		for _, cGroup := range topic.consumerGroups {
			cGroup.lock.Unlock()
		}

		topic.lock.Unlock()
	}
}
