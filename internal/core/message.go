package core

import (
	"bufio"
	"errors"
	"fmt"
)

const (
	ECHO  = 1
	P_REG = 2
	C_REG = 3
	P_C_M = 4

	R_ECHO  = 101
	R_P_REG = 102
	R_C_REG = 103
	R_P_C_M = 104
)

type Message struct {
	ECHO  *string
	P_REG *ProducerRegisterMessage
	C_REG *ConsumerRegisterMessage
	P_C_M []byte

	R_ECHO  *string
	R_P_REG *byte
	R_C_REG *byte
	R_P_C_M *byte
}

type ProducerRegisterMessage struct {
	port    uint16
	topicID uint16
}

func (m *ProducerRegisterMessage) fromByte(streamMessage []byte) {
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
}

func (m *ProducerRegisterMessage) toByte() []byte {
	data := make([]byte, 4)

	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)

	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)

	return data
}

type ConsumerRegisterMessage struct {
	port    uint16
	topicID uint16
	groupID uint16
}

func (m *ConsumerRegisterMessage) fromByte(streamMessage []byte) {
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
	m.groupID = uint16(streamMessage[4])<<8 + uint16(streamMessage[5])
}

func (m *ConsumerRegisterMessage) toByte() []byte {
	data := make([]byte, 6)

	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)

	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)

	data[4] = byte(m.groupID >> 8)
	data[5] = byte(m.groupID % 256)

	return data
}

func readFromStream(streamRW *bufio.ReadWriter) ([]byte, error) {
	header, err := streamRW.ReadByte()
	if err != nil {
		return nil, err
	}

	data, err := streamRW.Peek(int(header))
	if err != nil {
		return nil, err
	}

	_, err = streamRW.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err
}

func ReadMessageFromStream(streamRW *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(streamRW)
	if err != nil {
		return nil, err
	}

	parsedMsg, err := parseMessage(data)
	if err != nil {
		return nil, err
	}

	return parsedMsg, nil
}

func parseMessage(streamMessage []byte) (*Message, error) {
	switch streamMessage[0] {
	case ECHO:
		st := string(streamMessage[1:])
		return &Message{ECHO: &st}, nil
	case R_ECHO:
		st := string(streamMessage[1:])
		return &Message{R_ECHO: &st}, nil
	case P_REG:
		p := ProducerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{P_REG: &p}, nil
	case R_P_REG:
		st := streamMessage[1]
		return &Message{R_P_REG: &st}, nil
	case C_REG:
		p := ConsumerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{C_REG: &p}, nil
	case R_C_REG:
		st := streamMessage[1]
		return &Message{R_C_REG: &st}, nil
	case P_C_M:
		return &Message{P_C_M: streamMessage[1:]}, nil
	case R_P_C_M:
		st := streamMessage[1]
		return &Message{R_P_C_M: &st}, nil
	default:
		return nil, errors.New("Error parse message")
	}
}

func writeDataToStreamWithType(streamRW *bufio.ReadWriter, mtype byte, data string) error {
	var err error

	err = streamRW.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}

	err = streamRW.WriteByte(mtype)
	if err != nil {
		return err
	}

	_, err = streamRW.WriteString(data)
	if err != nil {
		return err
	}

	err = streamRW.Flush()
	if err != nil {
		return err
	}

	return nil
}

func WriteMessageToStream(streamRW *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeDataToStreamWithType(streamRW, ECHO, *message.ECHO); err != nil {
			return err
		}
	}

	if message.R_ECHO != nil {
		if err := writeDataToStreamWithType(streamRW, R_ECHO, *message.R_ECHO); err != nil {
			return err
		}
	}

	if message.P_REG != nil {
		data := string(message.P_REG.toByte())
		if err := writeDataToStreamWithType(streamRW, P_REG, data); err != nil {
			return err
		}
	}

	if message.R_P_REG != nil {
		data := fmt.Sprintf("%d", *message.R_P_REG)
		if err := writeDataToStreamWithType(streamRW, R_P_REG, data); err != nil {
			return err
		}
	}

	if message.C_REG != nil {
		data := string(message.C_REG.toByte())
		if err := writeDataToStreamWithType(streamRW, C_REG, data); err != nil {
			return err
		}
	}

	if message.R_C_REG != nil {
		data := fmt.Sprintf("%d", *message.R_C_REG)
		if err := writeDataToStreamWithType(streamRW, R_C_REG, data); err != nil {
			return err
		}
	}

	if message.P_C_M != nil {
		if err := writeDataToStreamWithType(streamRW, P_C_M, string(message.P_C_M)); err != nil {
			return err
		}
	}

	if message.R_P_C_M != nil {
		data := fmt.Sprintf("%d", *message.R_P_C_M)
		if err := writeDataToStreamWithType(streamRW, R_P_C_M, data); err != nil {
			return err
		}
	}

	return nil
}
