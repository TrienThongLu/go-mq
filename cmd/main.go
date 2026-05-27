package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/TrienThongLu/goMQ/internal/core"
)

func main() {
	switch os.Args[1] {
	case "server":
		broker := core.CreateBroker()
		err := broker.StartBrokerServer()
		if err != nil {
			fmt.Printf("Error starting broker: %v\n", err.Error())
		}
	case "producer":
		port, err := strconv.ParseInt(os.Args[2], 10, 32)
		if err != nil {
			fmt.Printf("Error starting producer: %v\n", err.Error())
		}

		topicID, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			fmt.Printf("Error starting producer: %v\n", err.Error())
		}

		producer := core.CreateProducer(uint16(port), uint16(topicID))
		// err = producer.StartProducerServer()
		err = producer.StartAndSimulateProducerServer()
		if err != nil {
			fmt.Printf("Error starting producer: %v\n", err.Error())
		}
	case "consumer":
		port, err := strconv.ParseInt(os.Args[2], 10, 32)
		if err != nil {
			fmt.Printf("Error starting consumer: %v\n", err.Error())
		}

		topicID, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			fmt.Printf("Error starting consumer: %v\n", err.Error())
		}

		groupID, err := strconv.ParseInt(os.Args[4], 10, 32)
		if err != nil {
			fmt.Printf("Error starting consumer: %v\n", err.Error())
		}

		consumer := core.CreateConsumer(uint16(port), uint16(topicID), uint16(groupID))
		err = consumer.StartConsumerServer()
		if err != nil {
			fmt.Printf("Error starting consumer: %v\n", err.Error())
		}
	}
}
