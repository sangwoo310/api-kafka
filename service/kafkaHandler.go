package service

import (
	"bithumb-cm/common/kafka"
	"fmt"
	"log"
	"net/http"
	"time"
)

type KafkaSetting struct {
	Address 		string
	Topic			string
	Offset			int64
}

type KafkaProducer struct {
	KafkaSetting 	*KafkaSetting
	JsonFilePath	string
}

// Args holds arguments passed to JSON RPC service
type Args struct {
	ProduceType 		string 			`json:"type"`
	LoopCount			int64			`json:"loop_count"`
	KafkaMessage		string			`json:"message"`
}

type SignerArgs struct {
	ProduceType 		string 			`json:"type"`
	LoopCount			int64			`json:"loop_count"`
}

func (p *KafkaProducer) AddressGenerateProducer(r *http.Request, args *Args, reply *string) error {
	log.Println("Producer API call")
	log.Println("Args is ", &args)

	*reply = "Return message is : AddressGenerateProducer"
	
	return nil
}

func (p *KafkaProducer) SignerProducer(r *http.Request, args *SignerArgs, reply *string) error {
	log.Println("Producer API call")
	log.Println("Args is ", &args)

	*reply = "Return message is : SignerProducer"

	return nil
}

func (s KafkaSetting) Consumer()  {
	c, err := kafka.NewConsumer(kafka.DefaultConsumerConfig(), []string{s.Address})
	if err != nil {
		fmt.Println(err)
		return
	}

	err = c.BindOffset(s.Topic, 0, s.Offset, func(message *kafka.ConsumerMessage, err *kafka.ConsumerError) {
		if err != nil {
			fmt.Println("error!", err)
			return
		}

		if message != nil {
			fmt.Println(string(message.Value), ":", message)
			return
		}
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	time.Sleep(1 * time.Hour)
}