package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: jsonData}); err != nil {
		return err
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(val); err != nil {
		return err
	}

	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: network.Bytes()}); err != nil {
		return err
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, exchange, key, msg string, player gamelogic.Player) AckType {
	username := player.Username
	newGameLog := routing.GameLog{CurrentTime: time.Now(), Message: msg, Username: username}
	if err := PublishGob(ch, exchange, key, newGameLog); err != nil {
		return NackRequeue
	}

	return Ack
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	var isDurable bool
	var autoDelete bool
	var isExclusive bool

	switch queueType {
	case Durable:
		isDurable = true
		autoDelete = false
		isExclusive = false
	case Transient:
		isDurable = false
		autoDelete = true
		isExclusive = true
	}
	queue, err := ch.QueueDeclare(queueName, isDurable, autoDelete, isExclusive, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		log.Fatal(err)
	}

	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		log.Fatal(err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var data T
			if err := json.Unmarshal(delivery.Body, &data); err != nil {
				log.Fatal(err)
			}
			ackType := handler(data)
			switch ackType {
			case Ack:
				delivery.Ack(false)
				log.Print("Acknowledged")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Print("Not acknowledged and requeued")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Print("Not acknowledged and discarded")
			}
		}
	}()
	return nil
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, amqp.Table{"ContentType": "application/gob"})
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var data T
			buf := bytes.NewBuffer(delivery.Body)
			dec := gob.NewDecoder(buf)
			if err := dec.Decode(&data); err != nil {
				log.Fatal(err)
			}
			ackType := handler(data)
			switch ackType {
			case Ack:
				delivery.Ack(false)
				log.Print("Acknowledged")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Print("Not acknowledged and requeued")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Print("Not acknowledged and discarded")
			}
		}
	}()
	return nil
}
