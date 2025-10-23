package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connection successful")
	gamelogic.PrintServerHelp()
	pauseChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	logHandler := func() func(routing.GameLog) pubsub.AckType {
		return func(gl routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			if err := gamelogic.WriteLog(gl); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
	}

	pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable, logHandler())

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		cmd := strings.ToLower(input[0])
		switch cmd {
		case "pause":
			fmt.Println("Pausing game")
			if pubsub.PublishJSON(pauseChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
				log.Fatal(err)
			}
		case "resume":
			fmt.Println("Resuming game")
			if pubsub.PublishJSON(pauseChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false}); err != nil {
				log.Fatal(err)
			}
		case "quit":
			fmt.Println("Shutting down")
			os.Exit(0)
		default:
			fmt.Println("Unknown command, please try again")
		}
		continue
	}

}
