package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
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
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)

	//handlers
	handlerPause := func(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
		return func(ps routing.PlayingState) pubsub.AckType {
			defer fmt.Print("> ")
			gs.HandlePause(ps)
			return pubsub.Ack
		}
	}

	handlerMove := func(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
		return func(am gamelogic.ArmyMove) pubsub.AckType {
			defer fmt.Print("> ")
			outcome := gs.HandleMove(am)
			if outcome == gamelogic.MoveOutComeSafe {
				return pubsub.Ack
			} else if outcome == gamelogic.MoveOutcomeMakeWar {
				key := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, username)
				if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, gamelogic.RecognitionOfWar{Defender: gs.GetPlayerSnap(), Attacker: am.Player}); err != nil {
					return pubsub.NackRequeue
				}
				return pubsub.Ack
			}
			return pubsub.NackDiscard
		}
	}

	handlerWar := func(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
		return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
			defer fmt.Print("> ")
			outcome, winner, loser := gs.HandleWar(war)
			key := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
			switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue
			case gamelogic.WarOutcomeNoUnits:
				return pubsub.NackDiscard
			case gamelogic.WarOutcomeYouWon:
				msg := fmt.Sprintf("%s won a war against %s", winner, loser)
				return pubsub.PublishGameLog(ch, routing.ExchangePerilTopic, key, msg, war.Attacker)
			case gamelogic.WarOutcomeOpponentWon:
				msg := fmt.Sprintf("%s won a war against %s", winner, loser)
				return pubsub.PublishGameLog(ch, routing.ExchangePerilTopic, key, msg, war.Attacker)
			case gamelogic.WarOutcomeDraw:
				msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
				return pubsub.PublishGameLog(ch, routing.ExchangePerilTopic, key, msg, war.Attacker)
			default:
				log.Print("Error: unknown war outcome")
				return pubsub.NackDiscard
			}
		}
	}

	pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), "army_moves.*", pubsub.Transient, handlerMove(gameState))
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.Durable, handlerWar(gameState))

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		cmd := strings.ToLower(input[0])

		switch cmd {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), move)
			fmt.Println("Move was published successfully")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Not enough arguments, please try again")
				continue
			}
			spamAmount, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			key := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
			for range spamAmount {
				msg := gamelogic.GetMaliciousLog()
				pubsub.PublishGameLog(ch, routing.ExchangePerilTopic, key, msg, gameState.GetPlayerSnap())
			}
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(0)
		default:
			fmt.Println("Unknown command, please try again")
		}
		continue
	}
}
