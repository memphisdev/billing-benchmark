package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main() {
	// get enviroment variables
	hostname := os.Getenv("MEMPHIS_HOST")
	password := os.Getenv("MEMPHIS_PASS")
	username := os.Getenv("MEMPHIS_USER")
	cgName := os.Getenv("MEMPHIS_CG")
	counsumerCountStr := os.Getenv("MEMPHIS_CON_COUNT")
	accountId := os.Getenv("ACCOUNT_ID")

	fmt.Println(hostname, password, username, cgName, counsumerCountStr, accountId)

	//validate env variables exist
	if hostname == "" || password == "" || username == "" || cgName == "" || counsumerCountStr == "" || accountId == "" {
		fmt.Printf("missing some enviroment variables")
		os.Exit(1)
	}

	accIdInt, err := strconv.Atoi(accountId)
	if err != nil {
		fmt.Printf("Error during conversion account id: %v", err.Error())
		os.Exit(1)
	}

	consumerCount, err := strconv.Atoi(counsumerCountStr)
	if err != nil {
		fmt.Printf("Error converting consumer count to int: %v", err.Error())
		os.Exit(1)
	}

	// creating message handler

	handler := func(msgs []*memphis.Msg, err error, ctx context.Context) {
		if err != nil {
			fmt.Printf("falied fetching :( ")
		}

		for _, msg := range msgs {
			msg.Ack()
		}
	}

	for i := 0; i < consumerCount; i++ {
		go func(hostname, username, password, cgName string, accountId int, handler memphis.ConsumeHandler, i int) {

			conn, err := memphis.Connect(hostname, username, memphis.Password(password), memphis.AccountId(accountId))
			if err != nil {
				fmt.Printf("Error connecting to memphis: %v", err.Error())
				os.Exit(1)
			}

			consumerName := generateString()
			stationName := "station" + strconv.Itoa(i)
			fmt.Println(stationName)

			consumer, err := conn.CreateConsumer(stationName, consumerName, memphis.ConsumerGroup(cgName))
			if err != nil {
				fmt.Printf("Error creating consumer: %v", err.Error())
			}

			consumer.Consume(handler)

		}(hostname, username, password, cgName, accIdInt, handler, i)
	}

	time.Sleep(time.Duration(1<<63 - 1))

}

func generateString() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const itemSize = 10

	rand.Seed(time.Now().UnixNano())

	result := make([]byte, itemSize)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}
