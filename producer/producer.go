package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main() {
	// getting environment variables
	hostname := os.Getenv("MEMPHIS_HOST")
	username := os.Getenv("MEMPHIS_USER")
	password := os.Getenv("MEMPHIS_PASS")
	accountId := os.Getenv("ACCOUNT_ID")

	fmt.Println("accountId", accountId, username, hostname, password)

	if hostname == "" || username == "" || password == "" || accountId == "" {
		fmt.Printf("Error one of the variables is empty")
		os.Exit(1)
	}

	accIdInt, err := strconv.Atoi(accountId)
	if err != nil {
		fmt.Printf("Error during conversion account id: %v", err.Error())
		os.Exit(1)
	}

	stationsCount := os.Getenv("MEMPHIS_STATIONS_COUNT")
	if stationsCount == "" {
		fmt.Printf("Error one of the variables is empty")
		os.Exit(1)
	}
	sCount, err := strconv.Atoi(stationsCount)
	if err != nil {
		fmt.Printf("Error during conversion producer count: %v", err.Error())
		os.Exit(1)
	}

	fileSizeParam := os.Getenv("MSG_SIZE") // Specify the desired file size in KB int64(1024 * NUM)
	if fileSizeParam == "" {
		fmt.Printf("Error one of the variables is empty")
		os.Exit(1)
	}
	fileSize, err := strconv.ParseInt(fileSizeParam, 10, 64)
	if err != nil {
		fmt.Printf("Error during conversion file size: %v", err.Error())
		os.Exit(1)
	}

	fileSize = fileSize * 1024
	// generate message
	jsonMessage, err := generateJSON(fileSize)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// creating memphis connection
	for i := 0; i < sCount; i++ {
		go func(hostname string, username string, password string, accountId int, fileSize int64, counter int, jsonMessage []byte, i int) {
			fmt.Println(hostname, username, password, accountId)
			conn, err := memphis.Connect(hostname, username, memphis.Password(password), memphis.AccountId(accountId))
			if err != nil {
				fmt.Printf("error creating connection: %v", err.Error())
				os.Exit(1)
			}
			defer conn.Close()

			stationName := "station" + strconv.Itoa(i)
			station, err := conn.CreateStation(stationName,
				memphis.RetentionTypeOpt(0),
				memphis.RetentionVal(300),
				memphis.StorageTypeOpt(0),
				memphis.Replicas(1),
			)
			if err != nil {
				fmt.Printf("error creating station: %v", err.Error()) //
				os.Exit(1)
			}

			fmt.Println("!", station.Name)

			pname := generateRandomItem()
			p, err := conn.CreateProducer(station.Name, pname)
			if err != nil {
				fmt.Printf("error creating producer: %v", err)
				os.Exit(1)
			}

			for {
				err = p.Produce(jsonMessage)
				if err != nil {
					fmt.Printf("Produce failed: %v", err)
				}
			}

		}(hostname, username, password, accIdInt, fileSize, i, jsonMessage, i)
	}

	time.Sleep(time.Duration(1<<63 - 1))

	fmt.Printf("finished")
}

func generateJSON(fileSize int64) ([]byte, error) {
	data := generateRandomData(fileSize)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func generateRandomData(fileSize int64) []string {
	var data []string
	currentSize := int64(0)

	for currentSize < fileSize {
		item := generateRandomItem()
		data = append(data, item)
		currentSize += int64(len(item))
	}

	return data
}

func generateRandomItem() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const itemSize = 10

	rand.Seed(time.Now().UnixNano())

	result := make([]byte, itemSize)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}
