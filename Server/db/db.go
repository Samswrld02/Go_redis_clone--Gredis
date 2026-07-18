package db

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// basic Gredis db
type Db struct {
	data  map[string]string
	mu    sync.Mutex
	CmdCh chan string
}

func NewGredisDb() *Db {
	return &Db{data: make(map[string]string), CmdCh: make(chan string, 10000)}
}

// Set db data
func (db *Db) Set(key string, value string) (bool, error) {
	db.mu.Lock()
	db.data[key] = value
	db.mu.Unlock()

	return true, nil
}

// Get cached db data
func (db *Db) Get(key string) (string, error) {

	if value, exists := db.data[key]; exists {
		return value, nil
	}

	return "", errors.New("Key doesn't exist in the database")
}

// aof worker
func (db *Db) StartAofWorker() {
	f, err := os.OpenFile("aof.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println("something went wrong with aof file")
	}

	defer f.Close()

	//read from queue of commands to handle
	for command := range db.CmdCh {
		fmt.Println("Queue has ", command)

		_, err = f.Write([]byte(command))

		if err != nil {
			fmt.Println("writing failed to file")
		}
	}

}
