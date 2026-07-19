package server

import (
	"Gredis/channel"
	client "Gredis/client"
	"Gredis/server/db"
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type Gredis struct {
	clients  map[int]*client.Client
	port     string
	addres   string
	db       *db.Db
	mu       sync.Mutex
	channels map[string]*channel.Channel
}

func NewGredis(port string) *Gredis {
	gredis := &Gredis{clients: make(map[int]*client.Client), port: port, db: db.NewGredisDb(), channels: make(map[string]*channel.Channel)}

	//print ascii for server
	gredis.printASCII()

	//restore db data if available
	gredis.restore()

	return gredis
}

// start Gredis server
func (s *Gredis) Serve() {

	ln, err := net.Listen("tcp", s.port)

	if err != nil {
		fmt.Printf("listner not set due to error: %v.\nPress ctr + c to exit\n", err)
		return
	}

	//start aof worker, tracking of commands
	go s.db.StartAofWorker()

	for {
		//await connection
		conn, err := ln.Accept()

		if err != nil {
			fmt.Printf("connection not possible %v", err)
			continue
		}

		//handle connection concurrently
		go s.handleConnection(conn)
	}

}

// handle individual connection
func (s *Gredis) handleConnection(c net.Conn) {

	defer c.Close()

	//register client
	client := s.registerClient(c)

	//send message to user channel
	client.Mess <- "user connected"

	read := bufio.NewReader(client.Connection)

	for {
		cmd, err := read.ReadBytes('\n')
		if err != nil {
			//remove connection
			s.removeClient(client.Id)
			return
		}

		protocol, err := s.parseCommand(cmd, client.Id)

		if err != nil {
			client.Mess <- err.Error()
		}

		client.Mess <- protocol
	}

}

func (s *Gredis) removeClient(clientId int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, clientId)
}

func (s *Gredis) registerClient(c net.Conn) *client.Client {
	//make client
	client := client.NewClient(c)

	//register client
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[client.Id] = client

	return s.clients[client.Id]
}

func (s *Gredis) respond() {

}

// server parser for incomming commands via tcp socket
func (s *Gredis) parseCommand(b []byte, clientId int) (string, error) {
	protocol := string(b)

	parts := strings.SplitN(protocol, " ", 3)

	if len(parts) < 2 {
		return "", errors.New("Invalid command try [GET KEY | SET KEY VALUE]")
	}

	//make command uppercase
	action := strings.ToUpper(parts[0])

	//trim key of enter character
	key := strings.TrimSuffix(parts[1], "\n")

	switch action {
	case "GET":
		//GET KEY
		value, err := s.db.Get(key)
		if err != nil {
			return "", err
		}
		return value, nil
	case "SET":
		//write command to worker for backup
		s.db.CmdCh <- protocol
		value := strings.TrimSuffix(parts[2], "\n")
		//SET KEY
		s.db.Set(key, value)
		return "SET command works hi form gredis", nil
	case "SUBSCRIBE":
		//pass in channel name and client pointer
		_, err := s.handleSubscription(key, s.clients[clientId])

		if err != nil {
			return "Couldn't connect to channel ", err
		}

		return "Connected to channel", nil
	case "PUBLISH":
		value := strings.TrimSuffix(parts[2], "\n")
		//broadcast everything send to the channel's channel
		_, err := s.handleBroadcast(key, value)

		if err != nil {
			return "", fmt.Errorf("failed, %v", err)
		}

		return "broadcast succesful", nil

	default:
		return "Choose command SET OR GET", nil
	}
}

func (s *Gredis) printASCII() {
	fmt.Printf(`  _________                                               .__  .__             ._.
 /   _____/ ______________  __ ___________    ____   ____ |  | |__| ____   ____| |
 \_____  \_/ __ \_  __ \  \/ // __ \_  __ \  /  _ \ /    \|  | |  |/    \_/ __ \ |
 /        \  ___/|  | \/\   /\  ___/|  | \/ (  <_> )   |  \  |_|  |   |  \  ___/\|
/_______  /\___  >__|    \_/  \___  >__|     \____/|___|  /____/__|___|  /\___  >_
        \/     \/                 \/                    \/             \/     \/\/
          __________           _________                                          
          \______   \___.__.  /   _____/____    _____                             
           |    |  _<   |  |  \_____  \\__  \  /     \                            
           |    |   \\___  |  /        \/ __ \|  Y Y  \                           
           |______  // ____| /_______  (____  /__|_|  /                           
                  \/ \/              \/     \/      \/                            
                                                                                  
                                                                                  
                                                                                  
                                                                                  
                                                                                  
                                                                                  `)
}

// create channel
func (s *Gredis) CreateChannel(channelName string) {
	s.channels[channelName] = channel.NewChannel(channelName)
}

// handle subscription to channel
func (s *Gredis) handleSubscription(channel string, client *client.Client) (bool, error) {
	//check if channel exists
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.channels[channel]; !exists {
		return false, errors.New("channel doesn't exist")
	}

	//register client to channel
	s.channels[channel].Subscribe(client)

	return true, nil
}

func (s *Gredis) handleBroadcast(channelName string, value string) (bool, error) {
	//check if channel exists
	if _, exists := s.channels[channelName]; !exists {
		return false, errors.New("channel doesn't exist")
	}

	//push message into channels channel
	s.channels[channelName].Ch <- value

	return true, nil
}

// read from aof file to restore data after crash or restart
func (s *Gredis) restore() {
	f, err := os.Open("aof.txt")

	if err != nil {
		fmt.Println("\nreading file backup failed went wrong")
		return
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.SplitN(scanner.Text(), " ", 3)

		switch strings.ToUpper(line[0]) {
		case "SET":
			//skip incomplete commmand
			if len(line) < 3 {
				continue
			}
			s.db.Set(line[1], line[2])
		case "DEL":
			if len(line) < 2 {
				continue
			}
			//delete logic
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("\nerror reading from backup file")
	}

	fmt.Println("\nBackup from aof file succesful")
}
